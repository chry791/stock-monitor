#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STOCK MONITOR v3.6 - Anteprima live a doppia campana (Europa + USA)
===================================================================
Scansione LIVE del mercato USA a borsa ancora aperta (~21:15 italiane),
con i prezzi in tempo reale (ritardo 15 minuti) inclusi nel piano EODHD.

Scopo: vedere PRIMA della chiusura chi sta crollando OGGI, per la
"Corsia 1" (trade lampo sul micro-rimbalzo di apertura del giorno dopo).
Finestra operativa: la lista esce ~21:15-21:30, gli ordini su gettex
si possono inserire fino alle 22:00.

Cosa fa:
  1. legge tickers.csv e prende SOLO i titoli USA (l'Europa a
     quell'ora e' gia' chiusa: per lei vale il report ufficiale);
  2. scarica i prezzi live a blocchi di 16 ticker per richiesta
     (~35 richieste, ~560 crediti API: briciole sul limite giornaliero);
  3. calcola la variazione di OGGI rispetto alla chiusura di ieri;
  4. per gli alert sotto la soglia, stima la distanza dal massimo a
     52 settimane partendo dal "dal max" del report del mattino;
  5. scrive docs/live.json per il pannello serale della dashboard.

Cosa NON fa (di proposito):
  - NON tocca data.json, history.json, registro.json ne' i CSV:
    il report ufficiale e il laboratorio restano basati sulle
    chiusure vere. Questa e' solo un'anteprima operativa.

La API key si legge da EOD_API_KEY (Secrets su GitHub Actions).
"""
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

# ============================================================
# CONFIGURAZIONE
# ============================================================
API_KEY = os.environ.get("EOD_API_KEY", "").strip()
# Quale sessione scansionare: "auto" decide dall'orologio (UTC):
#   12:00-17:59 UTC -> EUROPA (chiusure 17:30 italiane, sveglia 16:55)
#   altrimenti      -> USA    (chiusura 22:00 italiana, sveglia 21:15)
MARKETS = os.environ.get("MARKETS", "auto").strip().lower()
EU_EXCHANGES = ("LSE", "PA", "XETRA", "MC", "AS", "SW")
LIVE_THRESHOLD = float(os.environ.get("LIVE_THRESHOLD", "-5.0"))
MAX_ABS_CHANGE = 50.0        # scarta variazioni anomale (come scan.py)
BATCH = 16                   # ticker per richiesta all'endpoint live
TIMEOUT = 30

BASE = "https://eodhd.com/api"
ROOT = Path(__file__).parent
DOCS = ROOT / "docs"


def log(msg=""):
    print(msg, flush=True)


def resolve_markets():
    """Sceglie la sessione: env MARKETS=us/eu, oppure "auto" dall'orologio."""
    if MARKETS in ("us", "usa"):
        return "USA"
    if MARKETS in ("eu", "europa", "europe"):
        return "Europa"
    h = datetime.now(timezone.utc).hour
    return "Europa" if 12 <= h < 18 else "USA"


def load_watchlist(market):
    """Legge tickers.csv -> [(base, exchange, nome)] della sessione scelta."""
    wanted = EU_EXCHANGES if market == "Europa" else ("US",)
    out = []
    with open(ROOT / "tickers.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ticker = row["ticker"].strip()
            if "." not in ticker:
                continue
            base, exchange = ticker.rsplit(".", 1)
            if exchange in wanted:
                out.append((base, exchange, row["name"].strip()))
    return out


def load_morning_distmax():
    """Dal report del mattino (data.json): ticker USA -> dist_max di ieri.
    Serve per stimare il 'dal max' di stasera senza chiamate in piu'."""
    dm = {}
    f = DOCS / "data.json"
    if not f.exists():
        return dm
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
        for r in data.get("results", []):
            if r.get("dist_max") is not None:
                dm[r["ticker"]] = float(r["dist_max"])
    except Exception:
        pass
    return dm


def fetch_live(tickers):
    """Scarica i prezzi live (ritardo 15 min) a blocchi.
    'tickers' sono simboli completi (es. AAPL.US, AIR.PA).
    Ritorna {ticker_completo: (prezzo_attuale, chiusura_ieri, var_pct)}."""
    prices = {}
    for i in range(0, len(tickers), BATCH):
        chunk = tickers[i:i + BATCH]
        first, rest = chunk[0], ",".join(chunk[1:])
        params = {"api_token": API_KEY, "fmt": "json"}
        if rest:
            params["s"] = rest
        try:
            r = requests.get(f"{BASE}/real-time/{first}",
                             params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log(f"   (blocco {i // BATCH + 1}: errore {str(e)[:60]} - salto)")
            continue
        if isinstance(data, dict):
            data = [data]
        for item in data if isinstance(data, list) else []:
            code = str(item.get("code", "")).strip()
            try:
                close = float(item.get("close"))
                prev = float(item.get("previousClose"))
            except (TypeError, ValueError):
                continue                      # 'NA' o dati mancanti
            if close <= 0 or prev <= 0:
                continue
            pct = (close / prev - 1.0) * 100.0
            if abs(pct) > MAX_ABS_CHANGE:
                continue
            prices[code] = (close, prev, pct)
    return prices


def main():
    if not API_KEY:
        log("ERRORE: variabile d'ambiente EOD_API_KEY mancante.")
        sys.exit(1)

    start = datetime.now(timezone.utc)
    market = resolve_markets()
    log("=" * 60)
    log(f"STOCK MONITOR v3.6 - ANTEPRIMA LIVE ({market}, ritardo 15 min)")
    log("=" * 60)

    watch = load_watchlist(market)
    log(f"Titoli {market} in watchlist: {len(watch)}")
    dist_yday = load_morning_distmax()

    full = [f"{b}.{e}" for b, e, _ in watch]
    prices = fetch_live(full)
    log(f"Prezzi live ricevuti: {len(prices)}/{len(watch)}")

    names = {f"{b}.{e}": n for b, e, n in watch}
    alerts = []
    for tick, (close, prev, pct) in prices.items():
        if pct > LIVE_THRESHOLD:
            continue
        # stima del "dal max" di adesso: parte dal dato dell'ultimo report
        # e lo aggiorna con la variazione di oggi
        dm = dist_yday.get(tick)
        dm_est = (round(((1 + dm / 100.0) * (1 + pct / 100.0) - 1) * 100.0, 1)
                  if dm is not None else None)
        alerts.append({
            "ticker": tick,
            "name": names.get(tick, tick),
            "price": round(close, 2),
            "prev_close": round(prev, 2),
            "pct": round(pct, 2),
            "dist_max": dm_est,
        })
    alerts.sort(key=lambda a: a["pct"])

    DOCS.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": start.isoformat(timespec="seconds"),
        "market": market,
        "threshold": LIVE_THRESHOLD,
        "analyzed": len(prices),
        "count": len(alerts),
        "results": alerts,
    }
    (DOCS / "live.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    log(f"\nAlert intraday <= {LIVE_THRESHOLD}%: {len(alerts)}")
    for a in alerts[:20]:
        dm = (f"  (dal max stimato: {a['dist_max']:+.1f}%)"
              if a.get("dist_max") is not None else "")
        log(f"  {a['ticker']:12} {a['name'][:30]:30} {a['pct']:+7.2f}%{dm}")
    if len(alerts) > 20:
        log(f"  ... e altri {len(alerts) - 20}")
    log("\nScritto docs/live.json - il pannello live appare da solo.")
    if market == "Europa":
        log("Promemoria: chiusura borse europee 17:30. Filtro 3 prima!")
    else:
        log("Promemoria: ordini su gettex fino alle 22:00. Filtro 3 prima!")


if __name__ == "__main__":
    main()
