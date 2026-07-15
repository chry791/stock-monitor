#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STOCK MONITOR v3.0 - Scansione Bulk EODHD
==========================================
Stessa logica del Monitor Robusto v2.4 (soglia -5%, stessa watchlist,
stesso formato CSV), ma usa l'endpoint BULK di EODHD:
1 richiesta per exchange invece di 1 richiesta per ticker.
738 ticker su 7 mercati -> ~14-20 richieste totali, ~20 secondi.

Output:
  docs/data.json                     -> dati completi per la dashboard
  docs/history.json                  -> storico alert (ultimi 90 giorni)
  docs/reports/alert_report_DATA.csv -> report CSV (formato identico al v2.4)

La API key si legge dalla variabile d'ambiente EOD_API_KEY
(su GitHub Actions arriva dai Secrets, in locale: export EOD_API_KEY=...).
"""
import csv
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ============================================================
# CONFIGURAZIONE
# ============================================================
API_KEY = os.environ.get("EOD_API_KEY", "").strip()
ALERT_THRESHOLD = float(os.environ.get("ALERT_THRESHOLD", "-5.0"))
MAX_ABS_CHANGE = 50.0          # scarta variazioni anomale (come v2.4)
HISTORY_DAYS = 90              # giorni di storico da conservare
TIMEOUT = 60                   # timeout richieste HTTP (bulk = risposte grandi)

BASE = "https://eodhd.com/api"
ROOT = Path(__file__).parent
DOCS = ROOT / "docs"
REPORTS = DOCS / "reports"

# Valuta per exchange (il v2.4 scriveva sempre USD: qui e' corretta)
CURRENCY = {
    "US": "USD", "LSE": "GBX", "PA": "EUR", "XETRA": "EUR",
    "MC": "EUR", "AS": "EUR", "SW": "CHF", "MIL": "EUR", "TSE": "JPY",
}

# Alcuni mercati hanno sigle diverse sull'endpoint bulk: proviamo in ordine.
EXCHANGE_ALIASES = {}   # (Milano e Tokyo non sono coperte da EODHD: si usano gli ADR USA)
RESOLVED = {}   # cache: sigla watchlist -> sigla bulk funzionante


def log(msg=""):
    print(msg, flush=True)


# ============================================================
# WATCHLIST
# ============================================================
def load_watchlist():
    """Legge tickers.csv -> {exchange: {base_ticker: (nome, indice)}}"""
    watch = {}
    with open(ROOT / "tickers.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ticker = row["ticker"].strip()
            if "." not in ticker:
                continue
            base, exchange = ticker.rsplit(".", 1)
            watch.setdefault(exchange, {})[base] = (
                row["name"].strip(), row["index"].strip()
            )
    return watch


# ============================================================
# DOWNLOAD BULK
# ============================================================
def bulk_day(exchange, date=None):
    """Scarica l'intero exchange per un giorno.
    Prova le sigle alternative (EXCHANGE_ALIASES) finche' una risponde.
    Senza 'date' restituisce l'ultimo giorno disponibile.
    Ritorna (data_effettiva, {base_ticker: close})."""
    candidates = ([RESOLVED[exchange]] if exchange in RESOLVED
                  else EXCHANGE_ALIASES.get(exchange, [exchange]))
    last_err = None
    for code in candidates:
        params = {"api_token": API_KEY, "fmt": "json"}
        if date:
            params["date"] = date
        try:
            r = requests.get(f"{BASE}/eod-bulk-last-day/{code}",
                             params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            last_err = e
            continue
        if not isinstance(data, list) or not data:
            continue
        RESOLVED[exchange] = code
        if code != exchange:
            log(f"        (nota: per {exchange} il bulk usa la sigla '{code}')")
        prices, day = {}, None
        for item in data:
            tick = str(item.get("code", "")).strip()
            close = item.get("adjusted_close") or item.get("close")
            d = item.get("date")
            if not tick or close is None:
                continue
            try:
                close = float(close)
            except (TypeError, ValueError):
                continue
            if close <= 0:
                continue
            prices[tick] = close
            if d and (day is None or d > day):
                day = d
        return day, prices
    if last_err:
        raise last_err
    return None, {}


def diagnose_exchange(keywords):
    """Chiede a EODHD la lista ufficiale degli exchange e logga
    quelli che corrispondono alle parole chiave (per capire la sigla giusta)."""
    try:
        r = requests.get(f"{BASE}/exchanges-list/",
                         params={"api_token": API_KEY, "fmt": "json"},
                         timeout=TIMEOUT)
        r.raise_for_status()
        for ex in r.json():
            blob = (str(ex.get("Name", "")) + " " +
                    str(ex.get("Country", ""))).lower()
            if any(k in blob for k in keywords):
                log(f"        possibile sigla: {ex.get('Code'):8} "
                    f"{ex.get('Name')} ({ex.get('Country')})")
    except Exception as e:
        log(f"        (diagnostica non riuscita: {str(e)[:80]})")


def previous_trading_day(exchange, curr_date):
    """Trova il giorno di borsa precedente a curr_date per l'exchange:
    prova all'indietro (salta i weekend) finche' il bulk risponde con dati."""
    d = datetime.fromisoformat(curr_date).date()
    for _ in range(7):
        d = d - timedelta(days=1)
        if d.weekday() >= 5:          # sabato/domenica
            continue
        day, prices = bulk_day(exchange, d.isoformat())
        if prices:
            return day or d.isoformat(), prices
    return None, {}


# ============================================================
# SCANSIONE
# ============================================================
def scan():
    if not API_KEY:
        log("ERRORE: variabile d'ambiente EOD_API_KEY mancante.")
        sys.exit(1)

    start = datetime.now()
    log("=" * 70)
    log("STOCK MONITOR v3.0 - SCANSIONE BULK")
    log("=" * 70)
    log(f"Avvio: {start:%Y-%m-%d %H:%M:%S}   Soglia alert: {ALERT_THRESHOLD}%")

    watch = load_watchlist()
    total_tickers = sum(len(v) for v in watch.values())
    log(f"Watchlist: {total_tickers} ticker su {len(watch)} mercati\n")

    results, errors, missing = [], [], []
    global_trade_date = None

    for exchange in sorted(watch):
        names = watch[exchange]
        try:
            curr_date, curr = bulk_day(exchange)              # ultimo giorno
            if not curr:
                raise RuntimeError("nessun dato ricevuto")
            prev_date, prev = previous_trading_day(exchange, curr_date)
            if not prev:
                raise RuntimeError("giorno precedente non trovato")
        except Exception as e:
            log(f"[{exchange:6}] ERRORE: {str(e)[:120]}")
            errors.append(exchange)
            if exchange == "MIL":
                diagnose_exchange(["milan", "italy", "borsa"])
            elif exchange == "TSE":
                diagnose_exchange(["tokyo", "japan"])
            continue

        found = 0
        for base, (name, index) in names.items():
            pc, cc = prev.get(base), curr.get(base)
            if not pc or not cc:
                missing.append(f"{base}.{exchange}")
                continue
            pct = (cc / pc - 1.0) * 100.0
            if abs(pct) > MAX_ABS_CHANGE:                     # sanity check
                continue
            found += 1
            results.append({
                "ticker": f"{base}.{exchange}", "name": name,
                "index": index, "exchange": exchange,
                "currency": CURRENCY.get(exchange, "USD"),
                "prev_close": round(pc, 4), "curr_close": round(cc, 4),
                "pct": round(pct, 2),
                "prev_date": prev_date, "curr_date": curr_date,
            })
        if global_trade_date is None or (curr_date and curr_date > global_trade_date):
            global_trade_date = curr_date
        log(f"[{exchange:6}] {prev_date} -> {curr_date}   "
            f"{found}/{len(names)} ticker trovati")

    trade_date = global_trade_date or datetime.now().date().isoformat()

    # ---- Alert (con dedup base-ticker, come v2.4) ----
    results.sort(key=lambda x: x["pct"])
    seen, alerts = set(), []
    for r in results:
        if r["pct"] > ALERT_THRESHOLD:
            break
        if r["ticker"] not in seen:
            seen.add(r["ticker"])
            alerts.append(r)

    # ---- CSV: formato identico al v2.4 ----
    REPORTS.mkdir(parents=True, exist_ok=True)
    csv_name = f"alert_report_{trade_date}.csv"
    with open(REPORTS / csv_name, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Ticker", "Nome", "Indice", "Valuta",
                    "Close Prec", "Close Att", "Var %"])
        for a in alerts:
            w.writerow([a["ticker"], a["name"], a["exchange"], a["currency"],
                        f"{a['prev_close']:.2f}", f"{a['curr_close']:.2f}",
                        f"{a['pct']:.2f}%"])

    # ---- data.json per la dashboard ----
    DOCS.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "trade_date": trade_date,
        "threshold": ALERT_THRESHOLD,
        "total_watchlist": total_tickers,
        "total_analyzed": len(results),
        "alerts_count": len(alerts),
        "errors": errors,
        "missing": sorted(missing),
        "csv": f"reports/{csv_name}",
        "results": results,
    }
    (DOCS / "data.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    # ---- history.json (storico compatto) ----
    hist_file = DOCS / "history.json"
    history = []
    if hist_file.exists():
        try:
            history = json.loads(hist_file.read_text(encoding="utf-8"))
        except Exception:
            history = []
    history = [h for h in history if h.get("date") != trade_date]
    history.append({
        "date": trade_date,
        "count": len(alerts),
        "worst": alerts[0]["ticker"] if alerts else None,
        "worst_pct": alerts[0]["pct"] if alerts else None,
        "alerts": [{"t": a["ticker"], "n": a["name"], "p": a["pct"]}
                   for a in alerts],
        "csv": f"reports/{csv_name}",
    })
    history = sorted(history, key=lambda h: h["date"])[-HISTORY_DAYS:]
    hist_file.write_text(json.dumps(history, ensure_ascii=False),
                         encoding="utf-8")

    # ---- Riepilogo console ----
    elapsed = (datetime.now() - start).total_seconds()
    log("\n" + "=" * 70)
    log("ANALISI COMPLETATA")
    log("=" * 70)
    log(f"Tempo: {elapsed:.0f}s   Analizzati: {len(results)}/{total_tickers}"
        f"   Alert: {len(alerts)}")
    if alerts:
        log(f"\nTITOLI CON PERDITE <= {ALERT_THRESHOLD}%  ({trade_date}):")
        for a in alerts[:20]:
            log(f"  {a['ticker']:15} {a['name'][:32]:32} {a['pct']:+7.2f}%")
        if len(alerts) > 20:
            log(f"  ... e altri {len(alerts) - 20} (vedi CSV)")
        log(f"\nReport: docs/reports/{csv_name}")
    else:
        log("\nNessun alert oggi.")
    if missing:
        log(f"\nTicker senza dati ({len(missing)}): {', '.join(sorted(missing))}")
    if errors:
        log(f"\nATTENZIONE - mercati non scaricati: {', '.join(errors)}")
    log("=" * 70)


if __name__ == "__main__":
    scan()
