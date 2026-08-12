#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STOCK MONITOR v3.4 - Scansione Bulk EODHD
==========================================
Novita' della v3.4 (rispetto alla v3.3):
  IL LABORATORIO DEL SEGNALE - registro automatico degli alert:
    - ogni alert (comprato o no) viene salvato in docs/registro.json
      con data, prezzo, distanza dal massimo e colore del semaforo;
    - le scansioni successive, usando i prezzi che scaricano comunque,
      registrano cosa fa ogni titolo dopo 5, 10 e 20 giorni di borsa
      (ZERO chiamate API aggiuntive);
    - il dashboard mostra solo le statistiche aggregate (rimbalzo medio,
      % positivi, per fascia "dal max") in un riquadro compatto.
  Scopo: misurare con dati veri il valore del segnale -5%
  (vedi MANUALE_STRATEGIA.md, capitolo 10 - protocollo Volume 2).

Novita' della v3.3:
  LE SPIE DELL'ORSO - quattro pre-allarmi accanto al semaforo:
    1. TENDENZA:   S&P 500 (SPY) sotto la sua media a 200 giorni
    2. AMPIEZZA:   % dei titoli della watchlist sopra la propria media 200
                   (sotto il 40% = struttura interna che si rompe)
    3. NERVOSISMO: media degli alert giornalieri sugli ultimi 5 giorni
                   (tanti campanelli = mercato nervoso sotto la superficie)
    4. VIX:        l'indice della paura (sopra 25 = tensione alta)
  Le prime tre usano dati gia' scaricati (zero chiamate API in piu');
  il VIX costa 1 chiamata. Le spie non prevedono nulla: annunciano.
  Il semaforo resta l'unico che decide le dosi.

Novita' della v3.2:
  1. SEMAFORO DI REGIME (S&P 500 vs massimo 52 settimane):
       verde  = meno di -5%   -> strategia normale
       giallo = tra -5% e -15% -> tranche dimezzate, prudenza
       rosso  = oltre -15%    -> stop singoli titoli, riserva per l'indice
  2. DISTANZA DAL MASSIMO per ogni alert (campo "dist_max").

Il resto e' identico alla v3.0: stessa watchlist, stessa soglia -5%,
stesso formato CSV, stessi file di output.

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

# Soglie del semaforo di regime (distanza S&P 500 dal max 52 settimane)
REGIME_YELLOW = -5.0           # sotto questa % scatta il giallo
REGIME_RED = -15.0             # sotto questa % scatta il rosso
REGIME_TICKER = "SPY"          # ETF S&P 500, presente nel bulk USA

# Soglie delle spie dell'orso (v3.3)
BREADTH_ALERT = 40.0           # ampiezza: accesa sotto il 40% sopra media 200
NERVOUS_ALERT = 15.0           # nervosismo: accesa con media alert 5gg >= 15
NERVOUS_DAYS = 5               # giorni per la media del nervosismo
VIX_TICKER = "VIX.INDX"        # indice della paura
VIX_ALERT = 25.0               # VIX: accesa da 25 in su
BREADTH_MIN = 100              # minimo titoli validi per calcolare l'ampiezza

# Registro del segnale (v3.4)
LAB_HORIZONS = (5, 10, 20)     # giorni di borsa a cui misurare gli esiti
LAB_MAX_ENTRIES = 5000         # massimo alert conservati nel registro
LAB_CALENDAR_DAYS = 150        # giorni di borsa ricordati per contare i giorni

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
    Chiede il formato "extended" (che include hi_250d e le medie mobili
    ema_50/ema_200); se per qualche motivo fallisse, riprova senza.
    Senza 'date' restituisce l'ultimo giorno disponibile.
    Ritorna (data, {tick: close}, {"hi": max52w, "e50": ema50, "e200": ema200})."""
    candidates = ([RESOLVED[exchange]] if exchange in RESOLVED
                  else EXCHANGE_ALIASES.get(exchange, [exchange]))
    last_err = None
    for code in candidates:
        for extended in (True, False):
            params = {"api_token": API_KEY, "fmt": "json"}
            if extended:
                params["filter"] = "extended"
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
            extra = {"hi": {}, "e50": {}, "e200": {}}
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
                for key, fields in (("hi", ("hi_250d", "HI_250D")),
                                    ("e50", ("ema_50", "EMA_50")),
                                    ("e200", ("ema_200", "EMA_200"))):
                    raw = None
                    for f in fields:
                        raw = item.get(f)
                        if raw is not None:
                            break
                    try:
                        v = float(raw or 0)
                        if v > 0:
                            extra[key][tick] = v
                    except (TypeError, ValueError):
                        pass
                if d and (day is None or d > day):
                    day = d
            return day, prices, extra
    if last_err:
        raise last_err
    return None, {}, {"hi": {}, "e50": {}, "e200": {}}


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


def fetch_vix():
    """Scarica l'ultimo valore del VIX (1 chiamata API).
    Ritorna un float, oppure None se non disponibile."""
    try:
        frm = (datetime.now().date() - timedelta(days=14)).isoformat()
        r = requests.get(f"{BASE}/eod/{VIX_TICKER}",
                         params={"api_token": API_KEY, "fmt": "json",
                                 "from": frm},
                         timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            v = data[-1].get("adjusted_close") or data[-1].get("close")
            v = float(v)
            return v if v > 0 else None
    except Exception as e:
        log(f"        (VIX non disponibile: {str(e)[:80]})")
    return None


def fetch_spy_ma200():
    """Paracadute per la spia Tendenza: se il bulk non fornisce le medie,
    scarica lo storico di SPY (1 chiamata) e calcola la media a 200 giorni.
    Ritorna (ultimo_prezzo, media200) oppure (None, None)."""
    try:
        frm = (datetime.now().date() - timedelta(days=320)).isoformat()
        r = requests.get(f"{BASE}/eod/SPY.US",
                         params={"api_token": API_KEY, "fmt": "json",
                                 "from": frm},
                         timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        closes = []
        for row in data if isinstance(data, list) else []:
            v = row.get("adjusted_close") or row.get("close")
            try:
                v = float(v)
                if v > 0:
                    closes.append(v)
            except (TypeError, ValueError):
                pass
        if len(closes) >= 200:
            return closes[-1], sum(closes[-200:]) / 200.0
    except Exception as e:
        log(f"        (media 200 SPY non calcolabile: {str(e)[:80]})")
    return None, None


def compute_breadth_us(us_tickers):
    """Paracadute per la spia Ampiezza: se il bulk non fornisce le medie,
    scarica lo storico dei titoli USA (1 chiamata per titolo, in parallelo)
    e conta quanti stanno sopra la propria media a 200 giorni.
    Ritorna (sopra, totale)."""
    from concurrent.futures import ThreadPoolExecutor

    frm = (datetime.now().date() - timedelta(days=320)).isoformat()

    def one(base):
        try:
            r = requests.get(f"{BASE}/eod/{base}.US",
                             params={"api_token": API_KEY, "fmt": "json",
                                     "from": frm},
                             timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            closes = []
            for row in data if isinstance(data, list) else []:
                v = row.get("adjusted_close") or row.get("close")
                try:
                    v = float(v)
                    if v > 0:
                        closes.append(v)
                except (TypeError, ValueError):
                    pass
            if len(closes) >= 200:
                return closes[-1] > sum(closes[-200:]) / 200.0
        except Exception:
            pass
        return None

    above = total = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        for res in pool.map(one, us_tickers):
            if res is None:
                continue
            total += 1
            if res:
                above += 1
    return above, total


def update_registro(trade_date, alerts, all_prices, regime_level):
    """Registro del segnale (v3.4).
    - Aggiorna il calendario dei giorni di borsa visti.
    - Completa gli esiti (r5/r10/r20) degli alert passati usando i prezzi
      appena scaricati (zero chiamate API).
    - Aggiunge gli alert di oggi.
    - Ritorna le statistiche aggregate per il dashboard."""
    reg_file = DOCS / "registro.json"
    reg = {"calendar": [], "alerts": []}
    if reg_file.exists():
        try:
            reg = json.loads(reg_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    cal = reg.get("calendar", [])
    if trade_date not in cal:
        cal = sorted(set(cal + [trade_date]))[-LAB_CALENDAR_DAYS:]
    reg["calendar"] = cal
    idx = {d: i for i, d in enumerate(cal)}
    today_i = idx[trade_date]

    # 1) completa gli esiti degli alert passati
    for a in reg.get("alerts", []):
        if a.get("r20") is not None:
            continue                          # gia' completo
        ai = idx.get(a.get("date"))
        if ai is None:
            continue                          # troppo vecchio per il calendario
        elapsed = today_i - ai
        price = all_prices.get(a.get("t"))
        if not price or elapsed <= 0:
            continue
        for n in LAB_HORIZONS:
            key = f"r{n}"
            if elapsed >= n and a.get(key) is None:
                a[key] = round((price / a["p"] - 1.0) * 100.0, 2)

    # 2) aggiunge gli alert di oggi (senza duplicati)
    seen = {(a.get("t"), a.get("date")) for a in reg.get("alerts", [])}
    for al in alerts:
        k = (al["ticker"], trade_date)
        if k in seen:
            continue
        reg["alerts"].append({
            "date": trade_date, "t": al["ticker"], "n": al["name"],
            "p": al["curr_close"], "pct": al["pct"],
            "dm": al.get("dist_max"), "reg": regime_level,
        })
    reg["alerts"] = reg["alerts"][-LAB_MAX_ENTRIES:]
    reg_file.write_text(json.dumps(reg, ensure_ascii=False),
                        encoding="utf-8")

    # 3) statistiche aggregate
    def band(a):
        dm = a.get("dm")
        if dm is None:
            return None
        return "fresco" if dm > -10 else ("intermedio" if dm > -25
                                          else "profondo")

    def stat(entries, key):
        vals = [e[key] for e in entries if e.get(key) is not None]
        if not vals:
            return None
        pos = sum(1 for v in vals if v > 0)
        return {"n": len(vals),
                "avg": round(sum(vals) / len(vals), 2),
                "pos": round(pos / len(vals) * 100.0, 0)}

    entries = reg["alerts"]
    lab = {
        "since": entries[0]["date"] if entries else trade_date,
        "total": len(entries),
        "horizons": {f"r{n}": stat(entries, f"r{n}") for n in LAB_HORIZONS},
        "bands": {},
    }
    for b in ("fresco", "intermedio", "profondo"):
        grp = [e for e in entries if band(e) == b]
        lab["bands"][b] = {"total": len(grp),
                           "r5": stat(grp, "r5")}
    return lab


def previous_trading_day(exchange, curr_date):
    """Trova il giorno di borsa precedente a curr_date per l'exchange:
    prova all'indietro (salta i weekend) finche' il bulk risponde con dati."""
    d = datetime.fromisoformat(curr_date).date()
    for _ in range(7):
        d = d - timedelta(days=1)
        if d.weekday() >= 5:          # sabato/domenica
            continue
        day, prices, _ = bulk_day(exchange, d.isoformat())
        if prices:
            return day or d.isoformat(), prices
    return None, {}


# ============================================================
# SCANSIONE
# ============================================================
def is_split_artifact(ticker, trade_date, pct):
    """Vaccino anti-frazionamento (v3.7, caso Monster 11/8/2026).
    Per i crolli estremi (<= -30%) interroga l'endpoint splits di EODHD:
    se il titolo ha frazionato proprio quel giorno, l'alert e' un artefatto
    del prezzo dimezzato/frazionato e va scartato (il titolo non e' crollato).
    Costo: una chiamata API solo per gli alert estremi (rarissimi)."""
    if pct > -30.0:
        return False
    try:
        r = requests.get(f"{BASE}/splits/{ticker}",
                         params={"api_token": API_KEY, "fmt": "json",
                                 "from": trade_date, "to": trade_date},
                         timeout=20)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and len(data) > 0:
            log(f"   SPLIT rilevato su {ticker} il {trade_date} "
                f"({data[0].get('split','?')}): alert scartato, falso crollo")
            return True
    except Exception:
        pass          # in dubbio non scarto: meglio un falso alert che un buco
    return False


def scan():
    if not API_KEY:
        log("ERRORE: variabile d'ambiente EOD_API_KEY mancante.")
        sys.exit(1)

    start = datetime.now()
    log("=" * 70)
    log("STOCK MONITOR v3.4 - SCANSIONE BULK")
    log("=" * 70)
    log(f"Avvio: {start:%Y-%m-%d %H:%M:%S}   Soglia alert: {ALERT_THRESHOLD}%")

    watch = load_watchlist()
    total_tickers = sum(len(v) for v in watch.values())
    log(f"Watchlist: {total_tickers} ticker su {len(watch)} mercati\n")

    results, errors, missing = [], [], []
    all_prices = {}            # ticker completo -> chiusura odierna (per il registro)
    global_trade_date = None
    regime = None
    spy_trend = None          # spia tendenza: SPY vs media 200
    br_above, br_total = 0, 0  # spia ampiezza: titoli sopra la media 200

    for exchange in sorted(watch):
        names = watch[exchange]
        try:
            curr_date, curr, ex = bulk_day(exchange)          # ultimo giorno
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

        # --- Semaforo di regime + spia tendenza: SPY ---
        if exchange == "US":
            spy_c, spy_h = curr.get(REGIME_TICKER), ex["hi"].get(REGIME_TICKER)
            if spy_c and spy_h:
                rp = round((spy_c / spy_h - 1.0) * 100.0, 1)
                level = ("verde" if rp > REGIME_YELLOW
                         else "giallo" if rp > REGIME_RED
                         else "rosso")
                regime = {"index": "S&P 500 (SPY)", "pct": rp,
                          "level": level, "date": curr_date}
            spy_e200 = ex["e200"].get(REGIME_TICKER)
            if spy_c and spy_e200:
                sopra = spy_c > spy_e200
                spy_trend = {"on": not sopra,
                             "pct": round((spy_c / spy_e200 - 1.0) * 100.0, 1)}

        found = 0
        for base, (name, index) in names.items():
            pc, cc = prev.get(base), curr.get(base)
            if not pc or not cc:
                missing.append(f"{base}.{exchange}")
                continue
            pct = (cc / pc - 1.0) * 100.0
            if abs(pct) > MAX_ABS_CHANGE:                     # sanity check
                continue
            hi = ex["hi"].get(base)
            dist_max = (round((cc / hi - 1.0) * 100.0, 1)
                        if hi and cc <= hi * 1.05 else None)
            e2 = ex["e200"].get(base)                # spia ampiezza
            if e2:
                br_total += 1
                if cc > e2:
                    br_above += 1
            all_prices[f"{base}.{exchange}"] = cc    # per il registro (v3.4)
            found += 1
            results.append({
                "ticker": f"{base}.{exchange}", "name": name,
                "index": index, "exchange": exchange,
                "currency": CURRENCY.get(exchange, "USD"),
                "prev_close": round(pc, 4), "curr_close": round(cc, 4),
                "pct": round(pct, 2),
                "dist_max": dist_max,
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
            if is_split_artifact(r["ticker"], r["curr_date"], r["pct"]):
                continue
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

    # ---- Le spie dell'orso (v3.3) ----
    hist_file = DOCS / "history.json"
    history = []
    if hist_file.exists():
        try:
            history = json.loads(hist_file.read_text(encoding="utf-8"))
        except Exception:
            history = []

    # nervosismo: media alert degli ultimi NERVOUS_DAYS giorni (incluso oggi)
    prev_counts = [h.get("count", 0) for h in sorted(history, key=lambda h: h["date"])
                   if h.get("date") != trade_date][-(NERVOUS_DAYS - 1):]
    media5 = round((sum(prev_counts) + len(alerts)) / (len(prev_counts) + 1), 1)

    breadth_pct = round(br_above / br_total * 100.0, 1) if br_total else None
    vix = fetch_vix()

    # paracadute Ampiezza: se il bulk non ha fornito le medie mobili,
    # calcola l'ampiezza sui titoli USA scaricando i singoli storici
    if breadth_pct is None and "US" in watch:
        log("        (calcolo l'ampiezza dai singoli storici USA: ~1 minuto)")
        ba, bt = compute_breadth_us(sorted(watch["US"].keys()))
        if bt >= BREADTH_MIN:
            breadth_pct = round(ba / bt * 100.0, 1)

    # paracadute Tendenza: se il bulk non ha fornito le medie mobili,
    # calcola la media 200 di SPY dallo storico (1 chiamata in piu')
    if spy_trend is None:
        log("        (medie mobili assenti nel bulk: calcolo la media 200 di SPY)")
        sc, sm = fetch_spy_ma200()
        if sc and sm:
            spy_trend = {"on": sc <= sm,
                         "pct": round((sc / sm - 1.0) * 100.0, 1)}

    spie = {
        "tendenza": ({"on": spy_trend["on"], "pct": spy_trend["pct"]}
                     if spy_trend else None),
        "ampiezza": ({"on": breadth_pct < BREADTH_ALERT, "pct": breadth_pct}
                     if breadth_pct is not None else None),
        "nervosismo": {"on": media5 >= NERVOUS_ALERT, "media": media5,
                       "giorni": len(prev_counts) + 1},
        "vix": ({"on": vix >= VIX_ALERT, "value": round(vix, 1)}
                if vix is not None else None),
    }

    # ---- Registro del segnale (v3.4) ----
    lab = update_registro(trade_date, alerts, all_prices,
                          regime["level"] if regime else None)

    # ---- data.json per la dashboard ----
    DOCS.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "trade_date": trade_date,
        "threshold": ALERT_THRESHOLD,
        "total_watchlist": total_tickers,
        "total_analyzed": len(results),
        "alerts_count": len(alerts),
        "regime": regime,
        "spie": spie,
        "lab": lab,
        "errors": errors,
        "missing": sorted(missing),
        "csv": f"reports/{csv_name}",
        "results": results,
    }
    (DOCS / "data.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    # ---- history.json (storico compatto) ----
    history = [h for h in history if h.get("date") != trade_date]
    history.append({
        "date": trade_date,
        "count": len(alerts),
        "worst": alerts[0]["ticker"] if alerts else None,
        "worst_pct": alerts[0]["pct"] if alerts else None,
        "regime": regime["level"] if regime else None,
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
    if regime:
        log(f"\nSEMAFORO: {regime['level'].upper()}  "
            f"(S&P 500 a {regime['pct']:+.1f}% dal massimo 52 settimane)")
    else:
        log("\nSEMAFORO: non disponibile (SPY non trovato nei dati USA)")
    log("SPIE DELL'ORSO:")
    if spie["tendenza"]:
        st = spie["tendenza"]
        log(f"  Tendenza:   {'ACCESA' if st['on'] else 'spenta '}  "
            f"(SPY a {st['pct']:+.1f}% dalla media 200 giorni)")
    if spie["ampiezza"]:
        sa = spie["ampiezza"]
        log(f"  Ampiezza:   {'ACCESA' if sa['on'] else 'spenta '}  "
            f"({sa['pct']:.0f}% dei titoli sopra la propria media 200)")
    sn = spie["nervosismo"]
    log(f"  Nervosismo: {'ACCESO' if sn['on'] else 'spento '}  "
        f"(media {sn['media']} alert/giorno su {sn['giorni']} giorni)")
    if spie["vix"]:
        sv = spie["vix"]
        log(f"  VIX:        {'ACCESO' if sv['on'] else 'spento '}  "
            f"(valore {sv['value']})")
    else:
        log("  VIX:        n/d")
    r5 = lab["horizons"].get("r5")
    if r5:
        log(f"\nLABORATORIO: {lab['total']} alert registrati dal {lab['since']}"
            f" | dopo 5 giorni: {r5['avg']:+.2f}% medio,"
            f" {r5['pos']:.0f}% positivi (su {r5['n']})")
    else:
        log(f"\nLABORATORIO: {lab['total']} alert registrati"
            f" (primi esiti tra ~5 giorni di borsa)")
    if alerts:
        log(f"\nTITOLI CON PERDITE <= {ALERT_THRESHOLD}%  ({trade_date}):")
        for a in alerts[:20]:
            dm = (f"  (dal max: {a['dist_max']:+.1f}%)"
                  if a.get("dist_max") is not None else "")
            log(f"  {a['ticker']:15} {a['name'][:32]:32} {a['pct']:+7.2f}%{dm}")
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
