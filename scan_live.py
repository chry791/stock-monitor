#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Anteprima Stock Monitor con data e ora verificate per ogni quotazione.

Scrive solo docs/live.json: non modifica il registro e non invia ordini.
Quote live EODHD ritardate di circa 15-20 minuti.
"""
import csv
import json
import math
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

API_KEY = os.environ.get("EOD_API_KEY", "").strip()
MARKETS = os.environ.get("MARKETS", "auto").strip().lower()
EU_EXCHANGES = ("LSE", "PA", "XETRA", "MC", "AS", "SW")
EXCHANGE_TZ = {"US": "America/New_York", "LSE": "Europe/London",
               "PA": "Europe/Paris", "XETRA": "Europe/Berlin",
               "MC": "Europe/Madrid", "AS": "Europe/Amsterdam",
               "SW": "Europe/Zurich"}
LIVE_THRESHOLD = float(os.environ.get("LIVE_THRESHOLD", "-5.0"))
MAX_ABS_CHANGE = 50.0
MAX_AGE_MINUTES = 90
MAX_FUTURE_SECONDS = 300
BATCH = 16
TIMEOUT = 30
BASE = "https://eodhd.com/api"
ROOT = Path(__file__).parent
DOCS = ROOT / "docs"


def log(msg=""):
    print(msg, flush=True)


def resolve_markets(now=None):
    if MARKETS in ("us", "usa"):
        return "USA"
    if MARKETS in ("eu", "europa", "europe"):
        return "Europa"
    now = now or datetime.now(timezone.utc)
    hour = now.astimezone(ZoneInfo("Europe/Madrid")).hour
    return "Europa" if 12 <= hour < 18 else "USA"


def load_watchlist(market):
    wanted = EU_EXCHANGES if market == "Europa" else ("US",)
    out, seen = [], set()
    with open(ROOT / "tickers.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ticker = row["ticker"].strip()
            if "." not in ticker or ticker in seen:
                continue
            base, exchange = ticker.rsplit(".", 1)
            if exchange in wanted:
                out.append((base, exchange, row["name"].strip()))
                seen.add(ticker)
    return out


def load_reference_prices():
    try:
        data = json.loads((DOCS / "data.json").read_text(encoding="utf-8"))
        return {r["ticker"]: r for r in data.get("results", [])}
    except (OSError, ValueError, KeyError, TypeError):
        return {}


def validate_quote(item, now):
    """timestamp EODHD: secondi Unix UTC; NON sommare gmtoffset.

    L'ora salvata e' quella dichiarata dal fornitore, non quella del run.
    """
    if not isinstance(item, dict):
        return None, "invalid_payload"
    code = str(item.get("code", "")).strip()
    exchange = code.rsplit(".", 1)[-1]
    tz_name = EXCHANGE_TZ.get(exchange)
    if tz_name is None:
        return None, "unknown_exchange"
    try:
        raw_ts = item.get("timestamp")
        if isinstance(raw_ts, bool):
            raise ValueError("boolean timestamp")
        ts = float(raw_ts)
        if not math.isfinite(ts) or ts <= 0:
            raise ValueError("invalid timestamp")
        quoted_at = datetime.fromtimestamp(ts, timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None, "missing_or_invalid_timestamp"
    age = (now - quoted_at).total_seconds()
    if age < -MAX_FUTURE_SECONDS:
        return None, "future_timestamp"
    tz = ZoneInfo(tz_name)
    quote_date = quoted_at.astimezone(tz).date()
    if quote_date != now.astimezone(tz).date():
        return None, "old_date"
    if age > MAX_AGE_MINUTES * 60:
        return None, "old_timestamp"
    try:
        close = float(item.get("close"))
        prev = float(item.get("previousClose"))
        if not all(math.isfinite(x) and x > 0 for x in (close, prev)):
            raise ValueError("invalid price")
        pct = (close / prev - 1) * 100
        if not math.isfinite(pct):
            raise ValueError("invalid return")
    except (TypeError, ValueError, OverflowError):
        return None, "invalid_price"
    if abs(pct) > MAX_ABS_CHANGE:
        return None, "anomalous_change"
    return {"ticker": code, "price": close, "prev_close": prev,
            "pct": pct, "timestamp": int(ts),
            "quote_time": quoted_at.isoformat(timespec="seconds"),
            "quote_date": quote_date.isoformat(), "exchange_timezone": tz_name,
            "quote_age_minutes": round(max(0, age) / 60, 1)}, None


def estimate_distmax(quote, reference):
    """Non applicare due volte il calo quando data.json e' gia' di oggi."""
    if not reference:
        return None
    try:
        ref_date = reference["curr_date"]
        ref_close = float(reference["curr_close"])
        dm = float(reference["dist_max"])
        if (not isinstance(ref_date, str) or ref_date > quote["quote_date"]
                or not math.isfinite(ref_close) or ref_close <= 0
                or not math.isfinite(dm) or not -100 < dm <= 0):
            return None
        if ref_date < quote["quote_date"] and not math.isclose(
                ref_close, quote["prev_close"], rel_tol=0.002, abs_tol=0.0001):
            return None
        high = max(ref_close / (1 + dm / 100), quote["price"])
        return round((quote["price"] / high - 1) * 100, 1)
    except (KeyError, TypeError, ValueError, ZeroDivisionError, OverflowError):
        return None


def fetch_live(tickers, now=None):
    now = now or datetime.now(timezone.utc)
    prices, rejected, received = {}, Counter(), set()
    failed_batches = 0
    for i in range(0, len(tickers), BATCH):
        chunk = tickers[i:i + BATCH]
        params = {"api_token": API_KEY, "fmt": "json"}
        if len(chunk) > 1:
            params["s"] = ",".join(chunk[1:])
        try:
            response = requests.get(f"{BASE}/real-time/{chunk[0]}",
                                    params=params, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            # Il messaggio dell'eccezione potrebbe contenere l'URL con api_token.
            failed_batches += 1
            log(f"Blocco {i // BATCH + 1}: richiesta fallita ({type(exc).__name__}).")
            continue
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            failed_batches += 1
            continue
        for item in data:
            code = str(item.get("code", "")).strip() if isinstance(item, dict) else ""
            if code not in chunk:
                rejected["unexpected_symbol"] += 1
                continue
            received.add(code)
            quote, reason = validate_quote(item, now)
            if reason:
                rejected[reason] += 1
                continue
            if code not in prices or quote["timestamp"] > prices[code]["timestamp"]:
                prices[code] = quote
    return prices, {"requested": len(tickers), "received": len(received),
                    "failed_batches": failed_batches, "rejected": dict(rejected)}


def main():
    if not API_KEY:
        log("ERRORE: EOD_API_KEY mancante nei Secrets GitHub.")
        sys.exit(1)
    start = datetime.now(timezone.utc)
    market = resolve_markets(start)
    log(f"STOCK MONITOR - ANTEPRIMA {market} (date verificate)")
    watch = load_watchlist(market)
    full = [f"{b}.{e}" for b, e, _ in watch]
    names = {f"{b}.{e}": n for b, e, n in watch}
    references = load_reference_prices()
    prices, quality = fetch_live(full, start)
    alerts = []
    for tick, quote in prices.items():
        if quote["pct"] <= LIVE_THRESHOLD:
            alerts.append({**quote, "pct": round(quote["pct"], 2), "name": names.get(tick, tick),
                           "dist_max": estimate_distmax(quote, references.get(tick))})
    alerts.sort(key=lambda q: q["pct"])
    status = "unavailable" if not prices else ("ok" if len(prices) == len(full) else "partial")
    payload = {"schema_version": 2, "freshness_checked": True,
               "generated_at": start.isoformat(timespec="seconds"),
               "market": market, "threshold": LIVE_THRESHOLD,
               "max_quote_age_minutes": MAX_AGE_MINUTES,
               "status": status, "analyzed": len(prices), "count": len(alerts),
               "quality": quality, "results": alerts}
    DOCS.mkdir(parents=True, exist_ok=True)
    path = DOCS / "live.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    tmp.replace(path)
    log(f"Prezzi ricevuti: {quality['received']}/{len(full)}; utilizzabili: {len(prices)}.")
    log(f"Esclusioni: {json.dumps(quality['rejected'], ensure_ascii=False)}")
    log(f"Stato anteprima: {status}; alert con data verificata: {len(alerts)}.")
    if status != "ok":
        log("::warning::Copertura live incompleta: leggere lo stato sul dashboard.")
    for quote in alerts[:20]:
        log(f"  {quote['ticker']:12} {quote['pct']:+7.2f}%  quotazione {quote['quote_time']}")
    log("Scritto solo docs/live.json. Candidati e registro dipendono dalla Scansione giornaliera.")


if __name__ == "__main__":
    main()
