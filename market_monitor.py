#!/usr/bin/env python3
"""Stock Monitor Radar. Informational only: no account, positions or orders.

Pure calculations are shared by production, offline replay and verification.
Each series keeps its own date. Missing data never produce a green light.
Requires pandas, numpy, exchange-calendars. Network uses Python stdlib.
"""
import argparse
import csv
import hashlib
import html
import re
import unicodedata
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
import io
import json
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
CALENDARS = {"US": "XNYS", "LSE": "XLON", "PA": "XPAR", "XETRA": "XETR", "MC": "XMAD", "AS": "XAMS", "SW": "XSWX"}
CURRENCIES = {"US": "USD", "LSE": "GBX", "SW": "CHF"}
FRED = {
    "STLFSI4": ("Stress finanziario USA", "settimanale", 18),
    "T10Y2Y": ("Curva Treasury 10–2 anni", "giornaliero", 6),
    "SAHMREALTIME": ("Regola di Sahm", "mensile", 65),
}


def finite(value):
    try:
        return not isinstance(value, bool) and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def number(value, digits=3):
    return round(float(value), digits) if finite(value) else None


def load_json(path, default):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    def scalar(obj):
        if isinstance(obj, np.generic): return obj.item()
        raise TypeError(type(obj).__name__)
    tmp.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), default=scalar), encoding="utf-8")
    os.replace(tmp, path)


def area_for(exchange):
    return "USA" if exchange == "US" else "Regno Unito" if exchange == "LSE" else "Europa"


def watchlist(path):
    with open(path, encoding="utf-8-sig") as f:
        rows = {r["ticker"].strip(): r for r in csv.DictReader(f)}
    return {t: {"ticker": t, "name": r.get("name", t), "exchange": t.rsplit(".", 1)[-1],
                "area": area_for(t.rsplit(".", 1)[-1])} for t, r in rows.items() if "." in t}


def session_context(now, exchanges, delay=30):
    """Most recent completed session + delay, including holidays and early closes.
    Calendars describe the main venue; ADR/OTC individual suspensions remain unknown.
    Failure is explicit; never replace a failed calendar with Monday-Friday guesses.
    """
    import exchange_calendars as xc
    out = {}
    for ex in set(exchanges):
        try:
            cal = xc.get_calendar(CALENDARS[ex])
            sessions = cal.sessions_in_range((now - timedelta(days=850)).date(), now.date())
            completed = [s for s in sessions if cal.session_close(s).to_pydatetime() + timedelta(minutes=delay) <= now]
            dates = [s.strftime("%Y-%m-%d") for s in completed]
            out[ex] = {"expected": dates[-1], "previous": dates[-2], "sessions": dates, "calendar": CALENDARS[ex]}
        except Exception as exc:
            out[ex] = {"expected": None, "previous": None, "sessions": [], "calendar": CALENDARS.get(ex), "error": type(exc).__name__}
    return out


def prepare_series(rows, cutoff, sessions=None):
    """Reject conflicting duplicate dates, invalid prices, future observations.
    Use adjusted close consistently; adjusted OHLC = raw OHLC * adj_close/close.
    """
    df = pd.DataFrame(rows).copy()
    if df.empty or "date" not in df or "adjusted_close" not in df or "close" not in df:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ["close", "adjusted_close", "high", "low", "volume"]:
        if c not in df:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")
    duplicate = df.groupby("date")[["close", "adjusted_close", "high", "low"]].nunique().max(axis=1)
    conflicts = set(duplicate[duplicate > 1].index)
    mask = (df.date.notna() & (df.date <= cutoff) & ~df.date.isin(conflicts)
            & np.isfinite(df.close) & (df.close > 0)
            & np.isfinite(df.adjusted_close) & (df.adjusted_close > 0))
    df = df[mask].drop_duplicates("date").sort_values("date").set_index("date")
    if df.empty:
        return df
    if sessions is not None:
        # Reindex before rolling: a missing session makes the relevant window
        # incomplete instead of silently treating 200 observations as 200 sessions.
        dates = [d for d in sessions if df.index[0] <= d <= cutoff]
        if not dates: return pd.DataFrame()
        df = df.reindex(dates)
        df.index.name = "date"
    adj = df.adjusted_close
    factor = adj / df.close
    valid_hl = (np.isfinite(df.high) & np.isfinite(df.low) & (df.high >= df.close)
                & (df.low <= df.close) & (df.low > 0))
    df["ahigh"] = (df.high * factor).where(valid_hl)
    df["alow"] = (df.low * factor).where(valid_hl)
    df["ret"] = (adj / adj.shift(1) - 1) * 100
    df["raw_ret"] = (df.close / df.close.shift(1) - 1) * 100
    df["ma50"] = adj.rolling(50, min_periods=50).mean()
    df["ma200"] = adj.rolling(200, min_periods=200).mean()
    df["slope200"] = (df.ma200 / df.ma200.shift(20) - 1) * 100
    df["dist50"] = (adj / df.ma50 - 1) * 100
    df["dist200"] = (adj / df.ma200 - 1) * 100
    df["high252"] = df.ahigh.rolling(252, min_periods=252).max()
    df["low252"] = df.alow.rolling(252, min_periods=252).min()
    df["dist_max"] = (adj / df.high252 - 1) * 100
    df["new_high"] = np.where(df.high252.notna(), (df.ahigh >= df.high252).astype(float), np.nan)
    df["new_low"] = np.where(df.low252.notna(), (df.alow <= df.low252).astype(float), np.nan)
    df["rel_volume"] = df.volume / df.volume.shift(1).rolling(20, min_periods=20).mean()
    df["anomaly"] = (df.ret.abs() > 50)
    return df.dropna(subset=["close", "adjusted_close"])


def phase(row):
    if not finite(row.get("dist200")) or not finite(row.get("slope200")):
        return None
    below, falling = row["dist200"] < 0, row["slope200"] < 0
    return "ORSO" if below and falling else "CADUTA" if below else "RIPRESA" if falling else "TORO"


def snapshot(ticker, meta, df, ctx, config):
    result = dict(meta, ticker=ticker, status="missing", asof=None, expected=ctx.get("expected"),
                  reason="Storico assente", alert=False, candidate=False, source="EODHD", price_basis="adjusted")
    if df.empty:
        return result
    d, row = df.index[-1], df.iloc[-1]
    result.update(asof=d, price=number(row.close, 4), pct=number(row.ret), raw_pct=number(row.raw_ret),
                  dist_max=number(row.dist_max), dist50=number(row.dist50), dist200=number(row.dist200),
                  slope200=number(row.slope200), phase=phase(row), rel_volume=number(row.rel_volume),
                  currency=CURRENCIES.get(meta["exchange"], "EUR"), anomaly=bool(row.anomaly), observations=len(df),
                  corporate_adjustment=bool(finite(row.raw_ret) and finite(row.ret) and abs(row.raw_ret - row.ret) > 3))
    if not ctx.get("expected"):
        result.update(status="unknown", reason="Calendario non verificabile")
    elif d != ctx["expected"]:
        result.update(status="stale", reason="Manca la chiusura attesa")
    elif len(df) < 2 or df.index[-2] != ctx["previous"]:
        result.update(status="gap", reason="Manca la seduta precedente")
    elif row.anomaly:
        result.update(status="review", reason="Movimento oltre il 50%: da verificare, conservato")
    else:
        result.update(status="ok", reason=None, alert=bool(row.ret <= config["drop_pct"]),
                      candidate=bool(row.ret <= config["drop_pct"] and finite(row.dist_max)
                                     and row.dist_max <= config["candidate_distance_pct"]))
    result["spark"] = [number(v) for v in df.adjusted_close.tail(35)]
    return result


def region_series(tickers, series, context, config):
    """Fixed watchlist denominator, no filling missing daily prices.
    Returns breadth / pressure history with explicit per-metric coverage.
    """
    dates = sorted(set(d for t in tickers for d in series.get(t, pd.DataFrame()).index))
    dates = dates[-80:]
    metrics = []
    for day in dates:
        values = []
        for t in tickers:
            df = series.get(t)
            if df is None or day not in df.index:
                continue
            r = df.loc[day]
            if not finite(r.ret) or bool(r.anomaly):
                continue
            ex = t.rsplit(".", 1)[-1]
            sess = context.get(ex, {}).get("sessions", [])
            # Exclude returns across a missing exchange session, including suspensions.
            idx = df.index.get_loc(day)
            if sess and day in sess:
                si = sess.index(day)
                if si and (idx == 0 or df.index[idx-1] != sess[si-1]):
                    continue
            values.append(r)
        n = len(values)
        if not n:
            continue
        counts = {k: [r for r in values if finite(r[k])] for k in ("dist50", "dist200", "new_low")}
        m = {"date": day, "n": n, "coverage": number(100*n/len(tickers)),
             "pressure": number(100*sum(r.ret <= config["drop_pct"] for r in values)/n),
             "advance": sum(r.ret > 0 for r in values), "decline": sum(r.ret < 0 for r in values),
             "unchanged": sum(r.ret == 0 for r in values)}
        for field, key in [("dist50", "breadth50"), ("dist200", "breadth200")]:
            valid = counts[field]
            m[key] = number(100*sum(r[field] > 0 for r in valid)/len(valid)) if valid else None
            m[key+"_coverage"] = number(100*len(valid)/len(tickers))
        valid = counts["new_low"]
        m["new_lows"] = number(100*sum(r.new_low for r in valid)/len(valid)) if valid else None
        m["new_highs"] = number(100*sum(r.new_high for r in valid)/len(valid)) if valid else None
        m["new_lows_coverage"] = number(100*len(valid)/len(tickers))
        m["ad_net_pct"] = number(100*(m["advance"]-m["decline"])/n)
        metrics.append(m)
    return metrics


def raw_level(value, rule, previous="normal"):
    if not finite(value):
        return "unknown"
    value = float(value)
    low = rule["direction"] == "low"
    hit = (lambda threshold: value < threshold) if low else (lambda threshold: value >= threshold)
    if hit(rule["stress"]):
        return "stress"
    if hit(rule["attention"]):
        return "attention"
    # Exit margin applies to recovery only; deterioration is shown immediately.
    if previous in ("attention", "stress"):
        boundary = rule["attention"] + rule["exit_margin"] if low else rule["attention"] - rule["exit_margin"]
        if hit(boundary):
            return "attention"
    return "normal"


def indicator(key, title, value, day, quality, rule, previous=None, unit="%", detail="", previous_date=None):
    previous = previous or {}
    level = raw_level(value, rule, previous.get("level")) if quality == "ok" else "unknown"
    same = previous.get("level") == level and (previous_date is None or previous.get("asof") in (day,previous_date))
    sessions = (previous.get("sessions", 1) if previous.get("asof") == day else previous.get("sessions", 1)+1) if same else 1
    return {"key": key, "title": title, "value": number(value), "asof": day, "unit": unit,
            "quality": quality, "level": level, "sessions": sessions,
            "confirmed": level != "unknown" and sessions >= 2,
            "detail": detail, "rule": rule}


def build_report(watch, histories, config, now, context, previous=None, mode="live"):
    previous = previous or {}
    cutoff = now.date().isoformat()
    meta = {**watch, **config["benchmarks"]}
    series = {t: prepare_series(rows, cutoff, context.get(meta.get(t,{}).get("exchange"),{}).get("sessions")) for t, rows in histories.items()}
    stocks = [snapshot(t, m, series.get(t, pd.DataFrame()), context.get(m["exchange"], {}), config) for t, m in watch.items()]
    benchmarks = [snapshot(t, dict(m,ticker=t), series.get(t, pd.DataFrame()), context.get(m["exchange"], {}), config) for t,m in config["benchmarks"].items()]
    old = {i["key"]: i for i in previous.get("indicators", [])}
    indicators, regions = [], []
    for area in ("USA", "Europa", "Regno Unito"):
        members = [t for t, m in watch.items() if m["area"] == area]
        if not members:
            continue
        history = region_series(members, series, context, config)
        expected_dates = {context.get(watch[t]["exchange"], {}).get("expected") for t in members}
        latest = history[-1] if history else {}
        # Mixed exchange holidays remain visible; a combined region receives no light.
        aligned = len(expected_dates) == 1 and None not in expected_dates and latest.get("date") in expected_dates
        valid_stocks = [s for s in stocks if s["area"] == area and s["status"] == "ok"]
        regions.append({"area": area, "total": len(members), "valid": len(valid_stocks), "coverage": number(100*len(valid_stocks)/len(members)),
                        "asof": latest.get("date"), "aligned": aligned, "latest": latest, "history": history,
                        "universe": "Quotazioni della watchlist; include ADR, non componenti storici di un indice"})
        for kind, title in [("breadth50", "Sopra media 50"), ("breadth200", "Sopra media 200"),
                            ("pressure", "Titoli in calo ≥5%"), ("new_lows", "Nuovi minimi 252 sedute")]:
            coverage = latest.get(kind+"_coverage", latest.get("coverage", 0))
            quality = "ok" if aligned and coverage >= config["min_coverage_pct"] and latest.get("n",0) >= config["min_breadth_names"] else "partial"
            key = area+":"+kind
            indicators.append(indicator(key, area+" · "+title, latest.get(kind), latest.get("date"), quality, config["thresholds"][kind], old.get(key),
                                        detail=f"Copertura {number(coverage)}%. Soglia descrittiva sperimentale.",
                                        previous_date=context.get(watch[members[0]]["exchange"],{}).get("previous")))
    for b in benchmarks:
        if b["ticker"] in ("SPY.US", "SXXP.INDX", "FTSE.INDX"):
            key = b["ticker"]+":trend"
            frame=series.get(b["ticker"],pd.DataFrame())
            raw_dist=frame.iloc[-1].dist200 if not frame.empty else None
            indicators.append(indicator(key, b["name"]+" · tendenza", raw_dist, b["asof"], b["status"], config["thresholds"]["trend"], old.get(key), detail="Distanza dalla media semplice a 200 sedute; pendenza su 20 sedute.", previous_date=context.get(b["exchange"],{}).get("previous")))
    bmap = {b["ticker"]: b for b in benchmarks}
    vix = bmap.get("VIX.INDX", {})
    v3 = bmap.get("VIX3M.INDX", {})
    indicators.append(indicator("vix", "VIX · volatilità attesa", vix.get("price"), vix.get("asof"), vix.get("status"), config["thresholds"]["vix"], old.get("vix"), unit="punti", detail="Volatilità implicita a 30 giorni, non probabilità di crollo."))
    ratio_ok = vix.get("status") == v3.get("status") == "ok" and vix.get("asof") == v3.get("asof") and v3.get("price", 0) > 0
    ratio = vix["price"]/v3["price"] if ratio_ok else None
    indicators.append(indicator("vix_ratio", "VIX / VIX3M", ratio, vix.get("asof"), "ok" if ratio_ok else "missing", config["thresholds"]["vix_ratio"], old.get("vix_ratio"), unit="×", detail="Confronto 30 giorni / 3 mesi, stessa data. Non è il rapporto tra futures VIX."))
    leadership = {"status": "missing", "value": None, "asof": None, "label": "RSP / SPY · variazione 20 sedute", "detail": "Rendimento relativo rettificato; riflette anche differenze settoriali e di stile."}
    if bmap.get("RSP.US",{}).get("status") == bmap.get("SPY.US",{}).get("status") == "ok":
        aligned = pd.concat([series["RSP.US"].adjusted_close, series["SPY.US"].adjusted_close],axis=1,keys=["rsp","spy"]).dropna()
        if len(aligned) >= 21 and aligned.index[-1] == bmap["SPY.US"]["asof"]:
            rr = aligned.rsp / aligned.spy
            leadership.update(status="ok", value=number((rr.iloc[-1]/rr.iloc[-21]-1)*100), asof=aligned.index[-1], spark=[number(v,5) for v in rr.tail(60)])
    changes = []
    old_changes={c["key"]:c for c in previous.get("changes",[])}
    for i in indicators:
        p = old.get(i["key"])
        change=old_changes.get(i["key"])
        origin=change.get("from") if change and change.get("asof")==i["asof"] else p.get("level") if p else None
        if origin and origin != i["level"]:
            changes.append({"key":i["key"],"title":i["title"],"from":origin,"to":i["level"],"asof":i["asof"]})
    signature = hashlib.sha256("\n".join(sorted(watch)).encode()).hexdigest()[:12]
    return {"schema_version": 5, "version": config["version"], "mode": mode, "generated_at": now.isoformat(),
            "watchlist_signature": signature, "watchlist_changed": bool(previous.get("watchlist_signature") and previous["watchlist_signature"] != signature),
            "rules": {"drop_pct":config["drop_pct"],"candidate_distance_pct":config["candidate_distance_pct"],"min_coverage_pct":config["min_coverage_pct"]},
            "status":"ok" if all(s["status"]=="ok" for s in stocks) else "partial",
            "stocks": sorted(stocks, key=lambda s:s.get("pct") if s.get("pct") is not None else 999),
            "benchmarks": benchmarks, "regions": regions, "indicators": indicators, "changes": changes,
            "leadership": leadership, "macro": [], "news": [], "news_status":"not_loaded", "usage": {},
            "quality": {"total":len(stocks), "valid":sum(s["status"]=="ok" for s in stocks), "issues":[{"ticker":s["ticker"],"status":s["status"],"asof":s["asof"],"expected":s["expected"],"reason":s["reason"]} for s in stocks if s["status"]!="ok"],
                        "benchmark_issues":[{"ticker":s["ticker"],"status":s["status"],"asof":s["asof"],"expected":s["expected"],"reason":"Riferimento: "+str(s["reason"])} for s in benchmarks if s["status"]!="ok"]},
            "limits": ["Soglie informative, capacità predittiva non validata fuori campione.", "Ampiezza sulla watchlist corrente, non su tutti i titoli del mercato.", "CANDIDATO significa che i filtri sono rispettati; nessuna raccomandazione di acquisto.", "Massimo/minimo: 252 osservazioni OHLC rettificate; variazione basata su chiusure rettificate."]}


class Client:
    def __init__(self, key, interval=0.25):
        self.key, self.interval = key, interval
        self.lock, self.next_request, self.credits, self.requests = threading.Lock(), 0.0, 0, 0

    def get(self, endpoint, params=None, cost=1):
        query = dict(params or {}, api_token=self.key, fmt="json")
        url = "https://eodhd.com/api/" + endpoint + "?" + urllib.parse.urlencode(query)
        for attempt in range(2):
            with self.lock:
                wait = max(0, self.next_request - time.monotonic())
                self.next_request = max(self.next_request, time.monotonic()) + self.interval
                self.requests += 1
                self.credits += cost
            if wait:
                time.sleep(wait)
            try:
                req = urllib.request.Request(url, headers={"User-Agent":"StockMonitor/5 personal research"})
                with urllib.request.urlopen(req, timeout=25) as response:
                    return json.load(response)
            except urllib.error.HTTPError as exc:
                if exc.code in (429, 500, 502, 503, 504) and attempt == 0:
                    time.sleep(min(10, max(1, int(exc.headers.get("Retry-After", "2")) if exc.headers.get("Retry-After", "2").isdigit() else 2)))
                    continue
                return {"error": "http_"+str(exc.code)}
            except Exception as exc:
                if attempt == 0:
                    time.sleep(1)
                    continue
                return {"error": type(exc).__name__}


def public_macro(now):
    out = []
    for code,(name,frequency,maxage) in FRED.items():
        item = {"id":code,"name":name,"frequency":frequency,"source":"https://fred.stlouisfed.org/series/"+code,"status":"missing","asof":None,"value":None}
        try:
            url="https://fred.stlouisfed.org/graph/fredgraph.csv?"+urllib.parse.urlencode({"id":code,"cosd":(now-timedelta(days=400)).date().isoformat()})
            with urllib.request.urlopen(url, timeout=20) as r:
                rows=list(csv.DictReader(io.StringIO(r.read().decode("utf-8-sig"))))
            vals=[]
            for r in rows:
                d=r.get("observation_date",r.get("DATE", ""))
                if d and d<=now.date().isoformat() and finite(r.get(code)):
                    vals.append((d,float(r[code])))
            if vals:
                d,v=sorted(vals)[-1]
                age=(now.date()-datetime.fromisoformat(d).date()).days
                item.update(asof=d,value=v,status="ok" if age<=maxage else "stale",retrieved_at=now.isoformat())
        except Exception as exc:
            item["error"] = type(exc).__name__
        out.append(item)
    return out


# Source labels describe the linked website, not an independent fact check.
NEWS_SOURCE_TYPES = {
    "reuters.com": ("Reuters", "editorial"),
    "apnews.com": ("Associated Press", "editorial"),
    "cnbc.com": ("CNBC", "editorial"),
    "bloomberg.com": ("Bloomberg", "editorial"),
    "ft.com": ("Financial Times", "editorial"),
    "wsj.com": ("The Wall Street Journal", "editorial"),
    "marketwatch.com": ("MarketWatch", "editorial"),
    "sec.gov": ("SEC", "institution"),
    "federalreserve.gov": ("Federal Reserve", "institution"),
    "ecb.europa.eu": ("Banca centrale europea", "institution"),
    "bls.gov": ("Bureau of Labor Statistics", "institution"),
    "ec.europa.eu": ("Commissione europea", "institution"),
    "businesswire.com": ("Business Wire", "press_release"),
    "globenewswire.com": ("GlobeNewswire", "press_release"),
    "finance.yahoo.com": ("Yahoo Finance", "aggregator"),
}

OFFICIAL_NEWS_FEEDS = (
    ("fed_press", "Federal Reserve · comunicati", "https://www.federalreserve.gov/feeds/press_all.xml", "federalreserve.gov"),
    ("fed_speeches", "Federal Reserve · interventi", "https://www.federalreserve.gov/feeds/speeches_and_testimony.xml", "federalreserve.gov"),
    ("ecb_press", "BCE · comunicati e interventi", "https://www.ecb.europa.eu/rss/press.html", "ecb.europa.eu"),
)


def news_topics(tags, has_symbols=False):
    """Descriptive categories from provider tags, never a cause of a price move."""
    normalized = {re.sub(r"[ _]+", "-", str(t).lower()) for t in tags}
    result = []
    groups = {
        "macro": {"economy", "macroeconomics", "inflation", "interest-rates", "monetary-policy", "central-banks", "federal-reserve", "ecb", "gdp", "employment", "tariffs", "trade-policy"},
        "earnings": {"earnings", "earnings-report", "earnings-results", "earnings-release", "earnings-per-share", "quarterly-results", "financial-results", "guidance", "earnings-surprise"},
        "crypto": {"crypto", "cryptocurrency", "bitcoin", "ethereum", "digital-assets"},
        "markets": {"markets", "stock-market", "bonds", "treasuries", "forex", "commodities", "oil", "gold"},
    }
    for category, terms in groups.items():
        if normalized & terms:
            result.append(category)
    return result or (["companies"] if has_symbols else ["markets"])


def parse_official_feed(data, feed_url):
    """Parse RSS/Atom headlines, dates and links. Never publish article bodies."""
    if len(data) > 1024 * 1024 or b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
        raise ValueError("unsupported_feed")
    root = ET.fromstring(data)
    local = lambda name: name.rsplit("}", 1)[-1]
    if local(root.tag) not in {"rss", "RDF", "feed"}:
        raise ValueError("not_a_feed")
    rows = []
    for entry in (x for x in root.iter() if local(x.tag) in {"item", "entry"}):
        fields = {}
        links = []
        for child in entry:
            name = local(child.tag)
            fields.setdefault(name, "".join(child.itertext()).strip())
            if name == "link" and child.attrib.get("rel", "alternate") == "alternate":
                links.append(child.attrib.get("href") or fields[name])
        value = fields.get("pubDate") or fields.get("published") or fields.get("date") or fields.get("updated", "")
        try:
            try:
                when = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                when = parsedate_to_datetime(value)
            value = when.isoformat() if when.tzinfo is not None else ""
        except (ValueError, TypeError, OverflowError):
            value = ""
        rows.append({"title": fields.get("title", ""), "date": value,
                     "link": urllib.parse.urljoin(feed_url, links[0]) if links else "", "symbols": []})
        if len(rows) >= 100:
            break
    return rows


def fetch_official_news(config, now):
    def fetch(feed):
        ident, name, url, domain = feed
        status = {"id": ident, "name": name, "url": url, "status": "ok", "received": 0, "accepted": 0, "rejected": {}}
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "StockMonitor/5 personal research", "Accept": "application/rss+xml, application/atom+xml, text/xml"})
            with urllib.request.urlopen(req, timeout=12) as response:
                final = urllib.parse.urlsplit(response.geturl())
                if final.scheme != "https" or not ((final.hostname or "") == domain or (final.hostname or "").endswith("." + domain)):
                    raise ValueError("unexpected_feed_redirect")
                data = response.read(1024 * 1024 + 1)
            rows = parse_official_feed(data, url)
            accepted, rejected, _ = filter_news(rows, [domain], now, max_age_days=news_settings(config)["days"])
            for item in accepted:
                item.update(topics=["macro"], retrieval_sources=[ident])
            status.update(received=len(rows), accepted=len(accepted), rejected=rejected)
            return accepted, status
        except urllib.error.HTTPError as exc:
            status.update(status="error", error="http_" + str(exc.code))
        except Exception as exc:
            status.update(status="error", error=type(exc).__name__)
        return [], status
    if not config.get("news_official_feeds", True):
        return [], []
    records, statuses = [], []
    with ThreadPoolExecutor(max_workers=3) as pool:
        for items, status in pool.map(fetch, OFFICIAL_NEWS_FEEDS):
            records.extend(items)
            statuses.append(status)
    return records, statuses


def deduplicate_news(records):
    """Merge canonical URLs and identical long headlines on the same UTC day."""
    unique, urls, titles = [], {}, {}
    ranks = {"institution": 0, "editorial": 1, "press_release": 2, "aggregator": 3}
    for item in sorted(records, key=lambda n: (n["date"], n["url"]), reverse=True):
        item = {**item}
        p = urllib.parse.urlsplit(item["url"])
        host = "finance.yahoo.com" if p.hostname == "finance.yahoo.com" or (p.hostname or "").endswith(".finance.yahoo.com") else p.hostname
        key = urllib.parse.urlunsplit((p.scheme, host or "", p.path, p.query, ""))
        normalized = " ".join(re.findall(r"\w+", unicodedata.normalize("NFKC", item["title"]).casefold()))
        title_key = (normalized, item["date"][:10]) if len(normalized) >= 50 else None
        index = urls.get(key)
        if index is None and title_key:
            index = titles.get(title_key)
        if index is None:
            index = len(unique)
            unique.append(item)
        else:
            old = unique[index]
            winner = item if ranks.get(item["source_type"], 9) < ranks.get(old["source_type"], 9) else old
            merged = {**winner}
            for field in ("tickers", "topics", "retrieval_sources"):
                merged[field] = sorted(set(old.get(field, [])) | set(item.get(field, [])))
            unique[index] = merged
        urls[key] = index
        if title_key:
            titles[title_key] = index
    for item in unique:
        item["ticker"] = item["tickers"][0] if len(item.get("tickers", [])) == 1 else None
    return sorted(unique, key=lambda n: (n["date"], n["url"]), reverse=True)


def news_settings(config):
    # Bound the work and include every possible retry in the credit estimate.
    return {
        "page_size": max(1, min(1000, int(config.get("news_page_size", 100)))),
        "max_pages": max(1, min(3, int(config.get("news_max_pages", 3)))),
        "days": max(1, min(7, int(config.get("news_lookback_days", 3)))),
        "max_tickers": max(0, min(20, int(config.get("news_max_tickers", 20)))),
        "max_items": max(50, min(500, int(config.get("news_max_items", 300)))),
        "target_items": max(1, min(100, int(config.get("news_target_items", 50)))),
    }


def filter_news(rows, allowed, now, ticker=None, max_age_days=7):
    """Reject untrusted URLs and invalid dates; keep headlines and metadata only."""
    out = []
    rejected = {}
    blocked_domains = {}
    allowed = sorted({str(d).lower().strip() for d in allowed if d}, key=len, reverse=True)

    def reject(reason, host=None):
        rejected[reason] = rejected.get(reason, 0) + 1
        if host:
            blocked_domains[host] = blocked_domains.get(host, 0) + 1

    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            reject("invalid_record")
            continue
        url = row.get("link")
        try:
            if not isinstance(url, str) or any(ord(c) <= 32 for c in url) or "\\" in url:
                raise ValueError("URL")
            p = urllib.parse.urlsplit(url)
            host = (p.hostname or "").lower()
            if p.scheme != "https" or not host or p.username is not None or p.password is not None or p.port not in (None, 443):
                raise ValueError("URL")
        except (TypeError, ValueError):
            reject("invalid_url")
            continue
        domain = next((d for d in allowed if host == d or host.endswith("." + d)), None)
        if not domain:
            reject("source_not_allowed", host)
            continue
        try:
            when = datetime.fromisoformat(str(row.get("date", "")).replace("Z", "+00:00"))
            if when.tzinfo is None:
                raise ValueError("timezone")
            age = (now - when).total_seconds()
        except (TypeError, ValueError, OverflowError):
            reject("invalid_date")
            continue
        if age < 0:
            reject("future_date")
            continue
        if age > max_age_days * 86400:
            reject("outside_window")
            continue
        title = row.get("title")
        if not isinstance(title, str) or not title.strip():
            reject("missing_title")
            continue
        # HTML entities occur in EODHD headlines; the dashboard still escapes all text.
        title = " ".join(html.unescape(title).split())[:300]
        symbols = row.get("symbols", [])
        symbols = sorted({s.upper() for s in symbols if isinstance(s, str) and 1 <= len(s) <= 32}) if isinstance(symbols, list) else []
        query = urllib.parse.urlencode([(k, v) for k, v in urllib.parse.parse_qsl(p.query, keep_blank_values=True)
                                       if not k.lower().startswith("utm_") and k.lower() not in ("guccounter", "guce_referrer", "guce_referrer_sig")])
        url = urllib.parse.urlunsplit(("https", host, p.path or "/", query, ""))
        publisher, kind = NEWS_SOURCE_TYPES.get(domain, (host, "allowed_source"))
        tags = row.get("tags", [])
        tags = [t[:80] for t in tags[:40] if isinstance(t, str)] if isinstance(tags, list) else []
        tag_keys = {re.sub(r"[ _]+", "-", t.lower()) for t in tags}
        content_kind = "press_release" if kind == "press_release" or tag_keys & {"press-releases", "press-release", "company-announcement", "european-regulatory-news"} else kind
        out.append({"title": title, "url": url, "source": host, "publisher": publisher,
                    "source_type": kind, "date": when.astimezone(timezone.utc).isoformat(),
                    "symbols": symbols, "ticker": ticker if ticker in symbols else None,
                    "original_publisher": None if kind == "aggregator" else publisher,
                    "content_kind": content_kind, "kind_basis": "feed_tags" if content_kind != kind else "domain",
                    "topics": news_topics(tags, bool(symbols)), "retrieval_sources": ["eodhd"]})
    return out, rejected, blocked_domains


def safe_news(rows, allowed, now, ticker=None):
    # Compatibility with older offline tools.
    return filter_news(rows, allowed, now, ticker)[0]


def collect_news(client, config, alerts, now):
    settings = news_settings(config)
    all_targets = list(dict.fromkeys(alerts))
    targets = all_targets[:settings["max_tickers"]]
    known_alerts = set(all_targets)
    records = []
    diagnostics = {
        "started_at": now.isoformat(), "lookback_days": settings["days"],
        "requested_tickers": targets, "alerts_total": len(all_targets),
        "alerts_not_queried": all_targets[len(targets):],
        "requests": 0, "successful_requests": 0, "received": 0,
        "eligible_records": 0, "rejected": {}, "excluded_domains": {},
        "errors": [], "limited_queries": [],
        "allowed_domains": config["news_allowed_domains"],
        "target_items": settings["target_items"],
    }
    for ticker in [None] + targets:
        for page in range(settings["max_pages"]):
            params = {"limit": settings["page_size"], "offset": page * settings["page_size"],
                      "from": (now - timedelta(days=settings["days"])).date().isoformat()}
            if ticker:
                params["s"] = ticker
            raw = client.get("news", params, cost=5)
            diagnostics["requests"] += 1
            if not isinstance(raw, list):
                # Do not publish arbitrary upstream error strings, URLs or credentials.
                code = raw.get("error") if isinstance(raw, dict) else None
                code = code if code in {"http_401", "http_402", "http_403", "http_429", "http_500", "http_502", "http_503", "http_504"} else "invalid_response"
                diagnostics["errors"].append({"query": ticker or "market", "code": code})
                break
            diagnostics["successful_requests"] += 1
            diagnostics["received"] += len(raw)
            accepted, rejected, domains = filter_news(raw, config["news_allowed_domains"], now, ticker, settings["days"])
            diagnostics["eligible_records"] += len(accepted)
            for name, counts in (("rejected", rejected), ("excluded_domains", domains)):
                for key, value in counts.items():
                    diagnostics[name][key] = diagnostics[name].get(key, 0) + value
            for item in accepted:
                # Exact provider symbol tags only: never infer a cause or company from text.
                item["tickers"] = sorted(known_alerts.intersection(item.pop("symbols")))
                item["ticker"] = item["tickers"][0] if len(item["tickers"]) == 1 else None
                records.append(item)
            if len(raw) < settings["page_size"]:
                break
            if page + 1 == settings["max_pages"]:
                diagnostics["limited_queries"].append(ticker or "market")
    official, statuses = fetch_official_news(config, now)
    diagnostics["official_feeds"] = statuses
    diagnostics["eodhd_received"] = diagnostics["received"]
    for item in official:
        item.pop("symbols", None)
        item.update(tickers=[], ticker=None)
        records.append(item)
    for status in statuses:
        diagnostics["received"] += status["received"]
        diagnostics["eligible_records"] += status["accepted"]
        for key, count in status["rejected"].items():
            diagnostics["rejected"][key] = diagnostics["rejected"].get(key, 0) + count
    all_news = deduplicate_news(records)
    priority = [n for n in all_news if n.get("tickers") or n["source_type"] == "institution"][:settings["max_items"]]
    selected_urls = {n["url"] for n in priority}
    remaining = [n for n in all_news if n["url"] not in selected_urls]
    news = sorted(priority + remaining[:settings["max_items"]-len(priority)], key=lambda n: (n["date"], n["url"]), reverse=True)
    diagnostics["admitted"] = len(news)
    diagnostics["unique_available"] = len(all_news)
    diagnostics["not_published_due_to_limit"] = len(all_news) - len(news)
    diagnostics["target_met"] = len(news) >= settings["target_items"]
    diagnostics["duplicates_removed"] = diagnostics["eligible_records"] - len(all_news)
    diagnostics["finished_at"] = datetime.now(timezone.utc).isoformat()
    failed_rss = any(s["status"] == "error" for s in statuses)
    if diagnostics["errors"] or failed_rss:
        status = "partial" if diagnostics["successful_requests"] or any(s["status"] == "ok" for s in statuses) else "unavailable"
    elif news:
        status = "ok"
    else:
        status = "filtered" if diagnostics["received"] else "empty"
    return {"news": news, "news_status": status, "news_diagnostics": diagnostics}


# Snapshot extension: delayed quotes, isolated from official daily history.
def quote_context(now, exchanges):
    """Latest session that has opened, with actual venue calendar boundaries."""
    import exchange_calendars as xc
    out = {}
    for ex in set(exchanges):
        try:
            cal = xc.get_calendar(CALENDARS[ex])
            sessions = cal.sessions_in_range((now-timedelta(days=850)).date(), now.date())
            begun = [s for s in sessions if cal.session_open(s).to_pydatetime() <= now]
            last = begun[-1]
            opening = cal.session_open(last).to_pydatetime()
            closing = cal.session_close(last).to_pydatetime()
            dates = [s.strftime('%Y-%m-%d') for s in begun]
            out[ex] = {'expected': dates[-1], 'previous': dates[-2], 'sessions': dates,
                       'calendar': CALENDARS[ex], 'open_at': opening.isoformat(),
                       'close_at': closing.isoformat(), 'market_open': opening <= now < closing}
        except Exception as exc:
            out[ex] = {'expected': None, 'previous': None, 'sessions': [],
                       'error': type(exc).__name__, 'market_open': None}
    return out


def fetch_quotes(client, meta, workers=6):
    """Request EVERY symbol, in same-exchange blocks of at most 16.

    Costs are per symbol, not per HTTP request. Do not log URLs/credentials.
    One unsupported symbol must not prevent the rest of a batch from loading.
    Failed blocks are isolated once; individual failures are not looped forever.
    """
    groups = {}
    for ticker in meta:
        groups.setdefault(ticker.rsplit('.', 1)[-1], []).append(ticker)
    batches = [items[i:i+16] for items in groups.values() for i in range(0, len(items), 16)]
    quotes, errors = {}, {}

    def request(chunk):
        params = {'s': ','.join(chunk[1:])} if len(chunk) > 1 else {}
        result = client.get('real-time/'+urllib.parse.quote(chunk[0], safe='.'), params, cost=len(chunk))
        if isinstance(result, dict) and result.get('code'):
            result = [result]
        if not isinstance(result, list):
            reason = result.get('error', 'invalid_payload') if isinstance(result, dict) else 'invalid_payload'
            # A denied/invalid symbol can poison a multi-symbol request.
            if len(chunk) > 1 and reason in ('http_400', 'http_403', 'http_404', 'invalid_payload'):
                found, failed = {}, {}
                for ticker in chunk:
                    q, e = request([ticker]); found.update(q); failed.update(e)
                return found, failed
            return {}, dict.fromkeys(chunk, reason)
        found, failed = {}, {}
        for row in result:
            if not isinstance(row, dict) or row.get('code') not in chunk:
                continue
            ticker = row['code']
            if ticker in found and found[ticker] != row:
                failed[ticker] = 'conflicting_quotes'
            found[ticker] = row
        for ticker in failed:
            found.pop(ticker, None)
        for ticker in chunk:
            if ticker not in found:
                failed.setdefault(ticker, 'not_returned')
        return found, failed

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for n, future in enumerate(as_completed([pool.submit(request, b) for b in batches]), 1):
            found, failed = future.result(); quotes.update(found); errors.update(failed)
            if n % 10 == 0 or n == len(batches):
                print(f'Fotografia: {n}/{len(batches)} blocchi letti', flush=True)
    return quotes, errors


def check_quote(raw, ticker, ctx, now, max_age_minutes=35):
    """UTC epoch already includes timezone. Reject pre/post-market and stale data.

    Closed venues may have a last regular-session quote up to 35 minutes before
    close. It is labelled as a delayed last trade, never an official close.
    """
    out = {'quote_status': 'missing', 'quote_reason': 'Quotazione non ricevuta',
           'quote_time': None, 'quote_age_minutes': None}
    if not isinstance(raw, dict) or raw.get('code') != ticker:
        return out
    if not ctx.get('expected'):
        return dict(out, quote_status='unknown', quote_reason='Calendario non verificabile')
    try:
        ts = raw.get('timestamp')
        if not finite(ts) or float(ts) <= 0: raise ValueError()
        when = datetime.fromtimestamp(float(ts), timezone.utc)
        age = (now-when).total_seconds()/60
        out.update(quote_time=when.isoformat(), quote_age_minutes=number(age, 1))
        if age < -2:
            return dict(out, quote_status='review', quote_reason='Orario futuro incoerente')
        if not finite(raw.get('close')) or float(raw['close']) <= 0:
            return dict(out, quote_status='review', quote_reason='Prezzo non valido')
        out['quote_price'] = number(raw['close'], 4)
        if not finite(raw.get('previousClose')) or float(raw['previousClose']) <= 0:
            return dict(out, quote_status='review', quote_reason='Chiusura precedente non disponibile')
        out['quote_previous_close'] = number(raw['previousClose'], 6)
        out['quote_pct'] = number((float(raw['close'])/float(raw['previousClose'])-1)*100)
        opening, closing = (datetime.fromisoformat(ctx[k]) for k in ('open_at','close_at'))
        if when < opening:
            return dict(out, quote_status='stale', quote_reason='Quotazione di una seduta precedente')
        if when > closing + timedelta(minutes=2):
            return dict(out, quote_status='review', quote_reason='Quotazione fuori seduta: non confrontabile con la chiusura ordinaria')
        reference = min(now, closing)
        if (reference-when).total_seconds() > max_age_minutes*60:
            return dict(out, quote_status='stale', quote_reason='Ultimo scambio troppo vecchio per la fotografia')
        if abs(out['quote_pct']) > 50:
            return dict(out, quote_status='review', quote_reason='Movimento oltre il 50%: verificare operazioni societarie/dati')
        return dict(out, quote_status='ok', quote_reason=None)
    except (TypeError, ValueError, OverflowError, OSError):
        return dict(out, quote_status='review', quote_reason='Orario o quotazione non validi')


def build_quote_report(watch, histories, config, now, contexts, daily, quotes, errors,
                       acquisition_started_at, acquisition_finished_at):
    """Produce a separate provisional view. Never archive this as daily EOD.

    Append/replace exactly one current bar, anchored to the last adjusted close.
    If quote.previousClose disagrees with the historical raw close, publish the
    quote but suppress technical extrapolation (splits, distributions, units).
    """
    meta = {**watch, **config['benchmarks']}
    base = {r['ticker']: r for r in daily['stocks']+daily['benchmarks']}
    working, info = {}, {}
    max_age = config.get('snapshot_max_age_minutes', 35)
    for ticker, m in meta.items():
        ctx = contexts.get(m['exchange'], {})
        checked = check_quote(quotes.get(ticker), ticker, ctx, now, max_age)
        checked.update(market_open=ctx.get('market_open'), market_close=ctx.get('close_at'),
                       technical_reason=None, observation_kind='unavailable', technical_usable=False)
        if ticker in errors:
            checked['request_error'] = errors[ticker]
        info[ticker] = checked
        rows = histories.get(ticker, [])
        df = prepare_series(rows, ctx.get('expected') or '', ctx.get('sessions'))
        # Use a finalized EOD price for a closed exchange if already available.
        b = base.get(ticker, {})
        if (ctx.get('market_open') is False and b.get('status') == 'ok'
                and b.get('asof') == ctx.get('expected')):
            working[ticker] = rows
            checked.update(observation_kind='eod_close', technical_usable=True,
                           observed_at=None, observation_date=b['asof'])
            continue
        if checked['quote_status'] != 'ok':
            working[ticker] = []
            continue
        checked.update(observation_kind='delayed_quote', observed_at=checked['quote_time'],
                       observation_date=ctx['expected'])
        prior = df[df.index < ctx['expected']] if not df.empty else df
        if prior.empty or prior.index[-1] != ctx['previous']:
            working[ticker] = []
            checked['technical_reason'] = 'Storico precedente non allineato: disponibili solo prezzo e variazione'
            continue
        prev = float(quotes[ticker]['previousClose'])
        if not math.isclose(float(prior.iloc[-1].close), prev, rel_tol=0.002, abs_tol=0.00001):
            working[ticker] = []
            checked['technical_reason'] = 'Chiusura precedente discordante: possibili split, dividendi o unità diverse; medie e filtro massimo sospesi'
            continue
        raw = quotes[ticker]
        price = float(raw['close'])
        factor = float(prior.iloc[-1].adjusted_close)/prev
        # Invalid session high/low must disable the 252-session maximum filter.
        hi, lo = raw.get('high'), raw.get('low')
        valid_hl = (finite(hi) and finite(lo) and float(hi) >= price >= float(lo) > 0)
        bar = {'date':ctx['expected'], 'close':price, 'adjusted_close':price*factor,
               'high':float(hi) if valid_hl else None, 'low':float(lo) if valid_hl else None,
               'volume':float(raw['volume']) if finite(raw.get('volume')) and float(raw['volume']) >= 0 else None}
        working[ticker] = [r for r in rows if str(r.get('date','')) < ctx['expected']]+[bar]
        checked['technical_usable'] = True
    view = build_report(watch, working, config, now, contexts, previous={}, mode='snapshot')
    for row in view['stocks']+view['benchmarks']:
        ticker = row['ticker']; q = info[ticker]; row.update(q)
        if q['observation_kind'] == 'delayed_quote':
            row.update(status='ok', asof=q['observation_date'], expected=q['observation_date'],
                       price=q['quote_price'], pct=q['quote_pct'], raw_pct=q['quote_pct'],
                       currency=CURRENCIES.get(row['exchange'], 'EUR'), price_basis='delayed_previous_close',
                       reason=q['technical_reason'], rel_volume=None,
                       alert=q['quote_pct'] <= config['drop_pct'])
            row['candidate'] = bool(row['alert'] and q['technical_usable'] and finite(row.get('dist_max'))
                                    and row['dist_max'] <= config['candidate_distance_pct'])
        elif q['observation_kind'] == 'unavailable':
            # Keep older known values visible without treating them as current.
            old = base.get(ticker, {})
            row.update(status=q['quote_status'], reason=q['quote_reason'], alert=False, candidate=False,
                       last_known_price=old.get('price'), last_known_date=old.get('asof'),
                       price=q.get('quote_price'), pct=q.get('quote_pct'), asof=None,
                       currency=CURRENCIES.get(row['exchange'], 'EUR'))
    # Recalculate pressure and A/D from quotes even when long history is missing.
    for region in view['regions']:
        members = [r for r in view['stocks'] if r['area'] == region['area']]
        valid = [r for r in members if r['status'] == 'ok' and finite(r.get('pct'))]
        days = {r.get('asof') for r in valid}
        expected = {contexts.get(r['exchange'], {}).get('expected') for r in members}
        aligned = len(expected) == 1 and None not in expected and days == expected
        day = next(iter(days)) if len(days) == 1 else None
        n = len(valid); coverage = 100*n/len(members)
        latest = region['latest']
        latest.update(date=day, n=n, coverage=number(coverage),
                      pressure=number(100*sum(r['alert'] for r in valid)/n) if n else None,
                      advance=sum(r['pct'] > 0 for r in valid), decline=sum(r['pct'] < 0 for r in valid),
                      unchanged=sum(r['pct'] == 0 for r in valid))
        region.update(valid=n, coverage=number(coverage), aligned=aligned, asof=day)
        old_region = next((r for r in daily['regions'] if r['area'] == region['area']), {})
        region['history'] = [p for p in old_region.get('history', []) if not day or p['date'] < day]
        if day: region['history'].append(dict(latest, provisional=True))
        for i in view['indicators']:
            if i['key'] == region['area']+':pressure':
                good = aligned and coverage >= config['min_coverage_pct'] and n >= config['min_breadth_names']
                i.update(value=latest['pressure'], asof=day, quality='ok' if good else 'partial',
                         level=raw_level(latest['pressure'], i['rule']) if good else 'unknown',
                         detail=f'Fotografia ritardata; copertura {number(coverage)}% della watchlist.')
    # VIX level needs a valid quote, not 252 historical bars. The ratio also
    # requires synchronized observations; do not mix a quote and an EOD close.
    bm = {r['ticker']: r for r in view['benchmarks']}
    v, v3 = bm.get('VIX.INDX', {}), bm.get('VIX3M.INDX', {})
    ratio_ok = v.get('status') == v3.get('status') == 'ok' and v.get('asof') == v3.get('asof')
    ratio_ok = ratio_ok and v.get('observation_kind') == v3.get('observation_kind') and finite(v3.get('price')) and v3['price'] > 0
    if ratio_ok and v.get('observation_kind') == 'delayed_quote':
        ratio_ok = abs((datetime.fromisoformat(v['observed_at'])-datetime.fromisoformat(v3['observed_at'])).total_seconds()) <= 300
    for i in view['indicators']:
        if i['key'] in ('vix', 'vix_ratio'):
            good = v.get('status') == 'ok' if i['key'] == 'vix' else ratio_ok
            val = v.get('price') if i['key'] == 'vix' else v['price']/v3['price'] if ratio_ok else None
            i.update(value=number(val), asof=v.get('asof'), quality='ok' if good else 'missing',
                     level=raw_level(val, i['rule']) if good else 'unknown')
    # Two scans on one day are NOT two confirmed sessions.
    for i in view['indicators']:
        i.update(confirmed=False, sessions=0, provisional=True,
                 detail=i['detail']+' Lettura provvisoria, non confermata dalla chiusura.')
    view['changes'] = []
    view['stocks'].sort(key=lambda r:r['pct'] if r.get('pct') is not None and r['status']=='ok' else 999)
    q = view['quality']
    q.update(valid=sum(r['status']=='ok' for r in view['stocks']),
             issues=[dict(ticker=r['ticker'],status=r['status'],asof=r.get('asof'),expected=r.get('expected'),reason=r.get('reason'))
                     for r in view['stocks'] if r['status'] != 'ok' or r.get('technical_reason')],
             benchmark_issues=[dict(ticker=r['ticker'],status=r['status'],asof=r.get('asof'),expected=r.get('expected'),reason=r.get('reason'))
                     for r in view['benchmarks'] if r['status'] != 'ok' or r.get('technical_reason')])
    view.update(status='ok' if q['valid']==len(watch) else 'partial' if q['valid'] else 'unavailable',
                acquisition_started_at=acquisition_started_at, acquisition_finished_at=acquisition_finished_at,
                requested=len(watch), quote_received=sum(t in quotes for t in watch),
                delayed_valid=sum(r['observation_kind']=='delayed_quote' for r in view['stocks']),
                eod_valid=sum(r['observation_kind']=='eod_close' for r in view['stocks']),
                max_quote_age_minutes=max_age, market_sessions={ex:{k:v for k,v in c.items() if k!='sessions'} for ex,c in contexts.items()},
                limits=['Fotografia delle quotazioni disponibili, normalmente ritardate di 15–20 minuti; acquisizione non simultanea.',
                        'Ogni titolo mantiene l’orario del fornitore. Titoli non aggiornati esclusi da alert e termometri.',
                        'A mercato chiuso si preferisce la chiusura EOD verificata, altrimenti l’ultimo scambio regolare sufficientemente recente.',
                        'Medie, massimi e candidati in seduta sono provvisori: operazioni societarie possono richiedere rettifiche successive.',
                        'Una discordanza oltre lo 0,2% tra chiusure precedenti sospende le estrapolazioni tecniche.',
                        'Macro e notizie mantengono la propria frequenza e data. Nessun segnale garantisce di anticipare una crisi.'])
    # The UI inherits shared news/macro/usage from the enclosing daily report.
    for field in ('news','macro','usage','news_status'):
        view.pop(field, None)
    return view


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--history-file",help="Replay locale CSV/CSV.GZ; nessuna API e nessuna quota consumata")
    parser.add_argument("--as-of",help="UTC ISO timestamp; richiesto solo nel replay")
    parser.add_argument("--output",default=str(ROOT/"docs/market.json"))
    args=parser.parse_args()
    if args.as_of and not args.history_file: parser.error("--as-of richiede --history-file")
    now=datetime.fromisoformat(args.as_of.replace("Z","+00:00")) if args.as_of else datetime.now(timezone.utc)
    if now.tzinfo is None: parser.error("--as-of deve includere il fuso orario")
    config=load_json(ROOT/"radar_config.json",None)
    if not config: raise SystemExit("radar_config.json mancante")
    watch=watchlist(ROOT/"tickers.csv")
    all_meta={**watch,**config["benchmarks"]}
    contexts=session_context(now,[m["exchange"] for m in all_meta.values()],config["eod_delay_minutes"])
    histories={}; client=None
    if args.history_file:
        df=pd.read_csv(args.history_file)
        histories={t:g.to_dict("records") for t,g in df.groupby("ticker") if t in all_meta}
    else:
        key=os.environ.get("EOD_API_KEY", "").strip()
        if not key: raise SystemExit("EOD_API_KEY mancante. La chiave va solo nel Secret GitHub.")
        client=Client(key,config["request_interval_seconds"])
        usage=client.get("user",cost=0)
        if isinstance(usage,dict) and usage.get("error") in ("http_401","http_402","http_403"):
            raise SystemExit("Accesso EODHD non disponibile: controlla Secret, abbonamento e quota. Nessun dato nuovo pubblicato.")
        news_limits=news_settings(config)
        estimated=6*len(all_meta)+10*news_limits["max_pages"]*(news_limits["max_tickers"]+1)
        if estimated>config["credit_budget_per_run"]: raise SystemExit("Budget configurato insufficiente per questa watchlist")
        if isinstance(usage,dict) and finite(usage.get("dailyRateLimit")):
            used=float(usage.get("apiRequests",0)) if usage.get("apiRequestsDate")==now.date().isoformat() else 0
            if float(usage["dailyRateLimit"])-used < estimated:
                raise SystemExit("Quota residua insufficiente; nessun aggiornamento pubblicato")
        frm=(now-timedelta(days=config["lookback_calendar_days"])).date().isoformat()
        def fetch(t): return t,client.get("eod/"+urllib.parse.quote(t,safe="."),{"from":frm,"to":now.date().isoformat(),"order":"a"})
        # One canary request avoids hundreds of requests after an authentication failure.
        probe=fetch("SPY.US")[1]
        if isinstance(probe,dict) and probe.get("error") in ("http_401","http_402","http_403"):
            raise SystemExit("Storico EODHD non accessibile con questo piano/Secret. Nessun dato nuovo pubblicato.")
        histories["SPY.US"]=probe if isinstance(probe,list) else []
        with ThreadPoolExecutor(max_workers=config["max_workers"]) as pool:
            futures=[pool.submit(fetch,t) for t in all_meta if t != "SPY.US"]
            for completed, f in enumerate(as_completed(futures), 1):
                t,rows=f.result()
                histories[t]=rows if isinstance(rows,list) else []
                if completed % 100 == 0 or completed == len(futures):
                    print(f"Storici: {completed+1}/{len(all_meta)} letti", flush=True)
    previous=load_json(args.output,{})
    if previous.get("mode") != ("historical_replay" if args.history_file else "live"): previous={}
    report=build_report(watch,histories,config,now,contexts,previous,mode="historical_replay" if args.history_file else "live")
    if client:
        report["macro"]=public_macro(datetime.now(timezone.utc))
        print("Acquisizione fotografia ritardata di tutta la watchlist…", flush=True)
        quote_start=datetime.now(timezone.utc)
        quotes, quote_errors=fetch_quotes(client,all_meta,config["max_workers"])
        quote_end=datetime.now(timezone.utc)
        live_context=quote_context(quote_end,[m["exchange"] for m in all_meta.values()])
        report["snapshot"]=build_quote_report(watch,histories,config,quote_end,live_context,report,
                                             quotes,quote_errors,quote_start.isoformat(),quote_end.isoformat())
        report["snapshot"]["request_errors"]=len(quote_errors)
        targets=[s["ticker"] for s in report["snapshot"]["stocks"] if s["alert"]]
        report.update(collect_news(client,config,targets,datetime.now(timezone.utc)))
        nd=report["news_diagnostics"]
        print(f"Notizie: {nd['received']} risultati ricevuti; {nd['admitted']} notizie uniche ammesse; "
              f"{len(nd['errors'])} richieste con errore; stato {report['news_status']}", flush=True)
        latest=client.get("user",cost=0)
        report["usage"]={k:latest.get(k) for k in ("apiRequests","apiRequestsDate","dailyRateLimit") if isinstance(latest,dict) and k in latest}
        report["usage"].update(estimated_run_credits=client.credits,run_requests=client.requests)
    if not any(s["status"]=="ok" for s in report["stocks"]) and not args.history_file:
        report["status"]="unavailable"
    report["completed_at"]=datetime.now(timezone.utc).isoformat() if client else now.isoformat()
    if client:
        report["run_id"]=os.environ.get("GITHUB_RUN_ID")
        live=report["snapshot"]
        print(f"Fotografia: {live['quality']['valid']}/{len(watch)} utilizzabili; "
              f"{live['delayed_valid']} quotazioni ritardate, {live['eod_valid']} chiusure verificate; "
              f"{sum(s['alert'] for s in live['stocks'])} alert", flush=True)
    write_json(args.output,report)
    if not args.history_file:
        archive=Path(args.output).with_name("market_history.json")
        hist=load_json(archive,[])
        day=max((i["asof"] for i in report["indicators"] if i.get("asof")),default=now.date().isoformat())
        entry={"date":day,"generated_at":report["generated_at"],"watchlist_signature":report["watchlist_signature"],"indicators":report["indicators"],"changes":report["changes"]}
        hist=[h for h in hist if h.get("date")!=day]+[entry]
        write_json(archive,sorted(hist,key=lambda x:x["date"])[-800:])
    print(f"Radar {config['version']}: {report['quality']['valid']}/{len(watch)} aggiornati; {sum(s['alert'] for s in report['stocks'])} alert; stato {report['status']}")
    print("Solo dati informativi. Nessun ordine, nessun accesso al portafoglio.")


if __name__=="__main__":
    main()
