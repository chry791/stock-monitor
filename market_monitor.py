#!/usr/bin/env python3
"""Stock Monitor Radar. Informational only: no account, positions or orders.

Pure calculations are shared by production, offline replay and verification.
Each series keeps its own date. Missing data never produce a green light.
Requires pandas, numpy, exchange-calendars. Network uses Python stdlib.
"""
import argparse
import csv
import hashlib
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


def safe_news(rows, allowed, now, ticker=None):
    out=[]
    for r in rows if isinstance(rows,list) else []:
        try:
            url=str(r.get("link", "")); p=urllib.parse.urlparse(url); host=(p.hostname or "").lower()
            when=datetime.fromisoformat(str(r.get("date","")).replace("Z","+00:00"))
            if when.tzinfo is None: continue
            if p.scheme != "https" or p.username or not any(host==d or host.endswith("."+d) for d in allowed): continue
            if not 0 <= (now-when).total_seconds() <= 7*86400: continue
            out.append({"title":str(r.get("title", ""))[:240],"url":url,"source":host,"date":when.isoformat(),"ticker":ticker})
        except (TypeError,ValueError):
            continue
    return out


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
        estimated=2*len(all_meta)+10*(config["news_max_tickers"]+1)
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
            for f in as_completed(futures):
                t,rows=f.result()
                histories[t]=rows if isinstance(rows,list) else []
    previous=load_json(args.output,{})
    if previous.get("mode") != ("historical_replay" if args.history_file else "live"): previous={}
    report=build_report(watch,histories,config,now,contexts,previous,mode="historical_replay" if args.history_file else "live")
    if client:
        report["macro"]=public_macro(now)
        targets=[s["ticker"] for s in report["stocks"] if s["alert"]][:config["news_max_tickers"]]
        news=[]; errors=0
        for t in [None]+targets:
            params={"limit":20,"from":(now-timedelta(days=3)).date().isoformat()}
            if t: params["s"]=t
            raw=client.get("news",params,cost=5)
            errors+=not isinstance(raw,list)
            news.extend(safe_news(raw,config["news_allowed_domains"],now,t))
        report["news"]=list({(n["url"],n["ticker"]):n for n in news}.values())
        report["news_status"]="partial" if errors else "ok"
        latest=client.get("user",cost=0)
        report["usage"]={k:latest.get(k) for k in ("apiRequests","apiRequestsDate","dailyRateLimit") if isinstance(latest,dict) and k in latest}
        report["usage"].update(estimated_run_credits=client.credits,run_requests=client.requests)
    if not any(s["status"]=="ok" for s in report["stocks"]) and not args.history_file:
        report["status"]="unavailable"
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
