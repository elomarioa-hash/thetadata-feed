#!/usr/bin/env python3
"""
thetadata_fetcher.py
────────────────────
Fetches SPXW + SPY options data from ThetaData terminal every minute.
Computes: Zero Gamma, Max Pain, GEX Wall, Delta Wall, Charm Pivot, Call/Put CVD
Writes to GitHub repo for TradingView Pine Seeds consumption.
"""

import os, csv, json, math, logging, subprocess
from datetime import datetime, date, timedelta
import requests

# ── Configuration ──────────────────────────────────────────────
TERMINAL     = "http://127.0.0.1:25510"
REPO_PATH    = r"C:\Users\hells\thetadata-feed"
DATA_DIR     = os.path.join(REPO_PATH, "data")
SESSION_FILE = os.path.join(REPO_PATH, ".session.json")

ASSETS = {
    "SPXW": "SPX",
    "SPY":  "SPY",
}

MONEYNESS = {
    "ITM": (0.60, 1.00),
    "ATM": (0.40, 0.60),
    "OTM": (0.00, 0.40),
}

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(message)s",
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(REPO_PATH, "fetcher.log")),
    ]
)
log = logging.getLogger(__name__)


# ── ThetaData API Helper ───────────────────────────────────────
def api_get(path: str, params: dict = None, timeout: int = 15) -> list:
    try:
        r = requests.get(f"{TERMINAL}{path}", params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        error = data.get("header", {}).get("error_type", "")
        if error and error != "null":
            log.warning(f"API error [{path}]: {error}")
            return []

        header = data.get("header", {}).get("format", [])
        rows   = data.get("response", [])

        if not rows:
            return []

        if isinstance(rows[0], list) and len(rows[0]) == 2 and isinstance(rows[0][0], dict):
            result = []
            for contract, values in rows:
                row = {**contract}
                if header and isinstance(values, list):
                    row.update(dict(zip(header, values)))
                elif isinstance(values, dict):
                    row.update(values)
                result.append(row)
            return result

        if isinstance(rows[0], list) and header:
            return [dict(zip(header, row)) for row in rows]

        if isinstance(rows[0], dict):
            return rows

        return []

    except requests.exceptions.ConnectionError:
        log.error("ThetaData terminal not reachable. Is it running?")
        return []
    except Exception as e:
        log.error(f"API request failed [{path}]: {e}")
        return []


# ── Spot Price ─────────────────────────────────────────────────
def get_spot(asset: str, index_root: str):
    if asset == "SPXW":
        rows = api_get("/v2/last/index/quote", {"root": index_root})
    else:
        rows = api_get("/v2/last/stock/quote", {"root": index_root})

    if not rows:
        return None

    r   = rows[0]
    bid = float(r.get("bid", 0) or 0)
    ask = float(r.get("ask", 0) or 0)
    return round((bid + ask) / 2, 2) if ask > 0 else None


# ── Expirations ────────────────────────────────────────────────
def get_expirations(root: str) -> list:
    rows      = api_get("/v2/list/expirations", {"root": root})
    today_str = date.today().strftime("%Y%m%d")
    exps      = []
    for r in rows:
        val = str(r.get("date", r) if isinstance(r, dict) else r)
        if val >= today_str and val.isdigit():
            exps.append(val)
    return sorted(exps)


def filter_expirations(exps: list, scope: str) -> list:
    today     = date.today()
    today_str = today.strftime("%Y%m%d")

    if scope == "0DTE":
        return [e for e in exps if e == today_str]

    if scope == "WEEKLY":
        cutoff = (today + timedelta(days=8)).strftime("%Y%m%d")
        return [e for e in exps if today_str <= e <= cutoff]

    if scope == "MONTHLY":
        cutoff = (today + timedelta(days=50)).strftime("%Y%m%d")
        return [e for e in exps if today_str <= e <= cutoff]

    return []


# ── Chain Data ─────────────────────────────────────────────────
def get_chain(root: str, exp: str) -> tuple:
    p      = {"root": root, "exp": exp}
    greeks = api_get("/v2/bulk_snapshot/option/greeks",        p)
    oi     = api_get("/v2/bulk_snapshot/option/open_interest", p)
    trades = api_get("/v2/bulk_snapshot/option/trade",         p)
    return greeks, oi, trades


# ── Strike Normalization ───────────────────────────────────────
def normalize_strike(raw) -> float:
    val = float(raw)
    return val / 1000.0 if val > 10_000 else val


# ── Metric Computation ─────────────────────────────────────────
def compute_metrics(all_greeks, all_oi, all_trades, spot, moneyness_filter="ALL") -> dict:
    oi_map    = {}
    trade_map = {}

    for r in all_oi:
        k = (str(r.get("strike", "")), str(r.get("right", "")).upper())
        oi_map[k] = int(r.get("open_interest", 0) or 0)

    for r in all_trades:
        k = (str(r.get("strike", "")), str(r.get("right", "")).upper())
        trade_map[k] = int(r.get("volume", 0) or 0)

    gex_by_k   = {}
    dex_by_k   = {}
    charm_by_k = {}
    pain_map   = {}
    call_cvd   = 0.0
    put_cvd    = 0.0

    for r in all_greeks:
        raw_k = r.get("strike", 0)
        right = str(r.get("right", "C")).upper()
        k     = normalize_strike(raw_k)

        gamma = float(r.get("gamma", 0) or 0)
        delta = float(r.get("delta", 0) or 0)
        charm = float(r.get("charm", 0) or 0)

        oi    = oi_map.get((str(raw_k), right), 0)
        vol   = trade_map.get((str(raw_k), right), 0)
        sign  = 1 if right == "C" else -1

        if moneyness_filter != "ALL":
            lo, hi = MONEYNESS[moneyness_filter]
            if not (lo <= abs(delta) <= hi):
                continue

        gex = gamma * oi * 100 * (spot ** 2) * 0.01 * sign
        gex_by_k[k] = gex_by_k.get(k, 0) + gex

        dex = delta * oi * 100
        dex_by_k[k] = dex_by_k.get(k, 0) + dex

        charm_by_k[k] = charm_by_k.get(k, 0) + (charm * oi * 100)

        pain_map.setdefault(k, {"c": 0, "p": 0})
        if right == "C":
            pain_map[k]["c"] += oi
        else:
            pain_map[k]["p"] += oi

        cvd_val  = abs(delta) * vol
        if right == "C":
            call_cvd += cvd_val
        else:
            put_cvd  += cvd_val

    if not gex_by_k:
        log.warning("No GEX data — empty chain?")
        return {k: spot for k in
                ["zero_gamma","max_pain","gex_wall","delta_wall","charm_pivot"]
               } | {"call_cvd": 0.0, "put_cvd": 0.0}

    strikes = sorted(gex_by_k.keys())

    # Zero Gamma
    cum        = 0.0
    zero_gamma = strikes[len(strikes) // 2]
    for k in strikes:
        prev  = cum
        cum  += gex_by_k[k]
        if prev < 0 <= cum:
            zero_gamma = k
            break

    # GEX Wall
    gex_wall = max(gex_by_k, key=lambda k: abs(gex_by_k[k]))

    # Delta Wall
    delta_wall = max(dex_by_k, key=lambda k: abs(dex_by_k[k]))

    # Charm Pivot
    total_charm_abs = sum(abs(v) for v in charm_by_k.values())
    charm_pivot = (
        sum(k * abs(v) for k, v in charm_by_k.items()) / total_charm_abs
        if total_charm_abs > 0 else spot
    )

    # Max Pain
    all_k     = sorted(pain_map.keys())
    max_pain  = spot
    best_loss = float("inf")
    for test in all_k:
        loss = sum(
            pain_map[k]["c"] * max(test - k, 0) +
            pain_map[k]["p"] * max(k - test, 0)
            for k in all_k
        )
        if loss < best_loss:
            best_loss = loss
            max_pain  = test

    return {
        "zero_gamma":  round(zero_gamma,  2),
        "max_pain":    round(max_pain,    2),
        "gex_wall":    round(gex_wall,    2),
        "delta_wall":  round(delta_wall,  2),
        "charm_pivot": round(charm_pivot, 2),
        "call_cvd":    round(call_cvd,    2),
        "put_cvd":     round(put_cvd,     2),
    }


# ── CSV Writer ─────────────────────────────────────────────────
def write_csv(filepath: str, row: dict) -> None:
    exists = os.path.exists(filepath)
    with open(filepath, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)


def write_levels(root, scope, metrics, ts):
    path = os.path.join(DATA_DIR, f"{root}_{scope}_LVL.csv")
    write_csv(path, {
        "date":   ts,
        "open":   metrics["max_pain"],
        "high":   metrics["gex_wall"],
        "low":    metrics["delta_wall"],
        "close":  metrics["zero_gamma"],
        "volume": metrics["charm_pivot"],
    })


def write_cvd(root, scope, moneyness, metrics, price_pct, ts):
    suffix = "" if moneyness == "ALL" else f"_{moneyness}"
    path   = os.path.join(DATA_DIR, f"{root}_{scope}_CVD{suffix}.csv")
    write_csv(path, {
        "date":   ts,
        "open":   round(metrics["call_cvd"] / 1000, 4),
        "high":   round(metrics["put_cvd"]  / 1000, 4),
        "low":    0,
        "close":  price_pct,
        "volume": 0,
    })


# ── Session Open Tracker ───────────────────────────────────────
def load_session() -> dict:
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE) as f:
            return json.load(f)
    return {}


def save_session(data: dict) -> None:
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f)


# ── Git Push ───────────────────────────────────────────────────
def git_push() -> None:
    os.chdir(REPO_PATH)
    subprocess.run(["git", "add", "-A"],          capture_output=True)
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if diff.returncode != 0:
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M")
        subprocess.run(["git", "commit", "-m", f"data:{ts}"], capture_output=True)
        subprocess.run(["git", "push", "origin", "main"],     capture_output=True)
        log.info(f"Pushed to GitHub @ {ts}")
    else:
        log.info("No changes to push")


# ── Main ───────────────────────────────────────────────────────
def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    session   = load_session()
    today_str = date.today().isoformat()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    for asset, index_root in ASSETS.items():
        spot = get_spot(asset, index_root)
        if not spot:
            log.warning(f"Could not get spot for {asset} — skipping")
            continue

        session_key  = f"{asset}_{today_str}"
        session_open = session.get(session_key, spot)
        if session_key not in session:
            session[session_key] = spot
            log.info(f"Session open set for {asset}: {spot}")

        price_pct = round(((spot - session_open) / session_open) * 100, 4)
        all_exps  = get_expirations(asset)

        for scope in ["0DTE", "WEEKLY", "MONTHLY"]:
            exps = filter_expirations(all_exps, scope)
            if not exps:
                log.info(f"No expirations for {asset} {scope}")
                continue

            agg_greeks, agg_oi, agg_trades = [], [], []
            for exp in exps:
                g, o, t = get_chain(asset, exp)
                agg_greeks += g
                agg_oi     += o
                agg_trades += t

            if not agg_greeks:
                log.warning(f"Empty greeks for {asset} {scope}")
                continue

            metrics_all = compute_metrics(
                agg_greeks, agg_oi, agg_trades, spot, "ALL")
            write_levels(asset, scope, metrics_all, timestamp)

            for money in ["ALL", "ATM", "ITM", "OTM"]:
                m = compute_metrics(
                    agg_greeks, agg_oi, agg_trades, spot, money)
                write_cvd(asset, scope, money, m, price_pct, timestamp)

            log.info(
                f"{asset:5s} {scope:8s} | "
                f"ZG={metrics_all['zero_gamma']:8.2f}  "
                f"MP={metrics_all['max_pain']:8.2f}  "
                f"GEX={metrics_all['gex_wall']:8.2f}  "
                f"DEX={metrics_all['delta_wall']:8.2f}  "
                f"Charm={metrics_all['charm_pivot']:8.2f}"
            )

    save_session(session)
    git_push()


if __name__ == "__main__":
    main()