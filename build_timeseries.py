#!/usr/bin/env python3
"""
Build an Excel workbook with 5 daily time series (two columns each):
  - BTC-USD Adj Close (Yahoo Finance)
  - VIX (^VIX) Adj Close (Yahoo Finance)
  - West Singapore PM2.5 daily average (data.gov.sg NEA hourly -> daily mean)
  - Earthquake counts per day (USGS)
  - Sunspot number per day (SIDC/SILSO daily file)

Output: GroupProjectData.xlsx (in the same folder)

Usage:
  python build_timeseries.py --start 2021-01-01 --end 2026-04-19
  # If --start/--end omitted, defaults to last 1826 days (5 years) up to today.
"""

import argparse
import datetime as dt
import io
import sys
import time
from typing import Optional, Dict, Tuple

import pandas as pd
import requests
import yfinance as yf

# -----------------------
# Helpers
# -----------------------

def daterange(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)

def daily_frame(start: dt.date, end: dt.date) -> pd.DataFrame:
    idx = pd.date_range(start, end, freq="D")
    return pd.DataFrame({"date": idx})

def to_sheet(df: pd.DataFrame, start: dt.date, end: dt.date, value_col: str) -> pd.DataFrame:
    # NEW: ensure single-level, string column names
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = ["_".join(map(str, c)) for c in df.columns]
    else:
        df = df.copy()
        df.columns = [str(c) for c in df.columns]

    base = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})
    if "date" not in df.columns or value_col not in df.columns:
        # return empty skeleton (prevents MergeError)
        out = base.copy()
        out["value"] = pd.NA
        out["date"] = out["date"].dt.date
        return out[["date", "value"]]

    out = base.merge(df[["date", value_col]], on="date", how="left")
    out.rename(columns={value_col: "value"}, inplace=True)
    out["date"] = out["date"].dt.date
    return out[["date", "value"]]

# -----------------------
# 1) Yahoo Finance (Adj Close)
# -----------------------

def fetch_yahoo_adjclose(symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    """
    Fetch adjusted daily close for [symbol] between start..end (inclusive calendar).
    Uses auto_adjust=True so 'Close' is adjusted; we expose it as 'adj_close'.
    """
    end_plus = end + dt.timedelta(days=1)  # history end is exclusive
    tk = yf.Ticker(symbol)
    data = tk.history(start=start, end=end_plus, interval="1d", auto_adjust=True)
    if data is None or data.empty:
        return pd.DataFrame(columns=["date", "adj_close"])

    df = data.reset_index()
    # yfinance can use 'Date' or 'Datetime'
    if "Date" in df.columns:
        df.rename(columns={"Date": "date"}, inplace=True)
    elif "Datetime" in df.columns:
        df.rename(columns={"Datetime": "date"}, inplace=True)
    else:
        df.rename(columns={df.columns[0]: "date"}, inplace=True)  # fallback

    # With auto_adjust=True, 'Close' is already adjusted
    if "Close" not in df.columns:
        # Very rare edge case — try to flatten and find 'Close'
        df.columns = [c if isinstance(c, str) else "_".join(map(str, c)) for c in df.columns]
    close_col = "Close" if "Close" in df.columns else None
    if close_col is None:
        return pd.DataFrame(columns=["date", "adj_close"])

    df = df[["date", "Close"]].rename(columns={"Close": "adj_close"})
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df = df[(df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))]
    return df

# -----------------------
# 2) PM2.5 (NEA, hourly -> daily mean)
# -----------------------

NEA_PM25_URL = "https://api.data.gov.sg/v1/environment/pm25"
PM25_REGION = "west"  # change if needed

def fetch_pm25_west_daily(start: dt.date, end: dt.date, pause_s: float = 0.15) -> pd.DataFrame:
    """
    Loop days; for each day, call v1 ?date=YYYY-MM-DD and average the 1-hour readings for West region.
    Returns df with columns [date, pm25_daily].
    """
    rows = []
    sess = requests.Session()
    headers = {"User-Agent": "ntu-timeseries-student/1.0"}

    for d in daterange(start, end):
        try:
            r = sess.get(NEA_PM25_URL, params={"date": d.isoformat()}, timeout=30, headers=headers)
            r.raise_for_status()
            js = r.json()
            vals = []
            for item in js.get("items", []):
                ts = item.get("timestamp")
                reading = item.get("readings", {}).get("pm25_one_hourly", {}).get(PM25_REGION)
                if reading is not None:
                    vals.append(float(reading))
            if len(vals) > 0:
                rows.append({"date": pd.Timestamp(d), "pm25_daily": sum(vals)/len(vals)})
            else:
                rows.append({"date": pd.Timestamp(d), "pm25_daily": None})
        except Exception as e:
            # If a call fails, keep an empty for that date
            rows.append({"date": pd.Timestamp(d), "pm25_daily": None})
        time.sleep(pause_s)

    return pd.DataFrame(rows)

# -----------------------
# 3) Earthquake counts per day (USGS)
# -----------------------

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

def fetch_eq_counts(start: dt.date, end: dt.date, chunk_days: int = 14, pause_s: float = 0.2) -> pd.DataFrame:
    """
    Query USGS in small chunks (UTC), count events per day (all magnitudes).
    Returns df with [date, count].
    """
    sess = requests.Session()
    headers = {"User-Agent": "ntu-timeseries-student/1.0"}

    counts = {}

    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + dt.timedelta(days=chunk_days - 1), end)
        params = {
            "format": "geojson",
            "starttime": chunk_start.isoformat(),
            "endtime": (chunk_end + dt.timedelta(days=1)).isoformat(),  # make end inclusive
            "orderby": "time-asc",
            "limit": 20000  # generous cap
        }
        try:
            r = sess.get(USGS_URL, params=params, headers=headers, timeout=45)
            r.raise_for_status()
            js = r.json()
            feats = js.get("features", [])
            for f in feats:
                ms = f.get("properties", {}).get("time")  # milliseconds since epoch UTC
                if ms is None:
                    continue
                d_utc = pd.to_datetime(ms, unit="ms", utc=True).date()  # date in UTC
                counts[d_utc] = counts.get(d_utc, 0) + 1
        except Exception:
            # On failure, just leave days for this chunk as missing; they'll appear as NaN after merge
            pass

        chunk_start = chunk_end + dt.timedelta(days=1)
        time.sleep(pause_s)

    # Build frame and clip to range
    if counts:
        df = pd.DataFrame({"date": pd.to_datetime(list(counts.keys())), "eq_count": list(counts.values())})
        df["date"] = df["date"].dt.tz_localize(None)
        df = df[(df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))]
        return df
    else:
        return pd.DataFrame(columns=["date", "eq_count"])

# -----------------------
# 4) Sunspot number (SIDC/SILSO daily file + current month extension)
# -----------------------

SILSO_DAILY_URL = "https://www.sidc.be/silso/DATA/SN_d_tot_V2.0.txt"
SILSO_CURRENT_URL = "https://www.sidc.be/SILSO/DATA/EISN/EISN_current.txt"


def _parse_silso_daily_text(raw: str, value_col: int = 4, source_name: str = "") -> pd.DataFrame:
    """
    Parse SILSO daily-style text file.

    Expected columns:
    Year Month Day DecDate DailySunspot StdDev Nobs Definitive(1/0)

    Returns
    -------
    pd.DataFrame with columns: date, sunspot, source
    """
    rows = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split()
        if len(parts) <= value_col:
            continue

        try:
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            val = float(parts[value_col])
            if val < 0:
                val = np.nan
            rows.append({
                "date": pd.Timestamp(y, m, d),
                "sunspot": val,
                "source": source_name
            })
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["date", "sunspot", "source"])

    return pd.DataFrame(rows)


def fetch_sunspot_daily(start: dt.date, end: dt.date) -> pd.DataFrame:
    """
    Download SILSO historical daily file plus current-month update file.

    Priority rule
    -------------
    Historical data dominate current data on overlapping dates.

    Returns
    -------
    pd.DataFrame with columns: date, sunspot
    """
    headers = {"User-Agent": "ntu-timeseries-student/1.0"}

    # Historical daily file
    r_hist = requests.get(SILSO_DAILY_URL, timeout=45, headers=headers)
    r_hist.raise_for_status()
    df_hist = _parse_silso_daily_text(
        r_hist.text,
        value_col=4,
        source_name="historical"
    )

    # Current daily file
    r_cur = requests.get(SILSO_CURRENT_URL, timeout=45, headers=headers)
    r_cur.raise_for_status()
    df_cur = _parse_silso_daily_text(
        r_cur.text,
        value_col=4,
        source_name="current"
    )

    # Combine with historical taking priority on overlap:
    # put current first, historical second, then keep='last'
    df = pd.concat([df_cur, df_hist], ignore_index=True)

    if df.empty:
        return pd.DataFrame(columns=["date", "sunspot"])

    df = (
        df.sort_values("date")
          .drop_duplicates(subset="date", keep="last")
          .sort_values("date")
          .reset_index(drop=True)
    )

    mask = (df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))
    df = df.loc[mask, ["date", "sunspot"]].reset_index(drop=True)

    return df

# -----------------------
# Coordinator
# -----------------------

def main():
    parser = argparse.ArgumentParser(description="Build 5-sheet Excel with daily time series.")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--out", type=str, default="GroupProjectData.xlsx", help="Output Excel filename")
    args = parser.parse_args()

    today = dt.date.today()
    end = dt.date.fromisoformat(args.end) if args.end else today
    start = dt.date.fromisoformat(args.start) if args.start else (end - dt.timedelta(days=1826))

    print(f"[INFO] Building dataset from {start} to {end} ...")

    # --- BTC-USD Adj Close ---
    print("[INFO] Downloading BTC-USD (Adj Close) from Yahoo Finance ...")
    btc = fetch_yahoo_adjclose("BTC-USD", start, end)
    btc_sheet = to_sheet(btc, start, end, "adj_close")

    # --- VIX (^VIX) Adj Close ---
    print("[INFO] Downloading ^VIX (Adj Close) from Yahoo Finance ...")
    vix = fetch_yahoo_adjclose("^VIX", start, end)
    vix_sheet = to_sheet(vix, start, end, "adj_close")

    # --- PM2.5 West daily average ---
    print("[INFO] Fetching West Singapore PM2.5 (hourly -> daily mean) from data.gov.sg ...")
    pm25 = fetch_pm25_west_daily(start, end)
    pm25.rename(columns={"pm25_daily": "value"}, inplace=True)
    pm25_sheet = to_sheet(pm25.rename(columns={"pm25_daily": "value", "date":"date"}), start, end, "value")

    # --- Earthquake counts per day ---
    print("[INFO] Fetching Earthquake counts per day from USGS ...")
    eq = fetch_eq_counts(start, end)
    if not eq.empty:
        eq.rename(columns={"eq_count": "value"}, inplace=True)
    eq_sheet = to_sheet(eq if not eq.empty else pd.DataFrame(columns=["date","value"]), start, end, "value")

    # --- Sunspot number daily ---
    print("[INFO] Downloading Sunspot daily numbers from SILSO ...")
    ssn = fetch_sunspot_daily(start, end)
    if not ssn.empty:
        ssn.rename(columns={"sunspot": "value"}, inplace=True)
    ssn_sheet = to_sheet(ssn if not ssn.empty else pd.DataFrame(columns=["date","value"]), start, end, "value")

    # --- Write Excel ---
    print(f"[INFO] Writing Excel -> {args.out}")
    with pd.ExcelWriter(args.out, engine="openpyxl") as writer:
        btc_sheet.to_excel(writer, index=False, sheet_name="BTC_USD")
        vix_sheet.to_excel(writer, index=False, sheet_name="VIX")
        pm25_sheet.to_excel(writer, index=False, sheet_name="PM25_West")
        eq_sheet.to_excel(writer, index=False, sheet_name="EQ_Counts")
        ssn_sheet.to_excel(writer, index=False, sheet_name="Sunspot")

    print("[DONE] Wrote 5 sheets to", args.out)
    print("      Sheets: BTC_USD, VIX, PM25_West, EQ_Counts, Sunspot")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

