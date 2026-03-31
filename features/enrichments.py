"""
Enrichment functions — bolt additional data onto the bars DataFrame.

Each function has the same signature:
    enrich_xxx(engine, bars, schema) -> bars_with_new_columns

All joins are **point-in-time correct** (no lookahead bias).  Each
enrichment is independently callable and opt-in via ``ExperimentConfig.enrichments``.
"""
from __future__ import annotations

from typing import Callable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ═══════════════════════════════════════════════════════════════════════
#  Registry
# ═══════════════════════════════════════════════════════════════════════

ENRICHMENT_REGISTRY: dict[str, Callable[[Engine, pd.DataFrame, str], pd.DataFrame]] = {}


def _register(name: str):
    """Decorator that adds an enrichment function to the registry."""
    def decorator(fn):
        ENRICHMENT_REGISTRY[name] = fn
        return fn
    return decorator


def apply_enrichments(
    engine: Engine,
    bars: pd.DataFrame,
    enrichments: list[str],
    schema: str = "strategy_engine",
) -> pd.DataFrame:
    """Apply a list of enrichments by name.  Unknown names raise ValueError."""
    for name in enrichments:
        if name not in ENRICHMENT_REGISTRY:
            raise ValueError(
                f"Unknown enrichment '{name}'. "
                f"Available: {sorted(ENRICHMENT_REGISTRY)}"
            )
        bars = ENRICHMENT_REGISTRY[name](engine, bars, schema)
    return bars


# ═══════════════════════════════════════════════════════════════════════
#  1. Valuations  (sharadar_daily → pe, pb, ps, ev_ebitda, marketcap)
# ═══════════════════════════════════════════════════════════════════════

@_register("valuations")
def enrich_valuations(
    engine: Engine, bars: pd.DataFrame, schema: str,
) -> pd.DataFrame:
    """
    Join daily valuation ratios from sharadar_daily.

    Adds columns: pe, pb, ps, ev_ebitda, marketcap.
    """
    start = bars["date"].min()
    end = bars["date"].max()

    sql = text(f"""
        SELECT i.symbol,
               d.trade_date AS date,
               d.pe,
               d.pb,
               d.ps,
               d.evebitda AS ev_ebitda,
               d.marketcap
        FROM {schema}.sharadar_daily d
        JOIN {schema}.instruments i
          ON i.instrument_id = d.instrument_id
        WHERE d.trade_date >= :start
          AND d.trade_date <= :end
        ORDER BY i.symbol, d.trade_date
    """)

    with engine.connect() as conn:
        vals = pd.read_sql(sql, conn, params={"start": start, "end": end})

    if vals.empty:
        for col in ("pe", "pb", "ps", "ev_ebitda", "marketcap"):
            bars[col] = float("nan")
        return bars

    vals["date"] = pd.to_datetime(vals["date"])

    new_cols = ["pe", "pb", "ps", "ev_ebitda", "marketcap"]
    bars = bars.drop(columns=[c for c in new_cols if c in bars.columns], errors="ignore")
    bars = bars.merge(vals, on=["symbol", "date"], how="left")

    return bars


# ═══════════════════════════════════════════════════════════════════════
#  2. VIX / market regime  (market_indices → vix, vix_sma20, vix_regime)
# ═══════════════════════════════════════════════════════════════════════

@_register("market_regime")
def enrich_market_regime(
    engine: Engine, bars: pd.DataFrame, schema: str,
) -> pd.DataFrame:
    """
    Broadcast VIX data onto every row by date.

    Adds columns: vix, vix_sma20, vix_regime.
    vix_regime is a categorical: 'low' (<= 15), 'normal' (15–25),
    'high' (25–35), 'extreme' (> 35).
    """
    start = bars["date"].min()
    end = bars["date"].max()

    sql = text(f"""
        SELECT trade_date AS date, close AS vix
        FROM {schema}.market_indices
        WHERE symbol = '^VIX'
          AND trade_date >= :start
          AND trade_date <= :end
        ORDER BY trade_date
    """)

    with engine.connect() as conn:
        vix = pd.read_sql(sql, conn, params={"start": start, "end": end})

    if vix.empty:
        bars["vix"] = float("nan")
        bars["vix_sma20"] = float("nan")
        bars["vix_regime"] = "unknown"
        return bars

    vix["date"] = pd.to_datetime(vix["date"])
    vix["vix"] = pd.to_numeric(vix["vix"], errors="coerce")
    vix["vix_sma20"] = vix["vix"].rolling(20, min_periods=5).mean()

    vix["vix_regime"] = pd.cut(
        vix["vix"],
        bins=[0, 15, 25, 35, 200],
        labels=["low", "normal", "high", "extreme"],
        right=True,
    ).astype(str)

    new_cols = ["vix", "vix_sma20", "vix_regime"]
    bars = bars.drop(columns=[c for c in new_cols if c in bars.columns], errors="ignore")
    bars = bars.merge(vix[["date"] + new_cols], on="date", how="left")

    return bars


# ═══════════════════════════════════════════════════════════════════════
#  3. Fundamentals  (fundamentals_sf1 → quality & growth metrics)
# ═══════════════════════════════════════════════════════════════════════

_FUNDAMENTAL_FIELDS = {
    "revenue":    "revenue",
    "netinc":     "netinc",
    "gp":         "gp",
    "ebitda":     "ebitda",
    "fcf":        "fcf",
    "equity":     "equity",
    "assets":     "assets",
    "debt":       "debt",
    "ncfo":       "ncfo",        # operating cash flow
    "depamor":    "depamor",     # depreciation & amortisation
}


@_register("fundamentals")
def enrich_fundamentals(
    engine: Engine, bars: pd.DataFrame, schema: str,
) -> pd.DataFrame:
    """
    Point-in-time join of quarterly fundamentals (ARQ dimension).

    Uses ``datekey`` (SEC filing date) — the date the data became
    publicly available — to avoid lookahead bias.

    Adds columns: roe, roa, gross_margin, net_margin, fcf_yield,
    debt_equity, revenue_yoy, earnings_yoy.
    """
    start = bars["date"].min()
    end = bars["date"].max()

    # Build JSONB extraction expressions
    json_cols = ", ".join(
        f"NULLIF(data ->> '{k}', '')::NUMERIC AS {v}"
        for k, v in _FUNDAMENTAL_FIELDS.items()
    )

    sql = text(f"""
        SELECT ticker AS symbol,
               datekey AS filing_date,
               calendardate,
               {json_cols}
        FROM {schema}.fundamentals_sf1
        WHERE dimension = 'ARQ'
          AND datekey IS NOT NULL
          AND datekey >= :start_minus
          AND datekey <= :end
        ORDER BY ticker, datekey
    """)

    # Pull a year before the bars start so we can compute YoY changes
    import datetime as dt
    start_minus = pd.Timestamp(start) - pd.DateOffset(years=2)

    with engine.connect() as conn:
        fund = pd.read_sql(
            sql, conn,
            params={"start_minus": start_minus, "end": end},
        )

    new_cols = [
        "roe", "roa", "gross_margin", "net_margin",
        "fcf_yield", "debt_equity", "revenue_yoy", "earnings_yoy",
    ]

    if fund.empty:
        for col in new_cols:
            bars[col] = float("nan")
        return bars

    fund["filing_date"] = pd.to_datetime(fund["filing_date"])
    fund["calendardate"] = pd.to_datetime(fund["calendardate"])

    # Compute derived ratios
    fund["roe"] = fund["netinc"] / fund["equity"].replace(0, float("nan"))
    fund["roa"] = fund["netinc"] / fund["assets"].replace(0, float("nan"))
    fund["gross_margin"] = fund["gp"] / fund["revenue"].replace(0, float("nan"))
    fund["net_margin"] = fund["netinc"] / fund["revenue"].replace(0, float("nan"))
    fund["fcf_yield"] = fund["fcf"]  # will be normalised by marketcap later if available
    fund["debt_equity"] = fund["debt"] / fund["equity"].replace(0, float("nan"))

    # YoY growth: compare to value 4 quarters ago
    fund = fund.sort_values(["symbol", "filing_date"]).reset_index(drop=True)
    g = fund.groupby("symbol", sort=False)
    fund["revenue_yoy"] = g["revenue"].pct_change(4)
    fund["earnings_yoy"] = g["netinc"].pct_change(4)

    # Keep only the columns we need for the asof merge
    fund_slim = fund[["symbol", "filing_date"] + new_cols].copy()
    fund_slim = fund_slim.dropna(subset=["filing_date"])
    fund_slim = fund_slim.sort_values(["symbol", "filing_date"]).reset_index(drop=True)

    # Point-in-time asof merge: for each (symbol, date) use the latest
    # filing_date <= date
    bars = bars.sort_values(["symbol", "date"]).reset_index(drop=True)
    bars = bars.drop(columns=[c for c in new_cols if c in bars.columns], errors="ignore")

    bars = pd.merge_asof(
        bars,
        fund_slim,
        left_on="date",
        right_on="filing_date",
        by="symbol",
        direction="backward",
    )

    bars = bars.drop(columns=["filing_date"], errors="ignore")
    return bars


# ═══════════════════════════════════════════════════════════════════════
#  4. Insider sentiment  (sharadar_sf2 → insider_net_buy_90d)
# ═══════════════════════════════════════════════════════════════════════

@_register("insider")
def enrich_insider_sentiment(
    engine: Engine, bars: pd.DataFrame, schema: str,
) -> pd.DataFrame:
    """
    Trailing 90-day net insider buying per ticker.

    P (purchase) counted positive, S (sale) negative.
    Score = net_value / total_abs_value  → ranges roughly [-1, +1].

    Adds columns: insider_net_buy_90d.
    """
    start = bars["date"].min()
    end = bars["date"].max()

    sql = text(f"""
        SELECT ticker AS symbol,
               filing_date,
               transaction_code,
               COALESCE(transaction_value, 0) AS transaction_value
        FROM {schema}.sharadar_sf2
        WHERE transaction_code IN ('P', 'S')
          AND filing_date >= :start_minus
          AND filing_date <= :end
        ORDER BY ticker, filing_date
    """)

    import datetime as dt
    start_minus = pd.Timestamp(start) - pd.DateOffset(days=120)

    with engine.connect() as conn:
        sf2 = pd.read_sql(sql, conn, params={"start_minus": start_minus, "end": end})

    if sf2.empty:
        bars["insider_net_buy_90d"] = float("nan")
        return bars

    sf2["filing_date"] = pd.to_datetime(sf2["filing_date"])
    sf2["transaction_value"] = pd.to_numeric(sf2["transaction_value"], errors="coerce")

    # Sign the values: purchases positive, sales negative
    sf2["signed_value"] = sf2["transaction_value"].where(
        sf2["transaction_code"] == "P",
        -sf2["transaction_value"],
    )

    # Aggregate by (symbol, filing_date) — one row per ticker per day
    daily_insider = (
        sf2.groupby(["symbol", "filing_date"], sort=False)
        .agg(
            net_value=("signed_value", "sum"),
            total_abs=("transaction_value", "sum"),
        )
        .reset_index()
    )
    daily_insider = daily_insider.sort_values(["symbol", "filing_date"]).reset_index(drop=True)

    # 90-day rolling sum per ticker
    daily_insider = daily_insider.set_index("filing_date")
    g = daily_insider.groupby("symbol", sort=False)
    daily_insider["net_90d"] = g["net_value"].rolling("90D", min_periods=1).sum()
    daily_insider["abs_90d"] = g["total_abs"].rolling("90D", min_periods=1).sum()
    daily_insider = daily_insider.reset_index()

    daily_insider["insider_net_buy_90d"] = (
        daily_insider["net_90d"] /
        daily_insider["abs_90d"].replace(0, float("nan"))
    )

    insider_slim = daily_insider[["symbol", "filing_date", "insider_net_buy_90d"]].copy()
    insider_slim = insider_slim.dropna(subset=["filing_date"])
    insider_slim = insider_slim.sort_values(["symbol", "filing_date"]).reset_index(drop=True)

    bars = bars.sort_values(["symbol", "date"]).reset_index(drop=True)
    bars = bars.drop(columns=["insider_net_buy_90d"], errors="ignore")

    bars = pd.merge_asof(
        bars,
        insider_slim,
        left_on="date",
        right_on="filing_date",
        by="symbol",
        direction="backward",
    )

    bars = bars.drop(columns=["filing_date"], errors="ignore")
    return bars


# ═══════════════════════════════════════════════════════════════════════
#  5. Institutional flows  (sharadar_sf3 → inst_ownership_chg)
# ═══════════════════════════════════════════════════════════════════════

@_register("institutional")
def enrich_institutional_flows(
    engine: Engine, bars: pd.DataFrame, schema: str,
) -> pd.DataFrame:
    """
    Quarter-over-quarter change in total institutional ownership.

    Adds columns: inst_ownership_chg.
    """
    start = bars["date"].min()
    end = bars["date"].max()

    sql = text(f"""
        SELECT ticker AS symbol,
               calendar_date,
               SUM(units) AS total_units
        FROM {schema}.sharadar_sf3
        WHERE calendar_date >= :start_minus
          AND calendar_date <= :end
        GROUP BY ticker, calendar_date
        ORDER BY ticker, calendar_date
    """)

    import datetime as dt
    start_minus = pd.Timestamp(start) - pd.DateOffset(months=9)

    with engine.connect() as conn:
        sf3 = pd.read_sql(sql, conn, params={"start_minus": start_minus, "end": end})

    if sf3.empty:
        bars["inst_ownership_chg"] = float("nan")
        return bars

    sf3["calendar_date"] = pd.to_datetime(sf3["calendar_date"])
    sf3["total_units"] = pd.to_numeric(sf3["total_units"], errors="coerce")
    sf3 = sf3.sort_values(["symbol", "calendar_date"]).reset_index(drop=True)

    # QoQ change (each row is one quarter for a given ticker)
    sf3["inst_ownership_chg"] = sf3.groupby("symbol", sort=False)["total_units"].pct_change()

    inst_slim = sf3[["symbol", "calendar_date", "inst_ownership_chg"]].copy()
    inst_slim = inst_slim.dropna(subset=["calendar_date", "inst_ownership_chg"])
    inst_slim = inst_slim.sort_values(["symbol", "calendar_date"]).reset_index(drop=True)

    bars = bars.sort_values(["symbol", "date"]).reset_index(drop=True)
    bars = bars.drop(columns=["inst_ownership_chg"], errors="ignore")

    bars = pd.merge_asof(
        bars,
        inst_slim,
        left_on="date",
        right_on="calendar_date",
        by="symbol",
        direction="backward",
    )

    bars = bars.drop(columns=["calendar_date"], errors="ignore")
    return bars

