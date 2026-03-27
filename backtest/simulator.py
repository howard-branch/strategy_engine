from dataclasses import dataclass

import pandas as pd


@dataclass
class SimulatorConfig:
    cost_per_unit_turnover: float = 0.0005  # 5 bps


def simulate_backtest(
        weights: pd.DataFrame,
        bars: pd.DataFrame,
        config: SimulatorConfig | None = None,
) -> pd.DataFrame:
    """
    Simulate a backtest from target weights and daily bars.

    Expected weights columns:
        - date          : signal / rebalance date
        - symbol
        - target_weight

    Expected bars columns:
        - date
        - symbol
        - ret_1d

    Mechanics:
        - weights decided on date t are implemented on the next trading day
        - implemented weights are then held constant until the next rebalance
        - turnover is computed on the daily held-weight matrix
        - transaction cost = turnover * cost_per_unit_turnover
    """
    config = config or SimulatorConfig()

    required_weight_cols = {"date", "symbol", "target_weight"}
    missing_weight_cols = required_weight_cols - set(weights.columns)
    if missing_weight_cols:
        raise ValueError(f"weights missing required columns: {sorted(missing_weight_cols)}")

    required_bar_cols = {"date", "symbol", "ret_1d"}
    missing_bar_cols = required_bar_cols - set(bars.columns)
    if missing_bar_cols:
        raise ValueError(f"bars missing required columns: {sorted(missing_bar_cols)}")

    rets = bars[["date", "symbol", "ret_1d"]].copy()
    rets["date"] = pd.to_datetime(rets["date"])
    rets = rets.rename(columns={"ret_1d": "asset_return"})
    rets = rets.sort_values(["date", "symbol"]).reset_index(drop=True)

    w = weights[["date", "symbol", "target_weight"]].copy()
    w["date"] = pd.to_datetime(w["date"])
    w = w.sort_values(["date", "symbol"]).reset_index(drop=True)

    if rets.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "gross_return",
                "turnover",
                "transaction_cost",
                "net_return",
                "equity_curve",
            ]
        )

    trading_dates = pd.Index(sorted(rets["date"].drop_duplicates()))

    if w.empty:
        out = pd.DataFrame({"date": trading_dates})
        out["gross_return"] = 0.0
        out["turnover"] = 0.0
        out["transaction_cost"] = 0.0
        out["net_return"] = 0.0
        out["equity_curve"] = 1.0
        return out.reset_index(drop=True)

    # ------------------------------------------------------------------
    # 1) Convert signal date -> implementation date (next trading day)
    # ------------------------------------------------------------------
    next_date_map = {
        trading_dates[i]: trading_dates[i + 1]
        for i in range(len(trading_dates) - 1)
    }

    w["hold_date"] = w["date"].map(next_date_map)
    w = w.dropna(subset=["hold_date"]).copy()
    w["hold_date"] = pd.to_datetime(w["hold_date"])

    if w.empty:
        out = pd.DataFrame({"date": trading_dates})
        out["gross_return"] = 0.0
        out["turnover"] = 0.0
        out["transaction_cost"] = 0.0
        out["net_return"] = 0.0
        out["equity_curve"] = 1.0
        return out.reset_index(drop=True)

    # ------------------------------------------------------------------
    # 2) Build daily held-weight matrix across ALL trading dates
    #
    # Each hold_date row is a fresh target portfolio, which should remain
    # in force until the next hold_date. We achieve that by:
    #   - pivoting to wide on hold_date
    #   - filling missing symbols on rebalance dates with 0
    #   - reindexing to the full daily trading calendar
    #   - forward-filling through time
    # ------------------------------------------------------------------
    target_wide = (
        w.pivot_table(
            index="hold_date",
            columns="symbol",
            values="target_weight",
            aggfunc="last",
        )
        .sort_index()
        .fillna(0.0)
    )

    held_weights_wide = (
        target_wide.reindex(trading_dates)
        .ffill()
        .fillna(0.0)
    )
    held_weights_wide.index.name = "date"

    # ------------------------------------------------------------------
    # 3) Daily portfolio returns using held weights
    # ------------------------------------------------------------------
    held_weights_long = (
        held_weights_wide
        .astype(float)
        .stack()
        .rename("target_weight")
        .reset_index()
    )

    merged = held_weights_long.merge(rets, on=["date", "symbol"], how="left")
    merged["asset_return"] = merged["asset_return"].fillna(0.0)
    merged["weighted_return"] = merged["target_weight"] * merged["asset_return"]

    gross = (
        merged.groupby("date", sort=True)["weighted_return"]
        .sum()
        .rename("gross_return")
        .reset_index()
    )

    # ------------------------------------------------------------------
    # 4) Daily turnover from held weights
    #
    # On non-rebalance days, weights are unchanged => turnover 0.
    # On implementation dates, turnover captures the actual book change.
    # ------------------------------------------------------------------
    turnover = held_weights_wide.diff().abs().sum(axis=1)

    if len(turnover) > 0:
        first_nonzero_idx = (held_weights_wide.abs().sum(axis=1) > 0).idxmax()
        if held_weights_wide.loc[first_nonzero_idx].abs().sum() > 0:
            turnover.loc[first_nonzero_idx] = held_weights_wide.loc[first_nonzero_idx].abs().sum()

    turnover = turnover.fillna(0.0).rename("turnover").reset_index()

    # ------------------------------------------------------------------
    # 5) Costs and equity curve
    # ------------------------------------------------------------------
    out = gross.merge(turnover, on="date", how="left")
    out["turnover"] = out["turnover"].fillna(0.0)
    out["transaction_cost"] = out["turnover"] * config.cost_per_unit_turnover
    out["net_return"] = out["gross_return"] - out["transaction_cost"]
    out["equity_curve"] = (1.0 + out["net_return"]).cumprod()

    return out.sort_values("date").reset_index(drop=True)