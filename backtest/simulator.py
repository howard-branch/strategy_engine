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
    config = config or SimulatorConfig()

    rets = bars[["date", "symbol", "ret_1d"]].copy()
    rets = rets.rename(columns={"ret_1d": "asset_return"})

    w = weights.copy()
    w["date"] = pd.to_datetime(w["date"])
    rets["date"] = pd.to_datetime(rets["date"])

    # shift weights forward one bar: signal on t, hold on t+1
    unique_dates = sorted(rets["date"].drop_duplicates())
    next_date_map = {unique_dates[i]: unique_dates[i + 1] for i in range(len(unique_dates) - 1)}

    w["hold_date"] = w["date"].map(next_date_map)
    w = w.dropna(subset=["hold_date"]).copy()
    w = w.drop(columns=["date"]).rename(columns={"hold_date": "date"})

    merged = w.merge(rets, on=["date", "symbol"], how="left")
    merged["asset_return"] = merged["asset_return"].fillna(0.0)
    merged["weighted_return"] = merged["target_weight"] * merged["asset_return"]

    gross = (
        merged.groupby("date", sort=True)["weighted_return"]
        .sum()
        .rename("gross_return")
        .reset_index()
    )

    weights_wide = (
        w.pivot(index="date", columns="symbol", values="target_weight")
        .fillna(0.0)
        .sort_index()
    )

    turnover = weights_wide.diff().abs().sum(axis=1)
    if len(weights_wide) > 0:
        turnover.iloc[0] = weights_wide.iloc[0].abs().sum()

    turnover = turnover.rename("turnover").reset_index()

    out = gross.merge(turnover, on="date", how="left")
    out["turnover"] = out["turnover"].fillna(0.0)
    out["transaction_cost"] = out["turnover"] * config.cost_per_unit_turnover
    out["net_return"] = out["gross_return"] - out["transaction_cost"]
    out["equity_curve"] = (1.0 + out["net_return"]).cumprod()

    return out.sort_values("date").reset_index(drop=True)