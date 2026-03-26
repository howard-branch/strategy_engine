import numpy as np
import pandas as pd


def performance_summary(backtest_daily: pd.DataFrame) -> dict:
    r = backtest_daily["net_return"].fillna(0.0)

    if len(r) == 0:
        return {}

    equity = (1.0 + r).cumprod()
    peak = equity.cummax()
    drawdown = equity / peak - 1.0

    total_return = equity.iloc[-1] - 1.0
    ann_return = (1.0 + total_return) ** (252 / len(r)) - 1.0
    ann_vol = r.std(ddof=1) * np.sqrt(252)
    sharpe = ann_return / ann_vol if ann_vol > 0 else np.nan
    max_dd = drawdown.min()
    avg_turnover = backtest_daily["turnover"].mean()

    return {
        "total_return": total_return,
        "annualised_return": ann_return,
        "annualised_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "avg_daily_turnover": avg_turnover,
    }