from dataclasses import dataclass
import pandas as pd


@dataclass
class PortfolioConfig:
    top_n: int = 10
    gross_long: float = 0.5
    gross_short: float = 0.5


def construct_rank_based_portfolio(
        scores: pd.DataFrame,
        config: PortfolioConfig | None = None,
) -> pd.DataFrame:
    config = config or PortfolioConfig()

    frames = []

    for date, day in scores.groupby("date", sort=True):
        day = day.dropna(subset=["score"]).copy()
        if len(day) < 2 * config.top_n:
            continue

        day = day.sort_values("score")

        shorts = day.head(config.top_n).copy()
        longs = day.tail(config.top_n).copy()

        longs["target_weight"] = config.gross_long / config.top_n
        shorts["target_weight"] = -config.gross_short / config.top_n

        out = pd.concat([longs, shorts], axis=0)
        out = out[["date", "symbol", "target_weight"]]
        frames.append(out)

    if not frames:
        return pd.DataFrame(columns=["date", "symbol", "target_weight"])

    return pd.concat(frames, ignore_index=True).sort_values(["date", "symbol"]).reset_index(drop=True)