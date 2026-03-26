from dataclasses import dataclass
from typing import Dict, List, Set

import pandas as pd


@dataclass
class PortfolioConfig:
    # Target number of names per side
    top_n: int = 10

    # Hysteresis:
    # enter when in top/bottom entry_n
    # remain until outside top/bottom exit_n
    entry_n: int = 10
    exit_n: int = 20

    # Gross exposure
    gross_long: float = 0.5
    gross_short: float = 0.5

    # Rebalance frequency:
    # "D"      = every trading day
    # "W-FRI"  = last trading day of each week
    rebalance_frequency: str = "W-FRI"

    # Safety / behaviour
    min_names_per_side: int = 3
    allow_flips_same_day: bool = False
    renormalise_missing: bool = True


def construct_rank_based_portfolio(
        scores: pd.DataFrame,
        config: PortfolioConfig | None = None,
) -> pd.DataFrame:
    """
    Build a stateful rank-based portfolio with persistence + hysteresis.

    Expected input columns:
        - date
        - symbol
        - score
    Optional input columns:
        - tradable   (bool). If present, entries with tradable == False are excluded.

    Output columns:
        - date
        - symbol
        - target_weight

    Notes:
        - Emits DAILY weights, carrying positions forward between rebalance dates.
        - On rebalance dates:
            * existing longs are kept while still inside top `exit_n`
            * new longs only enter from top `entry_n`
            * existing shorts are kept while still inside bottom `exit_n`
            * new shorts only enter from bottom `entry_n`
        - This keeps your existing simulator compatible.
    """
    config = config or PortfolioConfig()

    required_cols = {"date", "symbol", "score"}
    missing = required_cols - set(scores.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df = scores.copy()
    df["date"] = pd.to_datetime(df["date"])

    if "tradable" in df.columns:
        df = df[df["tradable"].fillna(False)].copy()

    df = df.dropna(subset=["date", "symbol", "score"]).copy()
    df = df.sort_values(["date", "symbol"]).reset_index(drop=True)

    if df.empty:
        return pd.DataFrame(columns=["date", "symbol", "target_weight"])

    all_dates = list(df["date"].drop_duplicates().sort_values())
    rebalance_dates = set(_select_rebalance_dates(all_dates, config.rebalance_frequency))

    current_longs: Set[str] = set()
    current_shorts: Set[str] = set()
    current_weights: Dict[str, float] = {}

    frames: List[pd.DataFrame] = []

    for date in all_dates:
        day = df.loc[df["date"] == date, ["date", "symbol", "score"]].copy()
        day = day.drop_duplicates(subset=["symbol"], keep="last")
        day = day.sort_values("score", ascending=False).reset_index(drop=True)

        if day.empty:
            continue

        available_symbols = set(day["symbol"].tolist())

        if date in rebalance_dates:
            current_longs, current_shorts = _rebalance_membership(
                day=day,
                current_longs=current_longs,
                current_shorts=current_shorts,
                config=config,
            )
            current_weights = _build_equal_weights(
                longs=current_longs,
                shorts=current_shorts,
                gross_long=config.gross_long,
                gross_short=config.gross_short,
            )
        else:
            # Carry forward existing weights on non-rebalance days.
            current_weights = {
                symbol: weight
                for symbol, weight in current_weights.items()
                if symbol in available_symbols
            }

            if config.renormalise_missing:
                current_weights = _renormalise_weights(
                    current_weights,
                    gross_long=config.gross_long,
                    gross_short=config.gross_short,
                )

            current_longs = {s for s, w in current_weights.items() if w > 0}
            current_shorts = {s for s, w in current_weights.items() if w < 0}

        if current_weights:
            out = pd.DataFrame(
                {
                    "date": date,
                    "symbol": list(current_weights.keys()),
                    "target_weight": list(current_weights.values()),
                }
            ).sort_values("symbol")
            frames.append(out)

    if not frames:
        return pd.DataFrame(columns=["date", "symbol", "target_weight"])

    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )


def _select_rebalance_dates(
        dates: List[pd.Timestamp],
        frequency: str,
) -> List[pd.Timestamp]:
    s = pd.Series(pd.to_datetime(dates)).sort_values().drop_duplicates().reset_index(drop=True)

    if frequency == "D":
        return list(s)

    if frequency == "W-FRI":
        # Choose the last trading day present in each ISO week bucket.
        week_key = s.dt.strftime("%G-%V")
        return [grp.iloc[-1] for _, grp in s.groupby(week_key)]

    raise ValueError(f"Unsupported rebalance_frequency: {frequency}")


def _rebalance_membership(
        day: pd.DataFrame,
        current_longs: Set[str],
        current_shorts: Set[str],
        config: PortfolioConfig,
) -> tuple[Set[str], Set[str]]:
    """
    day must contain:
        symbol, score
    sorted order is not relied on.
    """
    if len(day) < 2 * config.min_names_per_side:
        return set(), set()

    ranks_desc = (
        day.sort_values("score", ascending=False)[["symbol"]]
        .reset_index(drop=True)
        .assign(long_rank=lambda x: x.index + 1)
    )
    ranks_asc = (
        day.sort_values("score", ascending=True)[["symbol"]]
        .reset_index(drop=True)
        .assign(short_rank=lambda x: x.index + 1)
    )

    ranks = ranks_desc.merge(ranks_asc, on="symbol", how="inner")

    long_entry_pool = set(ranks.loc[ranks["long_rank"] <= config.entry_n, "symbol"])
    long_keepable = set(ranks.loc[ranks["long_rank"] <= config.exit_n, "symbol"])

    short_entry_pool = set(ranks.loc[ranks["short_rank"] <= config.entry_n, "symbol"])
    short_keepable = set(ranks.loc[ranks["short_rank"] <= config.exit_n, "symbol"])

    # 1. Keep incumbents while still inside the looser exit threshold
    new_longs = {s for s in current_longs if s in long_keepable}
    new_shorts = {s for s in current_shorts if s in short_keepable}

    # 2. Prevent same-day flips unless explicitly allowed
    if not config.allow_flips_same_day:
        long_entry_pool -= new_shorts
        short_entry_pool -= new_longs

    # 3. Fill long side from best available entry candidates
    ranked_long_candidates = (
        ranks.loc[ranks["symbol"].isin(long_entry_pool)]
        .sort_values("long_rank")["symbol"]
        .tolist()
    )
    for symbol in ranked_long_candidates:
        if len(new_longs) >= config.top_n:
            break
        if symbol not in new_longs and symbol not in new_shorts:
            new_longs.add(symbol)

    # 4. Fill short side from best available entry candidates
    ranked_short_candidates = (
        ranks.loc[ranks["symbol"].isin(short_entry_pool)]
        .sort_values("short_rank")["symbol"]
        .tolist()
    )
    for symbol in ranked_short_candidates:
        if len(new_shorts) >= config.top_n:
            break
        if symbol not in new_shorts and symbol not in new_longs:
            new_shorts.add(symbol)

    # 5. Fallback: if too few names remain, relax to best available
    if len(new_longs) < config.min_names_per_side:
        fallback_longs = ranks.sort_values("long_rank")["symbol"].tolist()
        for symbol in fallback_longs:
            if len(new_longs) >= config.top_n:
                break
            if symbol not in new_longs and symbol not in new_shorts:
                new_longs.add(symbol)

    if len(new_shorts) < config.min_names_per_side:
        fallback_shorts = ranks.sort_values("short_rank")["symbol"].tolist()
        for symbol in fallback_shorts:
            if len(new_shorts) >= config.top_n:
                break
            if symbol not in new_shorts and symbol not in new_longs:
                new_shorts.add(symbol)

    return new_longs, new_shorts


def _build_equal_weights(
        longs: Set[str],
        shorts: Set[str],
        gross_long: float,
        gross_short: float,
) -> Dict[str, float]:
    weights: Dict[str, float] = {}

    if longs:
        long_weight = gross_long / len(longs)
        for symbol in sorted(longs):
            weights[symbol] = long_weight

    if shorts:
        short_weight = -gross_short / len(shorts)
        for symbol in sorted(shorts):
            weights[symbol] = short_weight

    return weights


def _renormalise_weights(
        weights: Dict[str, float],
        gross_long: float,
        gross_short: float,
) -> Dict[str, float]:
    if not weights:
        return {}

    out = dict(weights)

    long_symbols = [s for s, w in out.items() if w > 0]
    short_symbols = [s for s, w in out.items() if w < 0]

    long_sum = sum(out[s] for s in long_symbols)
    short_sum = -sum(out[s] for s in short_symbols)

    if long_symbols and long_sum > 0:
        scale = gross_long / long_sum
        for s in long_symbols:
            out[s] *= scale

    if short_symbols and short_sum > 0:
        scale = gross_short / short_sum
        for s in short_symbols:
            out[s] *= scale

    return out