import pandas as pd


def add_basic_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    g = df.groupby("symbol", sort=False)

    df["ret_1d"] = g["adj_close"].pct_change(1)
    df["ret_5d"] = g["adj_close"].pct_change(5)
    df["ret_20d"] = g["adj_close"].pct_change(20)
    df["ret_60d"] = g["adj_close"].pct_change(60)
    df["ret_120d"] = g["adj_close"].pct_change(120)

    df["vol_20d"] = g["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    df["adv_20d"] = g["dollar_volume"].rolling(20).mean().reset_index(level=0, drop=True)

    return df