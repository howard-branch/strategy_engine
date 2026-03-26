from __future__ import annotations

import pandas as pd
import yfinance as yf

from data.vendor.base import VendorHistoryClient


class YahooHistoryClient(VendorHistoryClient):
    def fetch_daily_bars(
            self,
            symbol: str,
            start_date: str,
            end_date: str,
    ) -> pd.DataFrame:
        # Keep explicit settings. Do not rely on yfinance defaults.
        df = yf.download(
            tickers=symbol,
            start=start_date,
            end=end_date,
            interval="1d",
            auto_adjust=False,
            actions=False,
            progress=False,
            repair=False,
            threads=False,
        )

        if df is None or df.empty:
            return pd.DataFrame(
                columns=[
                    "date",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "adj_close",
                    "volume",
                ]
            )

        # Flatten just in case
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]

        df = df.reset_index()

        # yfinance may return Date or Datetime depending on context
        date_col = "Date" if "Date" in df.columns else df.columns[0]

        rename_map = {
            date_col: "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        df = df.rename(columns=rename_map)

        required = ["date", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"{symbol}: missing Yahoo columns {missing}")

        # If Adj Close is missing for any reason, fall back to Close
        if "adj_close" not in df.columns:
            df["adj_close"] = df["close"]

        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        df["symbol"] = symbol

        out = df[
            ["date", "symbol", "open", "high", "low", "close", "adj_close", "volume"]
        ].copy()

        numeric_cols = ["open", "high", "low", "close", "adj_close", "volume"]
        for col in numeric_cols:
            out[col] = pd.to_numeric(out[col], errors="coerce")

        out = out.dropna(subset=["date", "symbol", "close", "adj_close", "volume"])
        out = out.sort_values("date").reset_index(drop=True)
        return out