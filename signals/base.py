from dataclasses import dataclass
import pandas as pd



@dataclass
class SignalOutput:
    scores: pd.DataFrame   # columns: date, symbol, score


class SignalBase:
    def generate(self, df: pd.DataFrame) -> SignalOutput:
        raise NotImplementedError

    @staticmethod
    def zscore_by_date(df: pd.DataFrame, col: str) -> pd.Series:
        return df.groupby("date")[col].transform(
            lambda x: (x - x.mean()) / (x.std(ddof=1) + 1e-8)
        )