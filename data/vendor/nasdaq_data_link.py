import pandas as pd
from .base import VendorHistoryClient

class NasdaqDataLinkClient(VendorHistoryClient):
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key

    def fetch_daily_bars(
            self,
            symbol: str,
            start_date: str,
            end_date: str,
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Hook this up once you subscribe to Nasdaq Data Link."
        )