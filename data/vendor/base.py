from abc import ABC, abstractmethod
import pandas as pd

class VendorHistoryClient(ABC):
    @abstractmethod
    def fetch_daily_bars(
            self,
            symbol: str,
            start_date: str,
            end_date: str,
    ) -> pd.DataFrame:
        """Return columns compatible with internal raw-bar schema."""
        raise NotImplementedError