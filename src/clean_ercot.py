"""Market-specific cleaning for ERCOT nodal datasets."""

from __future__ import annotations

import pandas as pd

from io_utils import validate_required_columns


def clean_ercot(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned["market"] = "ERCOT"
    cleaned["timestamp"] = pd.to_datetime(cleaned["timestamp"], utc=True, errors="raise")
    validate_required_columns(cleaned)
    return cleaned

