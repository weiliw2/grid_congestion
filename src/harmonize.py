"""Schema harmonization utilities."""

from __future__ import annotations

import pandas as pd

from io_utils import REQUIRED_COLUMNS, validate_required_columns


def harmonize_market_dataframes(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        raise ValueError("At least one dataframe is required for harmonization.")

    normalized: list[pd.DataFrame] = []
    for frame in frames:
        candidate = frame.copy()
        validate_required_columns(candidate)
        candidate["timestamp"] = pd.to_datetime(candidate["timestamp"], utc=True, errors="raise")

        numeric_columns = ["lmp", "energy", "congestion", "loss", "renewable_mw", "load_mw"]
        for column in numeric_columns:
            if column in candidate.columns:
                candidate[column] = pd.to_numeric(candidate[column], errors="coerce")

        normalized.append(candidate)

    combined = pd.concat(normalized, ignore_index=True, sort=False)
    ordered_columns = list(REQUIRED_COLUMNS) + [
        column for column in combined.columns if column not in REQUIRED_COLUMNS
    ]
    return combined.loc[:, ordered_columns]

