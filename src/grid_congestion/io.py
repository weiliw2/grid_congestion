"""Input/output helpers for congestion analysis."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .schema import REQUIRED_COLUMNS


def load_nodal_prices(path: str | Path) -> pd.DataFrame:
    """Load a CSV of nodal prices and validate the required columns."""
    df = pd.read_csv(path)

    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Input data is missing required columns: {missing_list}")

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="raise")

    numeric_columns = ["lmp", "energy", "congestion", "loss", "renewable_mw", "load_mw"]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def ensure_output_dir(path: str | Path) -> Path:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir
