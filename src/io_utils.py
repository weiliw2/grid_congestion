"""Shared file and dataframe IO helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = {
    "timestamp",
    "market",
    "node_id",
    "node_name",
    "lmp",
    "energy",
    "congestion",
    "loss",
}


def ensure_dir(path: str | Path) -> Path:
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def load_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path)


def load_dataframe(path: str | Path) -> pd.DataFrame:
    input_path = Path(path)
    if input_path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(input_path)
    return pd.read_csv(input_path)


def write_csv(df: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    ensure_dir(output_path.parent)
    df.to_csv(output_path, index=False)
    return output_path


def write_dataframe(df: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    ensure_dir(output_path.parent)
    if output_path.suffix.lower() in {".parquet", ".pq"}:
        df.to_parquet(output_path, index=False)
    else:
        df.to_csv(output_path, index=False)
    return output_path


def validate_required_columns(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Missing required columns: {missing_str}")
