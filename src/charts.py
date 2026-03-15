"""Chart-ready aggregations and placeholder figure exports."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from io_utils import ensure_dir


def build_hourly_profile(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["market", "hour_ending"])
        .agg(
            avg_lmp=("lmp", "mean"),
            avg_abs_congestion=("abs_congestion", "mean"),
        )
        .reset_index()
    )


def build_monthly_profile(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["market", "month"])
        .agg(avg_abs_congestion=("abs_congestion", "mean"))
        .reset_index()
    )


def save_placeholder_figure(path: str | Path, title: str) -> Path:
    output = Path(path)
    ensure_dir(output.parent)
    output.write_text(f"Placeholder figure: {title}\n", encoding="utf-8")
    return output

