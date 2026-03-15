"""Spatial helpers for node metadata."""

from __future__ import annotations

import pandas as pd


def attach_coordinates(df: pd.DataFrame, reference_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if reference_df is None:
        return df.copy()

    join_columns = [column for column in ["market", "node_id", "latitude", "longitude"] if column in reference_df.columns]
    if set(["market", "node_id"]).issubset(join_columns):
        coords = reference_df[join_columns].drop_duplicates()
        return df.merge(coords, on=["market", "node_id"], how="left")
    return df.copy()

