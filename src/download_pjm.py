"""PJM downloads powered by gridstatus."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd

from io_utils import ensure_dir
from logging_utils import get_logger

LOGGER = get_logger(__name__)


def _import_gridstatus() -> Any:
    try:
        import gridstatus  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "gridstatus is required for live PJM downloads. Install it with "
            "`pip install -r requirements.txt`."
        ) from exc
    return gridstatus


def _match_column(columns: list[str], candidates: list[str]) -> str | None:
    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def _extract_timestamp_column(df: pd.DataFrame) -> pd.Series:
    column = _match_column(
        list(df.columns),
        [
            "Interval Start",
            "Interval Start GMT",
            "Time",
            "Datetime",
            "Timestamp",
        ],
    )
    if column is None:
        raise ValueError("Unable to locate a timestamp column in PJM gridstatus output.")
    return pd.to_datetime(df[column], utc=True, errors="coerce")


def _extract_value_column(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    column = _match_column(list(df.columns), candidates)
    if column is None:
        raise ValueError(f"Unable to locate `{label}` in PJM gridstatus output.")
    return column


def _normalize_pjm_lmp(lmp_df: pd.DataFrame) -> pd.DataFrame:
    timestamp = _extract_timestamp_column(lmp_df)
    node_id_col = _extract_value_column(
        lmp_df,
        ["Pnode ID", "PNode ID", "Location Id", "Location ID", "Node ID"],
        "node_id",
    )
    node_name_col = _extract_value_column(
        lmp_df,
        ["Pnode Name", "PNode Name", "Location Name", "Node Name"],
        "node_name",
    )
    lmp_col = _extract_value_column(lmp_df, ["LMP"], "lmp")
    energy_col = _extract_value_column(
        lmp_df,
        ["Energy", "Energy Price", "System Energy Price"],
        "energy",
    )
    congestion_col = _extract_value_column(
        lmp_df,
        ["Congestion", "Congestion Price"],
        "congestion",
    )
    loss_col = _extract_value_column(lmp_df, ["Loss", "Loss Price"], "loss")
    zone_col = _match_column(list(lmp_df.columns), ["Zone", "Location Short Name", "Type"])

    normalized = pd.DataFrame(
        {
            "timestamp": timestamp,
            "market": "PJM",
            "node_id": lmp_df[node_id_col].astype(str),
            "node_name": lmp_df[node_name_col].astype(str),
            "lmp": pd.to_numeric(lmp_df[lmp_col], errors="coerce"),
            "energy": pd.to_numeric(lmp_df[energy_col], errors="coerce"),
            "congestion": pd.to_numeric(lmp_df[congestion_col], errors="coerce"),
            "loss": pd.to_numeric(lmp_df[loss_col], errors="coerce"),
        }
    )
    if zone_col:
        normalized["zone"] = lmp_df[zone_col].astype(str)
    return normalized.dropna(subset=["timestamp", "lmp"])


def _normalize_pjm_load(load_df: pd.DataFrame) -> pd.DataFrame:
    timestamp = _extract_timestamp_column(load_df)
    load_col = _extract_value_column(
        load_df,
        ["Load", "Load MW", "Load Forecast", "Instantaneous Load"],
        "load_mw",
    )
    normalized = pd.DataFrame(
        {
            "timestamp": timestamp,
            "load_mw": pd.to_numeric(load_df[load_col], errors="coerce"),
        }
    )
    return normalized.dropna(subset=["timestamp"]).groupby("timestamp", as_index=False)["load_mw"].mean()


def stage_pjm_file(source_path: str | Path, raw_dir: str | Path) -> Path:
    """Register a PJM file location by ensuring its target directory exists."""
    ensure_dir(raw_dir)
    return Path(source_path)


def download_pjm_dataset(
    start: str,
    end: str,
    raw_dir: str | Path,
    market: str = "REAL_TIME_HOURLY",
    locations: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    gridstatus = _import_gridstatus()
    api_key = os.getenv("PJM_API_KEY")
    if not api_key:
        raise ValueError("PJM_API_KEY is required for PJM downloads via gridstatus.")

    output_dir = ensure_dir(raw_dir)
    iso = gridstatus.PJM(api_key=api_key)

    LOGGER.info("Downloading PJM LMP data from %s to %s", start, end)
    lmp_df = iso.get_lmp(start=start, end=end, market=market, locations=locations)
    LOGGER.info("Downloading PJM load data from %s to %s", start, end)
    load_df = iso.get_load(start=start, end=end)
    LOGGER.info("Downloading PJM interconnection queue snapshot")
    queue_df = iso.get_interconnection_queue()

    raw_paths = {
        "lmp": output_dir / "pjm_lmp_raw.csv",
        "load": output_dir / "pjm_load_raw.csv",
        "interconnection_queue": output_dir / "pjm_interconnection_queue.csv",
    }
    lmp_df.to_csv(raw_paths["lmp"], index=False)
    load_df.to_csv(raw_paths["load"], index=False)
    queue_df.to_csv(raw_paths["interconnection_queue"], index=False)

    normalized = _normalize_pjm_lmp(lmp_df)
    normalized_load = _normalize_pjm_load(load_df)
    merged = normalized.merge(normalized_load, on="timestamp", how="left")
    return merged, raw_paths
