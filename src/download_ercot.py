"""ERCOT downloads powered by gridstatus."""

from __future__ import annotations

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
            "gridstatus is required for live ERCOT downloads. Install it with "
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
            "SCED Timestamp",
            "Time",
            "Datetime",
            "Timestamp",
        ],
    )
    if column is None:
        raise ValueError("Unable to locate a timestamp column in ERCOT gridstatus output.")
    return pd.to_datetime(df[column], utc=True, errors="coerce")


def _extract_value_column(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    column = _match_column(list(df.columns), candidates)
    if column is None:
        raise ValueError(f"Unable to locate `{label}` in ERCOT gridstatus output.")
    return column


def _normalize_ercot_lmp(lmp_df: pd.DataFrame) -> pd.DataFrame:
    timestamp = _extract_timestamp_column(lmp_df)
    node_id_col = _extract_value_column(
        lmp_df,
        ["Location", "Settlement Point", "Electrical Bus", "Location Name", "Node"],
        "node_id",
    )
    node_name_col = _match_column(
        list(lmp_df.columns),
        ["Location Name", "Settlement Point Name", "Electrical Bus", "Location"],
    ) or node_id_col
    lmp_col = _extract_value_column(lmp_df, ["LMP"], "lmp")
    energy_col = _match_column(list(lmp_df.columns), ["Energy", "System Lambda"])
    congestion_col = _match_column(list(lmp_df.columns), ["Congestion", "Congestion Price"])
    loss_col = _match_column(list(lmp_df.columns), ["Loss", "Loss Price"])
    location_type_col = _match_column(list(lmp_df.columns), ["Location Type", "Type"])

    normalized = pd.DataFrame(
        {
            "timestamp": timestamp,
            "market": "ERCOT",
            "node_id": lmp_df[node_id_col].astype(str),
            "node_name": lmp_df[node_name_col].astype(str),
            "lmp": pd.to_numeric(lmp_df[lmp_col], errors="coerce"),
            "energy": pd.to_numeric(lmp_df[energy_col], errors="coerce") if energy_col else pd.NA,
            "congestion": pd.to_numeric(lmp_df[congestion_col], errors="coerce") if congestion_col else pd.NA,
            "loss": pd.to_numeric(lmp_df[loss_col], errors="coerce") if loss_col else pd.NA,
        }
    )
    if location_type_col:
        normalized["zone"] = lmp_df[location_type_col].astype(str)
    return normalized.dropna(subset=["timestamp", "lmp"])


def _normalize_ercot_load(load_df: pd.DataFrame) -> pd.DataFrame:
    timestamp = _extract_timestamp_column(load_df)
    load_col = _extract_value_column(
        load_df,
        ["Load", "Demand", "Total Load", "Load MW"],
        "load_mw",
    )
    normalized = pd.DataFrame(
        {
            "timestamp": timestamp,
            "load_mw": pd.to_numeric(load_df[load_col], errors="coerce"),
        }
    )
    return normalized.dropna(subset=["timestamp"]).groupby("timestamp", as_index=False)["load_mw"].mean()


def stage_ercot_file(source_path: str | Path, raw_dir: str | Path) -> Path:
    """Register an ERCOT file location by ensuring its target directory exists."""
    ensure_dir(raw_dir)
    return Path(source_path)


def download_ercot_dataset(
    start: str,
    end: str,
    raw_dir: str | Path,
    locations: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    gridstatus = _import_gridstatus()
    output_dir = ensure_dir(raw_dir)
    iso = gridstatus.ERCOT()

    LOGGER.info("Downloading ERCOT LMP data from %s to %s", start, end)
    lmp_df = iso.get_lmp(start=start, end=end, locations=locations)
    LOGGER.info("Downloading ERCOT load data from %s to %s", start, end)
    load_df = iso.get_load(start=start, end=end)
    LOGGER.info("Downloading ERCOT interconnection queue snapshot")
    queue_df = iso.get_interconnection_queue()

    raw_paths = {
        "lmp": output_dir / "ercot_lmp_raw.csv",
        "load": output_dir / "ercot_load_raw.csv",
        "interconnection_queue": output_dir / "ercot_interconnection_queue.csv",
    }
    lmp_df.to_csv(raw_paths["lmp"], index=False)
    load_df.to_csv(raw_paths["load"], index=False)
    queue_df.to_csv(raw_paths["interconnection_queue"], index=False)

    normalized = _normalize_ercot_lmp(lmp_df)
    normalized_load = _normalize_ercot_load(load_df)
    merged = normalized.merge(normalized_load, on="timestamp", how="left")
    merged["energy"] = pd.to_numeric(merged["energy"], errors="coerce").fillna(
        merged["lmp"] - pd.to_numeric(merged["congestion"], errors="coerce").fillna(0) - pd.to_numeric(merged["loss"], errors="coerce").fillna(0)
    )
    merged["congestion"] = pd.to_numeric(merged["congestion"], errors="coerce").fillna(0.0)
    merged["loss"] = pd.to_numeric(merged["loss"], errors="coerce").fillna(0.0)
    return merged, raw_paths
