"""Central settings for the congestion monitor."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class AppSettings:
    project_root: Path
    raw_dir: Path
    processed_dir: Path
    exports_dir: Path
    reports_dir: Path
    maps_dir: Path
    figures_dir: Path
    stress_event_quantile: float = 0.95
    top_nodes_per_market: int = 10


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_market_config(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path) if path else get_project_root() / "config" / "markets.yml"
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_settings() -> AppSettings:
    root = get_project_root()
    return AppSettings(
        project_root=root,
        raw_dir=root / "data" / "raw",
        processed_dir=root / "data" / "processed",
        exports_dir=root / "data" / "exports",
        reports_dir=root / "reports",
        maps_dir=root / "maps",
        figures_dir=root / "figures",
    )

