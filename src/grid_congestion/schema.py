"""Schema definitions for nodal electricity price analysis."""

from __future__ import annotations

from dataclasses import dataclass

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

OPTIONAL_COLUMNS = {
    "renewable_mw",
    "load_mw",
    "zone",
}


@dataclass(frozen=True)
class AnalysisConfig:
    """Configuration for congestion event detection and renewable comparisons."""

    stress_event_quantile: float = 0.95
    top_nodes_per_market: int = 10
