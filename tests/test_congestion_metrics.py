from __future__ import annotations

import pandas as pd

from congestion_metrics import MetricConfig, run_analysis


def test_run_analysis_creates_market_outputs(tmp_path) -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-07-01T00:00:00Z",
                    "2025-07-01T00:00:00Z",
                    "2025-07-01T01:00:00Z",
                    "2025-07-01T01:00:00Z",
                ],
                utc=True,
            ),
            "market": ["PJM", "PJM", "ERCOT", "ERCOT"],
            "node_id": ["P1", "P2", "E1", "E2"],
            "node_name": ["PJM A", "PJM B", "ERCOT A", "ERCOT B"],
            "lmp": [30.0, 42.0, 20.0, 31.0],
            "energy": [20.0, 20.0, 16.0, 16.0],
            "congestion": [8.0, 20.0, 2.0, 13.0],
            "loss": [2.0, 2.0, 2.0, 2.0],
            "renewable_mw": [100.0, 100.0, 80.0, 80.0],
            "load_mw": [200.0, 200.0, 160.0, 160.0],
        }
    )

    results = run_analysis(df, tmp_path, MetricConfig(stress_event_quantile=0.5, top_nodes_per_market=2))

    assert not results["market_summary"].empty
    assert (tmp_path / "market_summary.csv").exists()
    assert (tmp_path / "report.md").exists()

