from __future__ import annotations

import pandas as pd

from harmonize import harmonize_market_dataframes


def test_harmonize_market_dataframes_combines_markets() -> None:
    pjm = pd.DataFrame(
        {
            "timestamp": ["2025-07-01T00:00:00Z"],
            "market": ["PJM"],
            "node_id": ["P1"],
            "node_name": ["PJM Node"],
            "lmp": [35.0],
            "energy": [28.0],
            "congestion": [5.0],
            "loss": [2.0],
        }
    )
    ercot = pd.DataFrame(
        {
            "timestamp": ["2025-07-01T00:00:00Z"],
            "market": ["ERCOT"],
            "node_id": ["E1"],
            "node_name": ["ERCOT Node"],
            "lmp": [25.0],
            "energy": [20.0],
            "congestion": [3.0],
            "loss": [2.0],
        }
    )

    result = harmonize_market_dataframes([pjm, ercot])

    assert len(result) == 2
    assert set(result["market"]) == {"PJM", "ERCOT"}

