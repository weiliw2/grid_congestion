from __future__ import annotations

import pandas as pd

from feature_engineering import add_congestion_features


def test_add_congestion_features_creates_expected_columns() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2025-07-01T00:00:00Z", "2025-07-01T00:00:00Z"], utc=True
            ),
            "market": ["PJM", "PJM"],
            "node_id": ["A", "B"],
            "node_name": ["A", "B"],
            "lmp": [30.0, 40.0],
            "energy": [20.0, 20.0],
            "congestion": [8.0, 18.0],
            "loss": [2.0, 2.0],
            "renewable_mw": [100.0, 100.0],
            "load_mw": [200.0, 200.0],
        }
    )

    result = add_congestion_features(df)

    assert "abs_congestion" in result.columns
    assert "price_separation" in result.columns
    assert "renewable_penetration" in result.columns
    assert "node_to_hub_spread" in result.columns
    assert "abs_node_to_hub_spread" in result.columns
    assert result["price_separation"].tolist() == [10.0, 10.0]
