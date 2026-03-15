"""Export helpers for bundling tables for downstream reporting."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def export_table_bundle(tables: dict[str, pd.DataFrame], output_dir: str | Path) -> list[Path]:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    exported: list[Path] = []
    for name, dataframe in tables.items():
        path = output_root / f"{name}.csv"
        dataframe.to_csv(path, index=False)
        exported.append(path)
    return exported

