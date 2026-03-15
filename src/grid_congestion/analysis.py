"""Core analysis routines for PJM and ERCOT congestion patterns."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .schema import AnalysisConfig


def _prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared["abs_congestion"] = prepared["congestion"].abs()
    prepared["congestion_share_of_lmp"] = np.where(
        prepared["lmp"].abs() > 1e-9,
        prepared["congestion"] / prepared["lmp"],
        np.nan,
    )
    prepared["price_separation"] = prepared.groupby(["market", "timestamp"])["lmp"].transform(
        lambda values: values.max() - values.min()
    )
    prepared["interval_mean_abs_congestion"] = prepared.groupby(["market", "timestamp"])[
        "abs_congestion"
    ].transform("mean")

    if {"renewable_mw", "load_mw"}.issubset(prepared.columns):
        prepared["renewable_penetration"] = np.where(
            prepared["load_mw"] > 0,
            prepared["renewable_mw"] / prepared["load_mw"],
            np.nan,
        )

    return prepared


def _summarize_markets(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("market")
        .agg(
            intervals=("timestamp", "nunique"),
            nodes=("node_id", "nunique"),
            avg_lmp=("lmp", "mean"),
            avg_congestion=("congestion", "mean"),
            avg_abs_congestion=("abs_congestion", "mean"),
            median_price_separation=("price_separation", "median"),
            p95_price_separation=("price_separation", lambda values: values.quantile(0.95)),
        )
        .reset_index()
        .sort_values("avg_abs_congestion", ascending=False)
    )
    return summary


def _summarize_nodes(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    summary = (
        df.groupby(["market", "node_id", "node_name"], dropna=False)
        .agg(
            observations=("timestamp", "size"),
            avg_lmp=("lmp", "mean"),
            avg_congestion=("congestion", "mean"),
            avg_abs_congestion=("abs_congestion", "mean"),
            max_abs_congestion=("abs_congestion", "max"),
            avg_price_separation=("price_separation", "mean"),
        )
        .reset_index()
    )
    summary["market_rank"] = summary.groupby("market")["avg_abs_congestion"].rank(
        method="first", ascending=False
    )
    return summary.loc[summary["market_rank"] <= top_n].sort_values(
        ["market", "market_rank"]
    )


def _renewable_impact(df: pd.DataFrame) -> pd.DataFrame:
    if "renewable_penetration" not in df.columns:
        return pd.DataFrame()

    renewable_df = df.dropna(subset=["renewable_penetration"]).copy()
    if renewable_df.empty:
        return pd.DataFrame()

    renewable_df["renewable_regime"] = renewable_df.groupby("market")["renewable_penetration"].transform(
        lambda values: np.where(
            values >= values.quantile(0.75),
            "high_renewables",
            np.where(values <= values.quantile(0.25), "low_renewables", "mid_range"),
        )
    )

    summary = (
        renewable_df.loc[renewable_df["renewable_regime"] != "mid_range"]
        .groupby(["market", "renewable_regime"])
        .agg(
            avg_abs_congestion=("abs_congestion", "mean"),
            avg_price_separation=("price_separation", "mean"),
            avg_lmp=("lmp", "mean"),
            mean_renewable_penetration=("renewable_penetration", "mean"),
        )
        .reset_index()
    )

    return summary.sort_values(["market", "renewable_regime"])


def _stress_events(df: pd.DataFrame, quantile: float) -> pd.DataFrame:
    interval_summary = (
        df.groupby(["market", "timestamp"])
        .agg(
            mean_abs_congestion=("abs_congestion", "mean"),
            max_abs_congestion=("abs_congestion", "max"),
            price_separation=("price_separation", "max"),
            top_node=("node_name", lambda values: values.iloc[0]),
        )
        .reset_index()
    )

    thresholds = interval_summary.groupby("market")["mean_abs_congestion"].transform(
        lambda values: values.quantile(quantile)
    )
    stress_events = interval_summary.loc[
        interval_summary["mean_abs_congestion"] >= thresholds
    ].copy()
    return stress_events.sort_values(["market", "mean_abs_congestion"], ascending=[True, False])


def build_markdown_report(
    market_summary: pd.DataFrame,
    node_summary: pd.DataFrame,
    renewable_summary: pd.DataFrame,
    stress_events: pd.DataFrame,
) -> str:
    lines = [
        "# Grid Congestion Analysis Report",
        "",
        "## Market overview",
    ]

    for row in market_summary.itertuples(index=False):
        lines.append(
            (
                f"- {row.market}: average absolute congestion was {row.avg_abs_congestion:.2f}, "
                f"with median nodal price separation of {row.median_price_separation:.2f} "
                f"across {row.intervals} intervals."
            )
        )

    lines.extend(["", "## Most exposed nodes"])
    for market, market_nodes in node_summary.groupby("market"):
        lines.append(f"### {market}")
        for row in market_nodes.itertuples(index=False):
            lines.append(
                f"- {row.node_name} ({row.node_id}) ranked #{int(row.market_rank)} with "
                f"average absolute congestion of {row.avg_abs_congestion:.2f}."
            )

    if not renewable_summary.empty:
        lines.extend(["", "## Renewable interaction"])
        for market, market_rows in renewable_summary.groupby("market"):
            high = market_rows.loc[market_rows["renewable_regime"] == "high_renewables"]
            low = market_rows.loc[market_rows["renewable_regime"] == "low_renewables"]
            if not high.empty and not low.empty:
                high_row = high.iloc[0]
                low_row = low.iloc[0]
                delta = high_row["avg_abs_congestion"] - low_row["avg_abs_congestion"]
                direction = "higher" if delta >= 0 else "lower"
                lines.append(
                    f"- {market}: high-renewable intervals showed {abs(delta):.2f} {direction} "
                    "average absolute congestion than low-renewable intervals."
                )

    if not stress_events.empty:
        sample_events = stress_events.head(5)
        lines.extend(["", "## Stress events"])
        for row in sample_events.itertuples(index=False):
            lines.append(
                f"- {row.market} at {row.timestamp.isoformat()}: "
                f"mean absolute congestion reached {row.mean_abs_congestion:.2f} "
                f"with price separation of {row.price_separation:.2f}."
            )

    return "\n".join(lines) + "\n"


def run_congestion_analysis(
    df: pd.DataFrame,
    output_dir: str | Path,
    config: AnalysisConfig | None = None,
) -> dict[str, pd.DataFrame | str]:
    config = config or AnalysisConfig()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    prepared = _prepare_features(df)
    market_summary = _summarize_markets(prepared)
    node_summary = _summarize_nodes(prepared, top_n=config.top_nodes_per_market)
    renewable_summary = _renewable_impact(prepared)
    stress_events = _stress_events(prepared, quantile=config.stress_event_quantile)
    report = build_markdown_report(
        market_summary=market_summary,
        node_summary=node_summary,
        renewable_summary=renewable_summary,
        stress_events=stress_events,
    )

    market_summary.to_csv(output_path / "market_summary.csv", index=False)
    node_summary.to_csv(output_path / "node_summary.csv", index=False)
    stress_events.to_csv(output_path / "stress_events.csv", index=False)
    if not renewable_summary.empty:
        renewable_summary.to_csv(output_path / "renewable_impact.csv", index=False)
    (output_path / "report.md").write_text(report, encoding="utf-8")

    return {
        "prepared": prepared,
        "market_summary": market_summary,
        "node_summary": node_summary,
        "renewable_summary": renewable_summary,
        "stress_events": stress_events,
        "report": report,
    }
