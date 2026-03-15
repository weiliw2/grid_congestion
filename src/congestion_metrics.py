"""Summary metrics and report generation for congestion analysis."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from feature_engineering import add_congestion_features
from io_utils import ensure_dir, write_csv


@dataclass(frozen=True)
class MetricConfig:
    stress_event_quantile: float = 0.95
    top_nodes_per_market: int = 10


def summarize_markets(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("market")
        .agg(
            intervals=("timestamp", "nunique"),
            nodes=("node_id", "nunique"),
            avg_lmp=("lmp", "mean"),
            avg_congestion=("congestion", "mean"),
            avg_abs_congestion=("abs_congestion", "mean"),
            median_abs_node_to_hub_spread=("abs_node_to_hub_spread", "median"),
            negative_price_frequency=("negative_price_flag", "mean"),
            median_price_separation=("price_separation", "median"),
            p95_price_separation=("price_separation", lambda values: values.quantile(0.95)),
        )
        .reset_index()
        .sort_values("avg_abs_congestion", ascending=False)
    )


def summarize_nodes(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    summary = (
        df.groupby(["market", "node_id", "node_name"], dropna=False)
        .agg(
            observations=("timestamp", "size"),
            avg_lmp=("lmp", "mean"),
            avg_congestion=("congestion", "mean"),
            avg_abs_congestion=("abs_congestion", "mean"),
            max_abs_congestion=("abs_congestion", "max"),
            avg_price_separation=("price_separation", "mean"),
            avg_node_to_hub_spread=("node_to_hub_spread", "mean"),
            avg_abs_node_to_hub_spread=("abs_node_to_hub_spread", "mean"),
        )
        .reset_index()
    )
    summary["market_rank"] = summary.groupby("market")["avg_abs_congestion"].rank(
        method="first", ascending=False
    )
    return summary.loc[summary["market_rank"] <= top_n].sort_values(["market", "market_rank"])


def renewable_impact(df: pd.DataFrame) -> pd.DataFrame:
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

    return (
        renewable_df.loc[renewable_df["renewable_regime"] != "mid_range"]
        .groupby(["market", "renewable_regime"])
        .agg(
            avg_abs_congestion=("abs_congestion", "mean"),
            avg_price_separation=("price_separation", "mean"),
            avg_lmp=("lmp", "mean"),
            mean_renewable_penetration=("renewable_penetration", "mean"),
        )
        .reset_index()
        .sort_values(["market", "renewable_regime"])
    )


def stress_events(df: pd.DataFrame, quantile: float) -> pd.DataFrame:
    interval_summary = (
        df.groupby(["market", "timestamp"])
        .agg(
            mean_abs_congestion=("abs_congestion", "mean"),
            max_abs_congestion=("abs_congestion", "max"),
            price_separation=("price_separation", "max"),
        )
        .reset_index()
    )
    thresholds = interval_summary.groupby("market")["mean_abs_congestion"].transform(
        lambda values: values.quantile(quantile)
    )
    return interval_summary.loc[interval_summary["mean_abs_congestion"] >= thresholds].sort_values(
        ["market", "mean_abs_congestion"],
        ascending=[True, False],
    )


def build_report(
    market_summary: pd.DataFrame,
    node_summary: pd.DataFrame,
    renewable_summary: pd.DataFrame,
    stress_summary: pd.DataFrame,
) -> str:
    lines = ["# Final Report", "", "## Question 1: Which regions show persistent price separation?"]

    for row in market_summary.itertuples(index=False):
        lines.append(
            f"- {row.market}: median nodal price separation is {row.median_price_separation:.2f}, "
            f"median node-to-hub spread is {row.median_abs_node_to_hub_spread:.2f}, "
            f"and average absolute congestion is {row.avg_abs_congestion:.2f} across {row.intervals} intervals."
        )

    top_nodes = node_summary.groupby("market").head(1)
    for row in top_nodes.itertuples(index=False):
        lines.append(
            f"- {row.market}: {row.node_name} ({row.node_id}) is the top recurring congestion-exposed node "
            f"with average absolute congestion of {row.avg_abs_congestion:.2f}."
        )

    lines.extend(["", "## Question 2: How much of the spread looks like congestion?"])
    for row in market_summary.itertuples(index=False):
        lines.append(
            f"- {row.market}: average absolute congestion is {row.avg_abs_congestion:.2f} while median price "
            f"separation is {row.median_price_separation:.2f} and median node-to-hub spread is "
            f"{row.median_abs_node_to_hub_spread:.2f}, suggesting spread and congestion move together in the sample."
        )
    lines.append(
        "- Spread is treated here as a congestion signal candidate rather than a definitive measure, because losses and market design effects can also contribute."
    )

    lines.extend(["", "## Question 3: Do high-renewable or low-net-load periods show abnormal spreads or negative prices?"])
    if not renewable_summary.empty:
        for market in renewable_summary["market"].unique():
            market_rows = renewable_summary.loc[renewable_summary["market"] == market]
            high = market_rows.loc[market_rows["renewable_regime"] == "high_renewables"]
            low = market_rows.loc[market_rows["renewable_regime"] == "low_renewables"]
            if not high.empty and not low.empty:
                high_row = high.iloc[0]
                low_row = low.iloc[0]
                delta = float(high_row["avg_abs_congestion"] - low_row["avg_abs_congestion"])
                descriptor = "higher" if delta >= 0 else "lower"
                lines.append(
                    f"- {market}: high-renewable intervals show {abs(delta):.2f} {descriptor} average absolute "
                    "congestion than low-renewable intervals."
                )
    else:
        lines.append("- Renewable and net-load fields are not available in this dataset, so this question remains open.")

    for row in market_summary.itertuples(index=False):
        lines.append(
            f"- {row.market}: negative price frequency is {row.negative_price_frequency:.1%} in the current sample."
        )

    if not stress_summary.empty:
        lines.append("- Stress-event intervals remain useful candidates for deeper low-net-load and renewable interaction checks:")
        for row in stress_summary.head(5).itertuples(index=False):
            lines.append(
                f"- {row.market} {row.timestamp.isoformat()}: mean absolute congestion {row.mean_abs_congestion:.2f}, "
                f"price separation {row.price_separation:.2f}."
            )

    lines.extend(["", "## Question 4: What are the implications for transmission, storage, and modernization?"])
    lines.append(
        "- Persistent high-spread nodes are candidate locations for transmission reinforcement or operational bottleneck review."
    )
    lines.append(
        "- If future runs show repeated spread during renewable-rich intervals, those regions become stronger candidates for storage deployment."
    )
    lines.append(
        "- Sustained nodal divergence across time would support broader grid modernization investment where renewable growth is outrunning transfer capability."
    )

    return "\n".join(lines) + "\n"


def run_analysis(df: pd.DataFrame, output_dir: str | Path, config: MetricConfig | None = None) -> dict[str, pd.DataFrame | str]:
    config = config or MetricConfig()
    export_dir = ensure_dir(output_dir)

    engineered = add_congestion_features(df)
    market_summary = summarize_markets(engineered)
    node_summary = summarize_nodes(engineered, config.top_nodes_per_market)
    renewable_summary = renewable_impact(engineered)
    stress_summary = stress_events(engineered, config.stress_event_quantile)
    report = build_report(market_summary, node_summary, renewable_summary, stress_summary)

    write_csv(engineered, export_dir / "harmonized_features.csv")
    write_csv(market_summary, export_dir / "market_summary.csv")
    write_csv(node_summary, export_dir / "node_summary.csv")
    write_csv(stress_summary, export_dir / "stress_events.csv")
    if not renewable_summary.empty:
        write_csv(renewable_summary, export_dir / "renewable_impact.csv")
    (export_dir / "report.md").write_text(report, encoding="utf-8")

    return {
        "engineered": engineered,
        "market_summary": market_summary,
        "node_summary": node_summary,
        "renewable_summary": renewable_summary,
        "stress_events": stress_summary,
        "report": report,
    }
