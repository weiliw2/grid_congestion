"""CLI for nodal congestion analysis."""

from __future__ import annotations

import argparse

from .analysis import run_congestion_analysis
from .io import ensure_output_dir, load_nodal_prices
from .schema import AnalysisConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze congestion patterns in PJM and ERCOT nodal electricity price data."
    )
    parser.add_argument("--input", required=True, help="Path to the nodal price CSV file.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where CSV summaries and a Markdown report will be written.",
    )
    parser.add_argument(
        "--stress-event-quantile",
        type=float,
        default=0.95,
        help="Quantile threshold used to flag high-congestion stress intervals.",
    )
    parser.add_argument(
        "--top-nodes",
        type=int,
        default=10,
        help="Number of most congestion-exposed nodes to retain per market.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    df = load_nodal_prices(args.input)
    output_dir = ensure_output_dir(args.output_dir)
    config = AnalysisConfig(
        stress_event_quantile=args.stress_event_quantile,
        top_nodes_per_market=args.top_nodes,
    )
    run_congestion_analysis(df, output_dir=output_dir, config=config)


if __name__ == "__main__":
    main()
