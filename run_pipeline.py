"""Run the end-to-end congestion analysis pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from charts import build_hourly_profile, build_monthly_profile, save_placeholder_figure
from clean_ercot import clean_ercot
from clean_pjm import clean_pjm
from config.settings import load_market_config, load_settings
from congestion_metrics import MetricConfig, run_analysis
from download_ercot import download_ercot_dataset
from download_pjm import download_pjm_dataset
from export_excel_pack import export_table_bundle
from harmonize import harmonize_market_dataframes
from io_utils import ensure_dir, load_dataframe, load_csv, write_csv
from mapbox_viz import (
    build_animation_dataset,
    build_map_dataset,
    write_market_map_html,
    write_spread_animation_html,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the US grid congestion monitor pipeline.")
    parser.add_argument("--input", help="Path to a harmonized sample CSV or staged raw file with market rows.")
    parser.add_argument(
        "--download-live",
        action="store_true",
        help="Download PJM and/or ERCOT data using gridstatus instead of reading --input.",
    )
    parser.add_argument("--start", help="Inclusive start timestamp for gridstatus downloads, for example 2025-07-01.")
    parser.add_argument("--end", help="Exclusive end timestamp for gridstatus downloads, for example 2025-07-03.")
    parser.add_argument(
        "--markets",
        default="PJM,ERCOT",
        help="Comma-separated list of markets to download when using --download-live.",
    )
    parser.add_argument(
        "--pjm-market",
        default="REAL_TIME_HOURLY",
        help="PJM market argument passed to gridstatus get_lmp.",
    )
    return parser


def _load_source_data(args: argparse.Namespace, settings) -> pd.DataFrame:
    if args.download_live:
        if not args.start or not args.end:
            raise ValueError("--start and --end are required when using --download-live.")

        markets = {market.strip().upper() for market in args.markets.split(",") if market.strip()}
        frames: list[pd.DataFrame] = []

        if "PJM" in markets:
            pjm_df, _ = download_pjm_dataset(
                start=args.start,
                end=args.end,
                raw_dir=settings.raw_dir / "pjm",
                market=args.pjm_market,
            )
            frames.append(clean_pjm(pjm_df))

        if "ERCOT" in markets:
            ercot_df, _ = download_ercot_dataset(
                start=args.start,
                end=args.end,
                raw_dir=settings.raw_dir / "ercot",
            )
            frames.append(clean_ercot(ercot_df))

        if not frames:
            raise ValueError("No supported markets selected. Use --markets PJM,ERCOT.")
        return harmonize_market_dataframes(frames)

    if not args.input:
        raise ValueError("Provide --input for local files or use --download-live with --start/--end.")

    source = load_dataframe(args.input)
    pjm = clean_pjm(source.loc[source["market"].str.upper() == "PJM"].copy())
    ercot = clean_ercot(source.loc[source["market"].str.upper() == "ERCOT"].copy())
    return harmonize_market_dataframes([pjm, ercot])


def main() -> None:
    args = build_parser().parse_args()
    settings = load_settings()
    market_config = load_market_config().get("markets", {})
    hub_reference_map = {
        market: config["hub_reference"]
        for market, config in market_config.items()
        if isinstance(config, dict) and "hub_reference" in config
    }
    harmonized = _load_source_data(args, settings)

    ensure_dir(settings.processed_dir)
    ensure_dir(settings.exports_dir)
    ensure_dir(settings.maps_dir)
    ensure_dir(settings.figures_dir)

    harmonized_path = write_csv(harmonized, settings.processed_dir / "harmonized_nodal_prices.csv")
    results = run_analysis(
        harmonized,
        output_dir=settings.exports_dir,
        config=MetricConfig(
            stress_event_quantile=settings.stress_event_quantile,
            top_nodes_per_market=settings.top_nodes_per_market,
            hub_reference_map=hub_reference_map,
        ),
    )

    write_csv(build_hourly_profile(results["engineered"]), settings.exports_dir / "hourly_profile.csv")
    write_csv(build_monthly_profile(results["engineered"]), settings.exports_dir / "monthly_profile.csv")

    export_table_bundle(
        {
            "market_summary_pack": results["market_summary"],
            "node_summary_pack": results["node_summary"],
            "stress_events_pack": results["stress_events"],
            "pjm_map_layer": build_map_dataset(results["engineered"], "PJM"),
            "ercot_map_layer": build_map_dataset(results["engineered"], "ERCOT"),
            "pjm_animation_layer": build_animation_dataset(results["engineered"], "PJM"),
            "ercot_animation_layer": build_animation_dataset(results["engineered"], "ERCOT"),
        },
        settings.exports_dir,
    )

    write_market_map_html(results["engineered"], "PJM", settings.maps_dir / "pjm_congestion_map.html")
    write_market_map_html(results["engineered"], "ERCOT", settings.maps_dir / "ercot_congestion_map.html")
    write_spread_animation_html(results["engineered"], "PJM", settings.maps_dir / "spread_animation.html")

    figure_titles = {
        "fig01_market_overview.png": "Market overview",
        "fig02_hourly_load_profile.png": "Hourly load profile",
        "fig03_hub_vs_node_spread.png": "Hub versus node spread",
        "fig04_negative_price_frequency.png": "Negative price frequency",
        "fig05_congestion_by_hour.png": "Congestion by hour",
        "fig06_congestion_by_month.png": "Congestion by month",
        "fig07_renewables_vs_spread.png": "Renewables versus spread",
        "fig08_top_congested_nodes.png": "Top congested nodes",
        "fig09_pjm_map_snapshot.png": "PJM map snapshot",
        "fig10_ercot_map_snapshot.png": "ERCOT map snapshot",
    }
    for filename, title in figure_titles.items():
        save_placeholder_figure(settings.figures_dir / filename, title)

    report_path = settings.reports_dir / "final_report.md"
    report_path.write_text(results["report"], encoding="utf-8")

    print(f"Harmonized data written to {harmonized_path}")
    print(f"Exports written to {settings.exports_dir}")
    print(f"Report written to {report_path}")


if __name__ == "__main__":
    main()
