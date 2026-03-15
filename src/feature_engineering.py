"""Feature engineering for nodal congestion analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _compute_hub_reference_lmp(
    df: pd.DataFrame,
    hub_reference_map: dict[str, dict[str, object]] | None = None,
) -> tuple[pd.Series, pd.Series]:
    if hub_reference_map:
        configured_reference, configured_source = _compute_configured_hub_reference_lmp(
            df,
            hub_reference_map,
        )
        if configured_reference.notna().any():
            fallback_reference, fallback_source = _compute_heuristic_hub_reference_lmp(df)
            reference_lmp = configured_reference.fillna(fallback_reference)
            reference_source = configured_source.where(configured_reference.notna(), fallback_source)
            return reference_lmp, reference_source
    return _compute_heuristic_hub_reference_lmp(df)


def _compute_configured_hub_reference_lmp(
    df: pd.DataFrame,
    hub_reference_map: dict[str, dict[str, object]],
) -> tuple[pd.Series, pd.Series]:
    configured_reference = pd.Series(np.nan, index=df.index, dtype="float64")
    configured_source = pd.Series("configured_missing", index=df.index, dtype="object")

    for market, rules in hub_reference_map.items():
        market_mask = df["market"].astype(str).str.upper() == str(market).upper()
        if not market_mask.any():
            continue

        match_column = str(rules.get("match_column", "node_name"))
        if match_column not in df.columns:
            continue

        match_values = {str(value).strip().lower() for value in rules.get("match_values", []) if str(value).strip()}
        if not match_values:
            continue

        reference_mask = market_mask & df[match_column].fillna("").astype(str).str.lower().isin(match_values)
        if not reference_mask.any():
            continue

        market_reference = (
            df["lmp"]
            .where(reference_mask)
            .groupby([df["market"], df["timestamp"]])
            .transform("mean")
        )
        configured_reference = configured_reference.where(~market_mask, market_reference)
        configured_source = configured_source.where(
            ~market_mask,
            np.where(market_reference.notna(), f"configured:{match_column}", "configured_missing"),
        )

    return configured_reference, configured_source


def _compute_heuristic_hub_reference_lmp(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    lowered_node_names = df["node_name"].fillna("").astype(str).str.lower()
    if "zone" in df.columns:
        lowered_zones = df["zone"].fillna("").astype(str).str.lower()
    else:
        lowered_zones = pd.Series("", index=df.index)

    hub_mask = lowered_node_names.str.contains("hub") | lowered_zones.str.contains("hub")
    hub_reference = df["lmp"].where(hub_mask).groupby([df["market"], df["timestamp"]]).transform("mean")
    fallback_reference = df.groupby(["market", "timestamp"])["lmp"].transform("mean")
    reference_lmp = hub_reference.fillna(fallback_reference)
    reference_source = pd.Series(
        np.where(hub_reference.notna(), "named_hub", "market_mean"),
        index=df.index,
    )
    return reference_lmp, reference_source


def add_congestion_features(
    df: pd.DataFrame,
    hub_reference_map: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    engineered = df.copy()
    engineered["abs_congestion"] = engineered["congestion"].abs()
    engineered["congestion_share_of_lmp"] = np.where(
        engineered["lmp"].abs() > 1e-9,
        engineered["congestion"] / engineered["lmp"],
        np.nan,
    )
    engineered["price_separation"] = engineered.groupby(["market", "timestamp"])["lmp"].transform(
        lambda values: values.max() - values.min()
    )
    engineered["interval_mean_abs_congestion"] = engineered.groupby(["market", "timestamp"])[
        "abs_congestion"
    ].transform("mean")
    engineered["negative_price_flag"] = engineered["lmp"] < 0
    engineered["hub_lmp"], engineered["hub_lmp_source"] = _compute_hub_reference_lmp(
        engineered,
        hub_reference_map=hub_reference_map,
    )
    engineered["node_to_hub_spread"] = engineered["lmp"] - engineered["hub_lmp"]
    engineered["abs_node_to_hub_spread"] = engineered["node_to_hub_spread"].abs()

    if {"renewable_mw", "load_mw"}.issubset(engineered.columns):
        engineered["renewable_penetration"] = np.where(
            engineered["load_mw"] > 0,
            engineered["renewable_mw"] / engineered["load_mw"],
            np.nan,
        )
        engineered["net_load_mw"] = engineered["load_mw"] - engineered["renewable_mw"]

    engineered["hour_ending"] = engineered["timestamp"].dt.hour
    engineered["month"] = engineered["timestamp"].dt.month
    return engineered
