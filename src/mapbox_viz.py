"""Interactive congestion map exports using Plotly and Mapbox-compatible basemaps."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

MARKET_CENTERS = {
    "PJM": {"lat": 39.9, "lon": -78.7, "zoom": 4.8},
    "ERCOT": {"lat": 31.0, "lon": -99.3, "zoom": 4.6},
}

COLOR_SCALE = [
    [0.0, "#f3d9b1"],
    [0.35, "#f28f3b"],
    [0.7, "#c8553d"],
    [1.0, "#5c1a1b"],
]


def build_map_dataset(df: pd.DataFrame, market: str) -> pd.DataFrame:
    market_df = df.loc[df["market"] == market].copy()
    if market_df.empty:
        return pd.DataFrame()
    if not {"latitude", "longitude"}.issubset(market_df.columns):
        return pd.DataFrame()

    map_df = (
        market_df.groupby(
            ["market", "node_id", "node_name", "zone", "latitude", "longitude"],
            dropna=False,
        )
        .agg(
            observations=("timestamp", "size"),
            avg_lmp=("lmp", "mean"),
            avg_congestion=("congestion", "mean"),
            avg_abs_congestion=("abs_congestion", "mean"),
            max_abs_congestion=("abs_congestion", "max"),
            avg_price_separation=("price_separation", "mean"),
            avg_node_to_hub_spread=("node_to_hub_spread", "mean"),
            avg_abs_node_to_hub_spread=("abs_node_to_hub_spread", "mean"),
            negative_price_frequency=("negative_price_flag", "mean"),
            hub_lmp_source=("hub_lmp_source", "first"),
        )
        .reset_index()
    )
    market_mean = map_df["avg_abs_congestion"].mean()
    market_std = map_df["avg_abs_congestion"].std(ddof=0)
    scale = market_std if pd.notna(market_std) and market_std > 1e-9 else 1.0
    map_df["persistent_congestion_score"] = (
        map_df["avg_abs_congestion"] * 0.5
        + map_df["max_abs_congestion"] * 0.3
        + map_df["avg_abs_node_to_hub_spread"] * 0.2
    )
    map_df["congestion_zscore"] = (map_df["avg_abs_congestion"] - market_mean) / scale
    map_df["map_size"] = map_df["persistent_congestion_score"].clip(lower=1.0) * 2.2 + 10
    map_df["hover_label"] = map_df["node_name"] + " (" + map_df["node_id"] + ")"
    return map_df.sort_values("persistent_congestion_score", ascending=False)


def build_animation_dataset(df: pd.DataFrame, market: str) -> pd.DataFrame:
    market_df = df.loc[df["market"] == market].copy()
    if market_df.empty:
        return pd.DataFrame()
    if not {"latitude", "longitude"}.issubset(market_df.columns):
        return pd.DataFrame()

    animation_df = (
        market_df.groupby(
            ["market", "timestamp", "node_id", "node_name", "zone", "latitude", "longitude"],
            dropna=False,
        )
        .agg(
            lmp=("lmp", "mean"),
            congestion=("congestion", "mean"),
            abs_congestion=("abs_congestion", "mean"),
            price_separation=("price_separation", "mean"),
            node_to_hub_spread=("node_to_hub_spread", "mean"),
            abs_node_to_hub_spread=("abs_node_to_hub_spread", "mean"),
            negative_price_flag=("negative_price_flag", "mean"),
        )
        .reset_index()
    )
    animation_df["frame_label"] = animation_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M UTC")
    animation_df["map_size"] = animation_df["abs_node_to_hub_spread"].clip(lower=0.5) * 3.0 + 10
    return animation_df.sort_values(["timestamp", "abs_node_to_hub_spread"], ascending=[True, False])


def _build_missing_coordinates_html(market: str, output_path: Path) -> Path:
    html = f"""<!doctype html>
<html>
<head><meta charset="utf-8"><title>{market} congestion map</title></head>
<body style="font-family: Georgia, serif; max-width: 720px; margin: 48px auto; line-height: 1.5;">
<h1>{market} congestion map</h1>
<p>No latitude/longitude columns were available, so an interactive map could not be rendered.</p>
<p>Add <code>latitude</code> and <code>longitude</code> to the harmonized dataset or merge node reference metadata before map generation.</p>
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")
    return output_path


def _build_missing_plotly_html(title: str, output_path: Path) -> Path:
    output_path.write_text(
        (
            "<!doctype html><html><head><meta charset=\"utf-8\"><title>"
            f"{title}</title></head><body style=\"font-family: Georgia, serif; max-width: 760px; margin: 48px auto; line-height: 1.5;\">"
            f"<h1>{title}</h1><p><code>plotly</code> is not installed in the current environment, so the interactive map was not rendered.</p>"
            "<p>Install dependencies with <code>pip install -r requirements.txt</code> to generate the full Plotly + Mapbox HTML output.</p>"
            "</body></html>"
        ),
        encoding="utf-8",
    )
    return output_path


def _get_map_style_and_center(df: pd.DataFrame, market: str) -> tuple[str, dict[str, float]]:
    token = os.getenv("MAPBOX_ACCESS_TOKEN")
    center = MARKET_CENTERS.get(
        market,
        {"lat": float(df["latitude"].mean()), "lon": float(df["longitude"].mean()), "zoom": 4.5},
    )
    return ("carto-positron" if token else "open-street-map"), center


def write_market_map_html(df: pd.DataFrame, market: str, output_path: str | Path) -> Path:
    output = Path(output_path)
    map_df = build_map_dataset(df, market)
    if map_df.empty:
        return _build_missing_coordinates_html(market, output)

    try:
        import plotly.graph_objects as go
    except ImportError:
        return _build_missing_plotly_html(f"{market} congestion map", output)

    map_style, center = _get_map_style_and_center(map_df, market)

    fig = go.Figure()
    fig.add_trace(
        go.Scattermapbox(
            lat=map_df["latitude"],
            lon=map_df["longitude"],
            mode="markers",
            marker={
                "size": map_df["map_size"],
                "color": map_df["avg_abs_congestion"],
                "colorscale": COLOR_SCALE,
                "showscale": True,
                "opacity": 0.86,
                "sizemode": "diameter",
                "colorbar": {"title": "Avg abs<br>congestion", "bgcolor": "#f7f1e5"},
            },
            text=map_df["hover_label"],
            customdata=map_df[
                [
                    "zone",
                    "persistent_congestion_score",
                    "avg_abs_congestion",
                    "avg_abs_node_to_hub_spread",
                    "avg_price_separation",
                    "negative_price_frequency",
                    "hub_lmp_source",
                ]
            ].to_numpy(),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Zone: %{customdata[0]}<br>"
                "Persistent score: %{customdata[1]:.2f}<br>"
                "Avg abs congestion: %{customdata[2]:.2f}<br>"
                "Avg abs node-to-hub spread: %{customdata[3]:.2f}<br>"
                "Avg price separation: %{customdata[4]:.2f}<br>"
                "Negative price freq: %{customdata[5]:.1%}<br>"
                "Hub reference: %{customdata[6]}<br>"
                "<extra></extra>"
            ),
            name="Nodes",
        )
    )
    fig.add_trace(
        go.Scattermapbox(
            lat=map_df["latitude"],
            lon=map_df["longitude"],
            mode="text",
            text=map_df["node_name"],
            textposition="top center",
            hoverinfo="skip",
            showlegend=False,
        )
    )
    fig.update_layout(
        mapbox={"style": map_style, "center": {"lat": center["lat"], "lon": center["lon"]}, "zoom": center["zoom"]},
        margin={"l": 20, "r": 20, "t": 74, "b": 20},
        paper_bgcolor="#f7f1e5",
        plot_bgcolor="#f7f1e5",
        title={
            "text": f"{market} Congestion Map<br><sup>Bubble size reflects persistent congestion score; toggle between congestion and node-to-hub spread.</sup>",
            "x": 0.03,
            "xanchor": "left",
            "font": {"family": "Georgia, serif", "size": 24, "color": "#3a2214"},
        },
        font={"family": "Georgia, serif", "color": "#3a2214"},
        updatemenus=[
            {
                "type": "buttons",
                "direction": "right",
                "x": 0.03,
                "y": 1.02,
                "xanchor": "left",
                "yanchor": "bottom",
                "bgcolor": "#efe1c6",
                "bordercolor": "#c8a97e",
                "buttons": [
                    {
                        "label": "Congestion",
                        "method": "restyle",
                        "args": [
                            {
                                "marker.color": [map_df["avg_abs_congestion"]],
                                "marker.colorbar.title.text": ["Avg abs<br>congestion"],
                            },
                            [0],
                        ],
                    },
                    {
                        "label": "Node-to-Hub Spread",
                        "method": "restyle",
                        "args": [
                            {
                                "marker.color": [map_df["avg_abs_node_to_hub_spread"]],
                                "marker.colorbar.title.text": ["Avg abs<br>node-hub spread"],
                            },
                            [0],
                        ],
                    },
                ],
            }
        ],
    )
    fig.write_html(output, full_html=True, include_plotlyjs="cdn")
    return output


def write_spread_animation_html(df: pd.DataFrame, market: str, output_path: str | Path) -> Path:
    output = Path(output_path)
    animation_df = build_animation_dataset(df, market)
    if animation_df.empty:
        return _build_missing_coordinates_html(market, output)

    try:
        import plotly.express as px
    except ImportError:
        return _build_missing_plotly_html(f"{market} spread animation", output)

    token = os.getenv("MAPBOX_ACCESS_TOKEN")
    if token:
        px.set_mapbox_access_token(token)

    map_style, center = _get_map_style_and_center(animation_df, market)
    fig = px.scatter_mapbox(
        animation_df,
        lat="latitude",
        lon="longitude",
        color="abs_node_to_hub_spread",
        size="map_size",
        size_max=32,
        animation_frame="frame_label",
        hover_name="node_name",
        hover_data={
            "zone": True,
            "lmp": ":.2f",
            "congestion": ":.2f",
            "node_to_hub_spread": ":.2f",
            "price_separation": ":.2f",
            "negative_price_flag": ":.0f",
            "latitude": False,
            "longitude": False,
            "map_size": False,
        },
        zoom=center["zoom"],
        center={"lat": center["lat"], "lon": center["lon"]},
        color_continuous_scale=[
            [0.0, "#fee8c8"],
            [0.4, "#fdbb84"],
            [0.7, "#e34a33"],
            [1.0, "#7f0000"],
        ],
        title=f"{market} Node-to-Hub Spread Animation",
    )
    fig.update_traces(marker={"opacity": 0.86, "sizemode": "diameter"})
    fig.update_layout(
        mapbox={"style": map_style},
        margin={"l": 20, "r": 20, "t": 74, "b": 20},
        paper_bgcolor="#f7f1e5",
        plot_bgcolor="#f7f1e5",
        title={
            "text": f"{market} Node-to-Hub Spread Animation<br><sup>Bubble size and color scale with absolute node-to-hub spread over time.</sup>",
            "x": 0.03,
            "xanchor": "left",
            "font": {"family": "Georgia, serif", "size": 24, "color": "#3a2214"},
        },
        font={"family": "Georgia, serif", "color": "#3a2214"},
    )
    fig.write_html(output, full_html=True, include_plotlyjs="cdn")
    return output
