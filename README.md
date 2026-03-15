# us-grid-congestion-monitor

This repository analyzes grid congestion patterns in PJM and ERCOT using nodal electricity price data. The goal is to understand how rapid renewable expansion interacts with transmission constraints and what those patterns may imply for grid investment, congestion management, and modernization.

`gridstatus` is the core data-ingestion layer for live market pulls. The project uses it to access a unified API across PJM and ERCOT for LMP, load, and interconnection queue data, while keeping market-specific cleaning and cross-market harmonization inside this repo.

## Core research questions

This project is organized around four questions:

1. Which PJM and ERCOT regions show persistent price separation?
2. To what extent can observed nodal price spread be interpreted as a congestion signal?
3. Are abnormal price spread and negative prices more common during high-renewable or low-net-load intervals?
4. What do these patterns imply for transmission investment, storage deployment, and grid modernization?

## Workflow

The pipeline is organized around these stages:

1. Download or stage raw market data into `data/raw/pjm` and `data/raw/ercot`.
2. Clean each market into a shared nodal schema.
3. Harmonize the combined dataset and engineer congestion features.
4. Quantify persistent price separation and congestion-linked spreads.
5. Test whether high-renewable and low-net-load intervals coincide with abnormal spreads or negative prices.
6. Export summaries, charts, map-ready tables, Excel-ready tables, and a narrative report.

## Expected harmonized schema

The core analysis expects these columns:

| column | description |
| --- | --- |
| `timestamp` | interval timestamp |
| `market` | `PJM` or `ERCOT` |
| `node_id` | unique node identifier |
| `node_name` | readable node name |
| `lmp` | nodal locational marginal price |
| `energy` | energy component |
| `congestion` | congestion component |
| `loss` | loss component |

Optional columns:

| column | description |
| --- | --- |
| `renewable_mw` | renewable generation for the interval |
| `load_mw` | system load for the interval |
| `zone` | pricing zone or region |
| `latitude` | map latitude |
| `longitude` | map longitude |

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=src python run_pipeline.py --input examples/sample_nodal_prices.csv
```

To run against live ISO data with `gridstatus`:

```bash
export PJM_API_KEY=your_api_key
PYTHONPATH=src python run_pipeline.py \
  --download-live \
  --start 2025-07-01 \
  --end 2025-07-03 \
  --markets PJM,ERCOT \
  --pjm-market REAL_TIME_HOURLY
```

Outputs are written to:

- `data/processed/`
- `data/exports/`
- `reports/final_report.md`
- `maps/`

Raw `gridstatus` extracts are also written to:

- `data/raw/pjm/pjm_lmp_raw.csv`
- `data/raw/pjm/pjm_load_raw.csv`
- `data/raw/pjm/pjm_interconnection_queue.csv`
- `data/raw/ercot/ercot_lmp_raw.csv`
- `data/raw/ercot/ercot_load_raw.csv`
- `data/raw/ercot/ercot_interconnection_queue.csv`

## Analytical framing

### 1. Persistent regional price separation

We use node-level and interval-level LMP comparisons to identify where price divergence is sustained rather than episodic. The key outputs are:

- top congested nodes by market
- price separation distributions by interval
- market and zone summaries for repeated spread behavior

### 2. Price spread as congestion signal

The pipeline treats price spread as a candidate congestion proxy, not an automatic proof of congestion. It evaluates:

- congestion component magnitude relative to total LMP
- interval price separation
- coincidence between large spreads and elevated congestion charges

This framing is important because spread can also reflect losses, scarcity, uplift-like effects in source data, or local data artifacts.

### 3. High-renewable and low-net-load stress conditions

The project explicitly tests whether unusual pricing outcomes cluster in intervals with:

- high renewable penetration
- low net load
- elevated negative price frequency
- unusually wide nodal price separation

### 4. Investment implications

The outputs are designed to support interpretation for:

- transmission expansion and reconductoring priorities
- storage siting near recurrent bottlenecks
- flexible interconnection and congestion management strategy
- broader grid modernization needs in high-renewable regions

## Repository layout

```text
us-grid-congestion-monitor/
├── config/
├── data/
├── notebooks/
├── src/
├── maps/
├── figures/
├── reports/
├── slides/
├── tests/
└── run_pipeline.py
```

## Current status

The repo includes:

- working schema validation and harmonization
- congestion feature engineering and summary metrics
- a pipeline runner for sample or staged CSV inputs
- `gridstatus`-based live download support for PJM and ERCOT
- placeholder download, chart, map, and export modules for future market-specific expansion
- starter notebooks, tests, report templates, and slide outline

The current generated report already covers:

- persistent node-level congestion exposure
- renewable-regime comparisons
- stress-event detection

The next layer is to deepen zone-level persistence analysis and make the investment implications more explicit in the final write-up.

## Next steps

- harden the `gridstatus` download adapters against market-specific schema drift
- enrich nodes with market metadata and coordinates
- replace placeholder HTML and figure generation with production visuals
- add seasonal, weather, and curtailment overlays
- add low-net-load metrics and zone-level persistence scoring
- extend the report to cite the strongest congestion-investment linkages automatically
