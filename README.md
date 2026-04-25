# Vina Subbasin Flow & Stage Dashboard

Hourly-refreshing dashboard of CDEC instantaneous flow (cfs) and river stage (ft) for 11 gauging stations relevant to the Vina Subbasin.

**Live:** https://cosmo1007.github.io/vina-flow-dashboard/
**Refresh on demand:** [Run the workflow on GitHub Actions](https://github.com/cosmo1007/vina-flow-dashboard/actions/workflows/refresh.yml).

## Stations

| Code | Name | River / Drainage | Sensors |
|------|------|------------------|---------|
| MUC | Mud Creek nr Chico | Mud Creek | flow, stage |
| MDV | Mud Creek Diversion at Chico | Mud Creek | stage only (often seasonal) |
| BIC | Big Chico Creek nr Chico | Big Chico Creek | flow, stage |
| BMA | Big Chico Creek at Manzanita Ave | Big Chico Creek | stage only (often seasonal) |
| LTA | Little Chico Ck at Taffee Ave | Little Chico Creek | stage only |
| BCK | Butte Creek nr Chico (USGS) | Butte Creek | flow, stage |
| BPD | Parrott Diversion from Butte Creek | Butte Creek | flow, stage |
| BCD | Butte Creek nr Durham | Butte Creek | flow, stage |
| BDH | Butte Ck nr Durham blw Gorrill Dam | Butte Creek | stage only |
| CNR | Cherokee Canal abv Nelson Rd | Cherokee Canal | stage only |
| CWC | Cottonwood Ck blw Hwy 99 | Cottonwood Creek | stage only |

Sensor selection per station was chosen by an empirical discovery pass against the CDEC `CSVDataServlet` endpoint — see `STATIONS` in `refresh_dashboard.py`. The discovery picks the highest-cadence (sensor, dur_code) combination that returns data, preferring hourly (`H`) over event (`E`) when both are available.

## Architecture

- **`refresh_dashboard.py`** — Fetches the trailing 120 days of flow + stage from CDEC, applies a conservative spike filter, and rewrites `index.html` in place by replacing the `const DATA = {...};` JSON blob.
- **`raw_cache/`** — Per-station, per-metric, per-month JSON files (e.g. `BIC_flow_2026-04.json`). Months that are fully complete and past the day-5 grace window are read from cache and never re-fetched. The current month is always re-fetched, and the prior month is re-fetched during the first 5 days of a new month to catch late-arriving CDEC values.
- **`index.html`** — Single-file dashboard. Chart.js for charts, Leaflet for the map. The dashboard JSON is embedded directly in the HTML; no fetch on page load.
- **`.github/workflows/refresh.yml`** — Hourly cron (`5 * * * *`) plus `workflow_dispatch` for the in-page "Refresh now" button. Commits and pushes when data has changed; otherwise no-ops.

## Time windows

Default view is the trailing 7 days. Toggle bar offers 24h / 7d / 30d / 90d / All. The cache always stores 120 days; the front end slices client-side.

## Thresholds

Each station has a `thresholds` block (`action_stage`, `monitor_stage`, `flood_stage`, `min_bypass_flow`) in `STATIONS`. All values are currently `null` — when populated with a citable source (NWS AHPS, water-right order, GSP appendix), the dashboard renders dashed reference lines on the matching axis.

## Running locally

```bash
pip install -r requirements.txt
python3 refresh_dashboard.py
open index.html
```
