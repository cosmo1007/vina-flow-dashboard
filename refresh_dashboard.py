#!/usr/bin/env python3
"""
Vina Subbasin Flow & Stage Dashboard - Refresh Script
=====================================================

Hourly refresh of CDEC flow (cfs) and river stage (ft) for the 11 gauging
stations relevant to the Vina Subbasin. Uses a monthly cache-freeze pattern:
prior months that are fully complete (and we're past day 5 of the following
month) are read from cache and never re-fetched. The current month is always
re-fetched. The prior month is also re-fetched during the first 5 days of a
new month to catch late-arriving CDEC values.

Output: rebuilds index.html in place by replacing the `const DATA = {...};`
JSON blob with fresh trailing-120-day data.

Usage:
    python3 refresh_dashboard.py
"""
import csv, json, os, re, sys
from datetime import datetime, timedelta, date, timezone
from io import StringIO

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library not installed. Run: pip install requests")
    sys.exit(1)

# -------- Config --------
HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(HERE, 'raw_cache')
DASHBOARD_FILE = os.path.join(HERE, 'index.html')

# How many days of history to retain in the dashboard JSON.
HISTORY_DAYS = 120

# How many days into a new month we still re-fetch the prior month's data
# (catches CDEC values that arrive late).
PRIOR_MONTH_REFETCH_WINDOW_DAYS = 5

# Per-station configuration. Sensor/dur values came from the discovery pass
# against CDEC's CSVDataServlet — see README for methodology.
# `flow` is None when the station has no published flow sensor.
STATIONS = {
    'MUC': {
        'name': 'Mud Creek nr Chico',
        'lat': 39.783356, 'lon': -121.886719, 'elev_ft': 170,
        'river': 'Sacramento River', 'operator': 'DWR North Region',
        'stage': {'sensor': '1', 'dur': 'E'},
        'flow':  {'sensor': '20', 'dur': 'E'},
        'thresholds': {},
    },
    'BIC': {
        'name': 'Big Chico Creek nr Chico',
        'lat': 39.768417, 'lon': -121.778603, 'elev_ft': 274,
        'river': 'Big Chico Creek', 'operator': 'DWR North Region',
        'stage': {'sensor': '1', 'dur': 'H'},
        'flow':  {'sensor': '20', 'dur': 'H'},
        'thresholds': {},
    },
    'MDV': {
        'name': 'Mud Creek Diversion at Chico',
        'lat': 39.762106, 'lon': -121.797419, 'elev_ft': 270,
        'river': 'Mud Creek', 'operator': 'Davids Engineering',
        'stage': {'sensor': '1', 'dur': 'E'},
        'flow':  None,
        'thresholds': {},
    },
    'BMA': {
        'name': 'Big Chico Creek at Manzanita Ave',
        'lat': 39.758250, 'lon': -121.795634, 'elev_ft': 286,
        'river': 'Big Chico Creek', 'operator': 'Davids Engineering',
        'stage': {'sensor': '1', 'dur': 'E'},
        'flow':  None,
        'thresholds': {},
    },
    'BCK': {
        'name': 'Butte Creek nr Chico',
        'lat': 39.725994, 'lon': -121.708862, 'elev_ft': 300,
        'river': 'Butte Creek', 'operator': 'USGS',
        'stage': {'sensor': '1', 'dur': 'H'},
        'flow':  {'sensor': '20', 'dur': 'H'},
        'thresholds': {},
    },
    'BPD': {
        'name': 'Parrott Diversion from Butte Creek',
        'lat': 39.708904, 'lon': -121.754189, 'elev_ft': 269,
        'river': 'Butte Creek', 'operator': 'DWR North Region',
        'stage': {'sensor': '1', 'dur': 'H'},
        'flow':  {'sensor': '20', 'dur': 'H'},
        'thresholds': {},
    },
    'BCD': {
        'name': 'Butte Creek nr Durham',
        'lat': 39.678013, 'lon': -121.777481, 'elev_ft': 190,
        'river': 'Butte Creek', 'operator': 'DWR North Region',
        'stage': {'sensor': '1', 'dur': 'H'},
        'flow':  {'sensor': '20', 'dur': 'H'},
        'thresholds': {},
    },
    'LTA': {
        'name': 'Little Chico Ck at Taffee Ave',
        'lat': 39.697050, 'lon': -121.893480, 'elev_ft': 145,
        'river': 'Little Chico Creek', 'operator': 'DWR North Region',
        'stage': {'sensor': '1', 'dur': 'H'},
        'flow':  None,
        'thresholds': {},
    },
    'BDH': {
        'name': 'Butte Ck nr Durham blw Gorrill Dam',
        'lat': 39.602169, 'lon': -121.785075, 'elev_ft': 133,
        'river': 'Butte Creek', 'operator': 'DWR North Region',
        'stage': {'sensor': '1', 'dur': 'E'},
        'flow':  None,
        'thresholds': {},
    },
    'CNR': {
        'name': 'Cherokee Canal abv Nelson Rd',
        'lat': 39.597539, 'lon': -121.702360, 'elev_ft': 170,
        'river': 'Cherokee Canal', 'operator': 'DWR North Region',
        'stage': {'sensor': '1', 'dur': 'E'},
        'flow':  None,
        'thresholds': {},
    },
    'CWC': {
        'name': 'Cottonwood Ck blw Hwy 99',
        'lat': 39.526944, 'lon': -121.689166, 'elev_ft': 125,
        'river': 'Cottonwood Creek', 'operator': 'DWR North Region',
        'stage': {'sensor': '1', 'dur': 'E'},
        'flow':  None,
        'thresholds': {},
    },
}

# -------- CDEC fetch --------
API = 'https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet'

def fetch(station, sensor, dur, start, end):
    """Fetch raw CSV rows from CDEC. Returns list of [timestamp_str, float]."""
    r = requests.get(API, params={'Stations': station, 'SensorNums': sensor,
                                   'dur_code': dur,
                                   'Start': start, 'End': end},
                     timeout=60)
    r.raise_for_status()
    rows = []
    for row in csv.DictReader(StringIO(r.text)):
        try:
            v = (row.get('VALUE') or '').strip()
            if v in ('', '---', '-9999', '-9998'):
                continue
            val = float(v)
            ts = (row.get('DATE TIME') or '').strip()
            if not ts:
                continue
            rows.append([ts, val])
        except Exception:
            continue
    return rows

# -------- Month helpers --------
def month_first(d):
    return date(d.year, d.month, 1)

def month_next(d):
    """Return first day of the month after d."""
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)

def months_in_window(start, end):
    """List of (year, month) tuples covering [start, end] inclusive."""
    months = []
    cur = month_first(start)
    last = month_first(end)
    while cur <= last:
        months.append((cur.year, cur.month))
        cur = month_next(cur)
    return months

def is_frozen(year, month, today):
    """A month is 'frozen' once we're past day 5 of the following month."""
    refetch_until = month_next(date(year, month, 1)) + timedelta(days=PRIOR_MONTH_REFETCH_WINDOW_DAYS)
    return today >= refetch_until

# -------- Cache I/O --------
def cache_path(code, metric, year, month):
    return os.path.join(CACHE_DIR, f'{code}_{metric}_{year}-{month:02d}.json')

def fetch_or_cache(code, metric, year, month, sensor_cfg, today):
    """Return (rows, source) for one (station, metric, month). source ∈ {cached, fetched}."""
    path = cache_path(code, metric, year, month)
    if is_frozen(year, month, today) and os.path.exists(path):
        try:
            return json.load(open(path))['rows'], 'cached'
        except Exception:
            pass
    # Fetch fresh
    start_d = date(year, month, 1)
    end_d = month_next(start_d) - timedelta(days=1)
    if end_d > today:
        end_d = today
    rows = fetch(code, sensor_cfg['sensor'], sensor_cfg['dur'],
                 start_d.isoformat(), end_d.isoformat())
    with open(path, 'w') as f:
        json.dump({'rows': rows, 'code': code, 'metric': metric,
                   'year_month': f'{year}-{month:02d}'}, f)
    return rows, 'fetched'

# -------- Spike filter --------
def clean_spikes(parsed):
    """
    Drop a single-point reading that is wildly inconsistent with both
    immediate neighbors when the neighbors themselves agree. Conservative:
    only fires for clear sensor glitches, not real storm spikes (since real
    spikes show on multiple consecutive readings).
    """
    if len(parsed) < 3:
        return list(parsed)
    cleaned = [parsed[0]]
    for i in range(1, len(parsed) - 1):
        ts, v = parsed[i]
        pv = parsed[i - 1][1]
        nv = parsed[i + 1][1]
        # Drop obvious negatives if neighbors are positive
        if v < 0 and pv >= 0 and nv >= 0:
            continue
        # Are neighbors in agreement?
        neighbor_avg = (pv + nv) / 2.0
        neighbor_span = abs(pv - nv)
        agree_tol = max(0.5, 0.3 * abs(neighbor_avg))
        if neighbor_span <= agree_tol:
            # Neighbors agree; flag v if it's far from them
            dev = abs(v - neighbor_avg)
            if dev > 5 * max(agree_tol, 1.0):
                continue
        cleaned.append(parsed[i])
    cleaned.append(parsed[-1])
    return cleaned

# -------- Build per-station data --------
def parse_ts(s):
    """CDEC format is 'YYYYMMDD HHMM'."""
    try:
        return datetime.strptime(s, '%Y%m%d %H%M')
    except Exception:
        return None

def build_metric_series(code, metric, sensor_cfg, today, start_window):
    """Fetch all months in window, concatenate, dedupe, spike-filter, trim to window.
    Returns (series, n_fetched, n_cached) where series is list of [iso_ts, value]."""
    months = months_in_window(start_window, today)
    n_fetched = n_cached = 0
    raw = []
    for (y, m) in months:
        rows, source = fetch_or_cache(code, metric, y, m, sensor_cfg, today)
        if source == 'cached':
            n_cached += 1
        else:
            n_fetched += 1
        raw.extend(rows)
    # Convert to (datetime, float), dedupe by timestamp, sort, filter
    parsed = []
    seen = set()
    for ts_str, val in raw:
        dt = parse_ts(ts_str)
        if dt is None or ts_str in seen:
            continue
        seen.add(ts_str)
        parsed.append((dt, float(val)))
    parsed.sort()
    parsed = clean_spikes(parsed)
    # Trim to [start_window, now]
    cutoff = datetime.combine(start_window, datetime.min.time())
    parsed = [(dt, v) for dt, v in parsed if dt >= cutoff]
    # Downsample older data to keep the embedded JSON small. Points within the
    # last 7 days keep full fidelity; older points are thinned to ~1 per hour.
    parsed = downsample_older(parsed, full_fidelity_days=7)
    series = [[dt.strftime('%Y-%m-%dT%H:%M'), round(v, 3)] for dt, v in parsed]
    return series, n_fetched, n_cached

def downsample_older(parsed, full_fidelity_days=7):
    """Keep every point within full_fidelity_days; older points: keep first
    point seen in each (date, hour) bucket."""
    if not parsed:
        return parsed
    cutoff = datetime.now() - timedelta(days=full_fidelity_days)
    recent = [(dt, v) for dt, v in parsed if dt >= cutoff]
    older = [(dt, v) for dt, v in parsed if dt < cutoff]
    seen_buckets = set()
    older_thinned = []
    for dt, v in older:
        bucket = (dt.year, dt.month, dt.day, dt.hour)
        if bucket in seen_buckets:
            continue
        seen_buckets.add(bucket)
        older_thinned.append((dt, v))
    return older_thinned + recent

# -------- Main build --------
def build_data():
    today = date.today()
    start_window = today - timedelta(days=HISTORY_DAYS)
    out = {
        'generated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'history_days': HISTORY_DAYS,
        'stations': {},
    }
    total_fetched = total_cached = 0
    for code, cfg in STATIONS.items():
        print(f'\n{code} ({cfg["name"]}):')
        station_out = {
            'meta': {
                'name': cfg['name'],
                'lat': cfg['lat'], 'lon': cfg['lon'],
                'elev_ft': cfg['elev_ft'],
                'river': cfg['river'],
                'operator': cfg['operator'],
                'has_stage': cfg.get('stage') is not None,
                'has_flow':  cfg.get('flow')  is not None,
                'thresholds': cfg.get('thresholds', {}),
            },
            'stage': [],
            'flow':  [],
            'current': {'stage': None, 'stage_at': None,
                        'flow':  None, 'flow_at':  None},
        }
        for metric in ('stage', 'flow'):
            scfg = cfg.get(metric)
            if scfg is None:
                print(f'  {metric:5s}: (no sensor)')
                continue
            try:
                series, nf, nc = build_metric_series(code, metric, scfg, today, start_window)
            except requests.HTTPError as e:
                print(f'  {metric:5s}: HTTP error {e}')
                continue
            except Exception as e:
                print(f'  {metric:5s}: ERROR {e}')
                continue
            total_fetched += nf
            total_cached += nc
            station_out[metric] = series
            if series:
                last_ts, last_val = series[-1]
                station_out['current'][metric] = last_val
                station_out['current'][f'{metric}_at'] = last_ts
                print(f'  {metric:5s}: {len(series):5d} pts  fetched={nf} cached={nc}  last={last_ts} val={last_val}')
            else:
                print(f'  {metric:5s}: (no data in window)  fetched={nf} cached={nc}')
        out['stations'][code] = station_out
    print(f'\nSummary: {total_fetched} fetched, {total_cached} cached')
    return out

def update_html(data):
    if not os.path.exists(DASHBOARD_FILE):
        print(f'NOTE: {DASHBOARD_FILE} not found. Skipping HTML update.')
        # Still drop a JSON file so the build is verifiable
        with open(os.path.join(HERE, 'data.json'), 'w') as f:
            json.dump(data, f)
        print(f'Wrote data.json instead.')
        return
    html = open(DASHBOARD_FILE).read()
    data_json = json.dumps(data, separators=(',', ':'))
    new_html, n = re.subn(r'const DATA = \{.*?\};\s*/\*END_DATA\*/',
                          f'const DATA = {data_json}; /*END_DATA*/',
                          html, count=1, flags=re.DOTALL)
    if n != 1:
        print('ERROR: could not find DATA placeholder in HTML.')
        sys.exit(1)
    with open(DASHBOARD_FILE, 'w') as f:
        f.write(new_html)
    print(f'\nUpdated {DASHBOARD_FILE}')

if __name__ == '__main__':
    print(f'Vina Flow & Stage Dashboard — Refresh')
    print(f'Date: {date.today()}  Cache: {CACHE_DIR}')
    os.makedirs(CACHE_DIR, exist_ok=True)
    data = build_data()
    update_html(data)
    print('\nDone.')
