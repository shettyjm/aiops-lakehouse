"""Vectorised synthetic telemetry generator for a 2,000-VM fleet (M1).

Design goals:
  * No per-row Python loops. Metrics live in dense numpy arrays shaped
    (n_vms, n_samples); baselines are filled per-app in vectorised slices and
    incident templates mutate row/column sub-blocks in place.
  * Deterministic given a seed (numpy default_rng).
  * Output matches the CLAUDE.md data contract exactly (wide vm_metrics rows,
    sparse app_events rows) so detectors and the copilot read it unchanged.

The public surface is:
    fleet   = load_fleet("fleet.yaml")
    scen    = load_scenarios("scenarios.yaml")
    result  = generate(fleet, scen, start=..., n_samples=..., seed=...)
    result.metrics  -> pandas.DataFrame (long, one row per vm x sample)
    result.events   -> pandas.DataFrame (app_events contract)

Writing to hive-partitioned parquet is handled by write_dataset(); the CLI in
bin/02_generate.py wires config, streaming and S3 upload on top.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# The nine numeric columns of the vm_metrics contract, in canonical order.
METRICS: tuple[str, ...] = (
    "cpu_pct", "heap_mb", "gc_pause_ms", "disk_io_await_ms", "disk_iops",
    "net_mbps", "net_retrans_pct", "app_latency_ms", "error_rate_pct",
)

# Metrics that must never go negative after noise/incidents.
_NONNEG = set(METRICS)

# Small absolute noise floors so near-zero baselines still jitter a little.
_NOISE_FLOOR = {
    "net_retrans_pct": 0.05,
    "error_rate_pct": 0.03,
    "gc_pause_ms": 1.0,
}

EVENT_COLUMNS = ["ts", "vm_id", "app", "site", "level", "event_type", "message"]
TOPOLOGY_COLUMNS = ["vm_id", "app", "site", "depends_on_app"]
SITES_COLUMNS = ["site", "location_type", "region"]


# --------------------------------------------------------------------------- #
# Spec loading
# --------------------------------------------------------------------------- #
def load_fleet(path: str | Path) -> dict:
    """Load and validate fleet.yaml."""
    fleet = yaml.safe_load(Path(path).read_text())
    if not fleet.get("apps"):
        raise ValueError("fleet.yaml has no 'apps'")
    for app, spec in fleet["apps"].items():
        if "count" not in spec or "baseline" not in spec:
            raise ValueError(f"app '{app}' needs 'count' and 'baseline'")
        missing = _NONNEG - set(spec["baseline"])
        if missing:
            raise ValueError(f"app '{app}' baseline missing metrics: {sorted(missing)}")
    fleet.setdefault("sites", ["dc-east", "dc-west"])
    fleet.setdefault("sample_interval_s", 30)
    fleet.setdefault("noise_pct", 0.08)
    return fleet


def scale_fleet(fleet_spec: dict, max_vms: int) -> int:
    """Proportionally shrink app counts to ~max_vms total (min 1 per app).

    Preserves each app's share of the fleet so incident targeting still works
    at reduced scale. Mutates fleet_spec in place; returns the resulting total.
    """
    apps = fleet_spec["apps"]
    total = sum(a["count"] for a in apps.values())
    if max_vms >= total:
        return total
    factor = max_vms / total
    for spec in apps.values():
        spec["count"] = max(1, round(spec["count"] * factor))
    return sum(a["count"] for a in apps.values())


def load_scenarios(path: str | Path | None) -> list[dict]:
    """Load scenarios.yaml; returns [] if path is None/missing."""
    if path is None:
        return []
    p = Path(path)
    if not p.exists():
        return []
    doc = yaml.safe_load(p.read_text()) or {}
    return doc.get("scenarios", []) or []


# --------------------------------------------------------------------------- #
# Fleet materialisation
# --------------------------------------------------------------------------- #
@dataclasses.dataclass
class Fleet:
    vm_id: np.ndarray       # (n_vms,) str
    app: np.ndarray         # (n_vms,) str
    site: np.ndarray        # (n_vms,) str  (site name)
    site_type: np.ndarray   # (n_vms,) str  (location_type)
    region: np.ndarray      # (n_vms,) str
    app_rows: dict[str, np.ndarray]   # app -> row indices into the arrays

    @property
    def n_vms(self) -> int:
        return self.vm_id.shape[0]


def normalize_sites(sites) -> list[dict]:
    """Accept plain strings (legacy) or {name, type, region} maps -> list of maps."""
    out = []
    for s in sites:
        if isinstance(s, str):
            out.append({"name": s, "type": "unknown", "region": "unknown"})
        else:
            out.append({"name": s["name"], "type": s.get("type", "unknown"),
                        "region": s.get("region", "unknown")})
    return out


def build_fleet(fleet_spec: dict) -> Fleet:
    """Materialise VM identities. VMs are grouped contiguously by app so that
    baseline fills and per-app incident targeting are simple array slices; each
    VM is round-robin-assigned a site (with its type + region)."""
    sites = normalize_sites(fleet_spec["sites"])
    vm_ids: list[str] = []
    apps: list[str] = []
    site_of: list[str] = []
    type_of: list[str] = []
    region_of: list[str] = []
    app_rows: dict[str, np.ndarray] = {}

    cursor = 0
    for app_name, spec in fleet_spec["apps"].items():
        count = int(spec["count"])
        idx = np.arange(cursor, cursor + count)
        app_rows[app_name] = idx
        for i in range(count):
            vm_ids.append(f"{app_name}-{i:04d}")
            apps.append(app_name)
            s = sites[(cursor + i) % len(sites)]    # global round-robin -> even spread
            site_of.append(s["name"])
            type_of.append(s["type"])
            region_of.append(s["region"])
        cursor += count

    return Fleet(
        vm_id=np.array(vm_ids),
        app=np.array(apps),
        site=np.array(site_of),
        site_type=np.array(type_of),
        region=np.array(region_of),
        app_rows=app_rows,
    )


def build_sites_dim(fleet: Fleet) -> pd.DataFrame:
    """Distinct (site, location_type, region) dimension table (static, no ts)."""
    df = pd.DataFrame({"site": fleet.site, "location_type": fleet.site_type,
                       "region": fleet.region})
    return df.drop_duplicates().reset_index(drop=True)[SITES_COLUMNS]


# --------------------------------------------------------------------------- #
# Generation
# --------------------------------------------------------------------------- #
@dataclasses.dataclass
class GenResult:
    metrics: pd.DataFrame          # long vm_metrics rows
    events: pd.DataFrame           # app_events rows
    incident_vms: dict[str, list[str]]   # scenario name -> vm_ids hit
    ts: np.ndarray                 # (n_samples,) datetime64[ns] sample grid
    topology: pd.DataFrame = dataclasses.field(default_factory=pd.DataFrame)
    sites: pd.DataFrame = dataclasses.field(default_factory=pd.DataFrame)


def build_topology(fleet: Fleet, fleet_spec: dict) -> pd.DataFrame:
    """One row per (vm_id, depends_on_app) from the app-level edges in
    fleet_spec['topology']. Static dimension table (no ts)."""
    edges = fleet_spec.get("topology", {}) or {}
    rows = []
    for i in range(fleet.n_vms):
        app = fleet.app[i]
        for dep in edges.get(app, []):
            rows.append((fleet.vm_id[i], app, fleet.site[i], dep))
    return pd.DataFrame(rows, columns=TOPOLOGY_COLUMNS)


def _baseline_matrix(fleet: Fleet, fleet_spec: dict, n_samples: int,
                     rng: np.random.Generator) -> dict[str, np.ndarray]:
    """One (n_vms, n_samples) float64 array per metric, filled with per-app
    baseline mean + gaussian noise, vectorised per app."""
    n = fleet.n_vms
    noise_pct = float(fleet_spec["noise_pct"])
    mats = {m: np.empty((n, n_samples), dtype=np.float64) for m in METRICS}

    _fill_app_baselines(fleet, fleet_spec, mats, noise_pct, rng)
    _apply_type_profiles(fleet, fleet_spec, mats)
    return mats


def _fill_app_baselines(fleet, fleet_spec, mats, noise_pct, rng):
    n_samples = mats[METRICS[0]].shape[1]
    for app_name, rows in fleet.app_rows.items():
        base = fleet_spec["apps"][app_name]["baseline"]
        r0, r1 = rows[0], rows[-1] + 1
        block = (r1 - r0, n_samples)
        for m in METRICS:
            mean = float(base[m])
            std = max(abs(mean) * noise_pct, _NOISE_FLOOR.get(m, 0.0))
            mats[m][r0:r1, :] = mean + rng.normal(0.0, std, size=block)
    return mats


def _apply_type_profiles(fleet: Fleet, fleet_spec: dict, mats: dict) -> None:
    """Scale each VM's baseline metrics by its site-type multipliers, in place,
    so location types (warehouse burst, manufacturing steady, ...) differ.
    No-op when there are no type_profiles (legacy flat-site fleets)."""
    profiles = fleet_spec.get("type_profiles", {}) or {}
    if not profiles:
        return
    for m in METRICS:
        mult = np.array([profiles.get(t, {}).get(m, 1.0) for t in fleet.site_type])
        if not np.allclose(mult, 1.0):
            mats[m] *= mult[:, None]


def _window_mask(n_samples: int, interval_s: int, start_min: float,
                 end_min: float) -> np.ndarray:
    """Boolean (n_samples,) mask for columns inside [start_min, end_min)."""
    minutes = np.arange(n_samples) * (interval_s / 60.0)
    return (minutes >= start_min) & (minutes < end_min)


def _target_rows(fleet: Fleet, scen: dict, rng: np.random.Generator) -> np.ndarray:
    """Resolve a scenario's target VM row indices. Filter by app (optional) and
    any of site / location_type / region, then take the first `count`."""
    if scen.get("vms"):
        wanted = set(scen["vms"])
        return np.array([i for i, v in enumerate(fleet.vm_id) if v in wanted])
    rows = fleet.app_rows[scen["app"]] if scen.get("app") else np.arange(fleet.n_vms)
    if scen.get("site"):
        rows = rows[fleet.site[rows] == scen["site"]]
    if scen.get("location_type"):
        rows = rows[fleet.site_type[rows] == scen["location_type"]]
    if scen.get("region"):
        rows = rows[fleet.region[rows] == scen["region"]]
    count = int(scen.get("count", 1))
    return rows[:count]   # deterministic first-N


def _elapsed_min(interval_s: int, n_samples: int, start_min: float,
                 mask: np.ndarray) -> np.ndarray:
    """Minutes elapsed since window start, 0 outside the window. Shape (n_samples,)."""
    minutes = np.arange(n_samples) * (interval_s / 60.0)
    el = np.where(mask, minutes - start_min, 0.0)
    return np.clip(el, 0.0, None)


def _apply_incidents(mats: dict[str, np.ndarray], fleet: Fleet, scenarios: list[dict],
                     ts: np.ndarray, interval_s: int, rng: np.random.Generator,
                     ) -> tuple[list[dict], dict[str, list[str]]]:
    """Overlay incident templates on the baseline matrices in place and collect
    the app_events they emit. Returns (events, incident_vms)."""
    n_samples = ts.shape[0]
    events: list[dict] = []
    incident_vms: dict[str, list[str]] = {}

    def emit(row: int, col: int, level: str, etype: str, msg: str) -> None:
        events.append({
            "ts": ts[col], "vm_id": fleet.vm_id[row], "app": fleet.app[row],
            "site": fleet.site[row], "level": level, "event_type": etype,
            "message": msg,
        })

    for scen in scenarios:
        name = scen["name"]
        template = scen["template"]
        start_min = float(scen["start_min"])
        end_min = float(scen["end_min"])
        params = scen.get("params", {}) or {}
        rows = _target_rows(fleet, scen, rng)
        incident_vms[name] = [fleet.vm_id[r] for r in rows]
        if rows.size == 0:
            continue
        mask = _window_mask(n_samples, interval_s, start_min, end_min)
        if not mask.any():
            continue
        cols = np.where(mask)[0]
        elapsed = _elapsed_min(interval_s, n_samples, start_min, mask)  # (n_samples,)

        if template == "heap_leak":
            _tpl_heap_leak(mats, rows, cols, elapsed, params, emit)
        elif template == "io_saturation":
            _tpl_io_saturation(mats, rows, cols, elapsed, end_min - start_min,
                               interval_s, params, emit)
        elif template == "net_partition":
            _tpl_net_partition(mats, rows, cols, interval_s, params, emit)
        elif template == "error_storm":
            _tpl_error_storm(mats, rows, cols, interval_s, params, emit)
        elif template == "noisy_neighbor":
            _tpl_noisy_neighbor(mats, rows, cols, params)
        else:
            raise ValueError(f"unknown incident template: {template!r}")

    return events, incident_vms


# --- individual templates (each mutates mats[...] for rows x cols) ---------- #
def _tpl_heap_leak(mats, rows, cols, elapsed, params, emit) -> None:
    leak = float(params.get("leak_mb_per_min", 20.0))
    cap = float(params.get("heap_cap_mb", 4096.0))
    delta = leak * elapsed[cols]                        # (n_cols,)
    for r in rows:
        base = mats["heap_mb"][r, cols]                 # baseline + per-sample noise
        # OOM and coupling track the underlying ramp (baseline mean + leak), so a
        # single noisy sample near the cap can't trigger a spurious OOM/restart.
        underlying = base.mean() + delta
        heap = base + delta                             # reported value keeps noise
        frac = np.clip(underlying / cap, 0.0, 1.5)
        mats["gc_pause_ms"][r, cols] += 120.0 * frac ** 2
        mats["app_latency_ms"][r, cols] += 200.0 * frac ** 2
        # OOM when the trend crosses the cap: emit once, then process "restarts".
        over = np.where(underlying >= cap)[0]
        if over.size:
            first = over[0]
            emit(int(r), int(cols[first]), "ERROR", "oom_kill",
                 f"java.lang.OutOfMemoryError: Java heap space (cap {int(cap)}MB)")
            heap[first:] = base[first:]                 # back to baseline
        mats["heap_mb"][r, cols] = np.minimum(heap, cap)


def _tpl_io_saturation(mats, rows, cols, elapsed, window_min, interval_s,
                       params, emit) -> None:
    await_ramp = float(params.get("await_ramp_ms_per_min", 3.0))
    collapse = float(params.get("iops_collapse_frac", 0.8))
    progress = np.clip(elapsed[cols] / max(window_min, 1e-6), 0.0, 1.0)
    for r in rows:
        mats["disk_io_await_ms"][r, cols] += await_ramp * elapsed[cols]
        mats["disk_iops"][r, cols] *= (1.0 - collapse * progress)
        mats["app_latency_ms"][r, cols] += 60.0 * progress
    _emit_periodic(rows, cols, interval_s, 300, emit, "WARN", "slow_query",
                   "slow query: statement exceeded 1000ms")


def _tpl_net_partition(mats, rows, cols, interval_s, params, emit) -> None:
    spike = float(params.get("retrans_spike_pct", 8.0))
    for r in rows:
        mats["net_retrans_pct"][r, cols] += spike
        mats["net_mbps"][r, cols] *= 0.5          # throughput sags under loss
    _emit_periodic(rows, cols, interval_s, 120, emit, "ERROR", "conn_reset",
                   "connection reset by peer during replication")


def _tpl_error_storm(mats, rows, cols, interval_s, params, emit) -> None:
    jump = float(params.get("error_jump_pct", 6.0))
    for r in rows:
        mats["error_rate_pct"][r, cols] += jump
        mats["app_latency_ms"][r, cols] += 40.0
    _emit_periodic(rows, cols, interval_s, 60, emit, "ERROR", "http_5xx",
                   "HTTP 503 Service Unavailable burst")


def _tpl_noisy_neighbor(mats, rows, cols, params) -> None:
    steal = float(params.get("cpu_steal_pct", 35.0))
    for r in rows:
        mats["cpu_pct"][r, cols] += steal           # metrics-only, no events


def _emit_periodic(rows, cols, interval_s, every_s, emit, level, etype, msg) -> None:
    """Emit an event on the target rows every `every_s` seconds within the window."""
    step = max(int(round(every_s / interval_s)), 1)
    for r in rows:
        for c in cols[::step]:
            emit(int(r), int(c), level, etype, msg)


def generate(fleet_spec: dict, scenarios: list[dict], start: datetime,
             n_samples: int, seed: int = 42) -> GenResult:
    """Generate `n_samples` samples for the whole fleet starting at `start`."""
    interval_s = int(fleet_spec["sample_interval_s"])
    rng = np.random.default_rng(seed)
    fleet = build_fleet(fleet_spec)

    ts = (np.datetime64(_as_naive_utc(start), "s")
          + np.arange(n_samples) * np.timedelta64(interval_s, "s")).astype("datetime64[ns]")

    mats = _baseline_matrix(fleet, fleet_spec, n_samples, rng)
    events, incident_vms = _apply_incidents(mats, fleet, scenarios, ts, interval_s, rng)

    # Clip non-negatives after all overlays.
    for m in _NONNEG:
        np.clip(mats[m], 0.0, None, out=mats[m])

    metrics_df = _to_long_frame(fleet, ts, mats)
    events_df = (pd.DataFrame(events, columns=EVENT_COLUMNS)
                 if events else pd.DataFrame(columns=EVENT_COLUMNS))
    if not events_df.empty:
        events_df = events_df.sort_values(["ts", "vm_id"]).reset_index(drop=True)

    return GenResult(metrics=metrics_df, events=events_df,
                     incident_vms=incident_vms, ts=ts,
                     topology=build_topology(fleet, fleet_spec),
                     sites=build_sites_dim(fleet))


def _as_naive_utc(dt: datetime) -> datetime:
    """Normalise to a tz-naive UTC datetime for numpy datetime64."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _to_long_frame(fleet: Fleet, ts: np.ndarray,
                   mats: dict[str, np.ndarray]) -> pd.DataFrame:
    """Melt (n_vms, n_samples) matrices into the long vm_metrics contract.

    Row-major ravel of each matrix (vm0's samples, then vm1's, ...) aligns with
    np.repeat(vm, n_samples) and np.tile(ts, n_vms)."""
    n_vms, n_samples = mats[METRICS[0]].shape
    data = {
        "ts": np.tile(ts, n_vms),
        "vm_id": np.repeat(fleet.vm_id, n_samples),
        "app": np.repeat(fleet.app, n_samples),
        "site": np.repeat(fleet.site, n_samples),
    }
    for m in METRICS:
        data[m] = mats[m].reshape(-1).astype(np.float32)
    df = pd.DataFrame(data)
    # Round the physical metrics to sane precision (keeps parquet compact).
    return df


# --------------------------------------------------------------------------- #
# Parquet output (hive-partitioned by dt / hour)
# --------------------------------------------------------------------------- #
def _partition_cols(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["ts"])
    out = df.copy()
    out["dt"] = ts.dt.strftime("%Y-%m-%d")
    out["hour"] = ts.dt.strftime("%H")
    return out


def write_flat(df: pd.DataFrame, root: str | Path, table: str,
               basename: str = "part-0.parquet") -> Path:
    """Write df as a single unpartitioned parquet under root/table/ (for static
    dimension tables like topology, which have no ts to partition by)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    target = Path(root) / table
    target.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                       target / basename)
    return target


def write_dataset(df: pd.DataFrame, root: str | Path, table: str,
                  basename: str = "part") -> Path:
    """Write df to root/table/dt=.../hour=.../<basename>-*.parquet (hive style)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    target = Path(root) / table
    if df.empty:
        target.mkdir(parents=True, exist_ok=True)
        return target
    part = _partition_cols(df)
    tbl = pa.Table.from_pandas(part, preserve_index=False)
    pq.write_to_dataset(
        tbl, root_path=str(target), partition_cols=["dt", "hour"],
        basename_template=f"{basename}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    return target
