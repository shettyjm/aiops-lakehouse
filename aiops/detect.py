"""Deterministic detection engine over the telemetry lake (M3).

All detection math runs in DuckDB SQL (grouped window/regression queries), never
per-VM Python loops, so it scales to 2,000 VMs in seconds. Detectors read either
local parquet (--source local) or the Iceberg tables (--source iceberg), both
registered as DuckDB relations `vm_metrics` and `app_events`.

Rules (thresholds from config.ini [detect]):
  heap_leak   trailing-30min regression slope + correlation per VM, OOM ETA
  io_spike    z-score of disk await vs the VM's own baseline
  net_retrans recent mean net_retrans_pct over a threshold
  apm_latency recent p95 app latency vs baseline p95, per app
  error_storm recent error_rate jump vs baseline, per VM

Alerts follow the contract: ts, severity, rule, vm_id, app, headline,
evidence_json, action.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from aiops import ingest

# Window sizes (minutes).
RECENT_MIN = 10          # "now" window for spike/threshold rules
HEAP_MIN = 30            # trailing window for the heap-leak regression
BASELINE_REF_MIN = 30    # healthy reference window anchored at data start

# Rule tuning not covered by [detect].
HEAP_MIN_SLOPE_MB = 4.0  # ignore heap drift below this MB/min
HEAP_MIN_CORR = 0.60     # monotonic-ish ramp; slope gate excludes healthy VMs,
                         # this tolerates cap-clipping flattening near OOM
HEAP_OOM_HORIZON = 120   # only alert if projected OOM within this many minutes
HEAP_P1_ETA = 45         # ETA at/under this = P1
LAT_FLOOR_MS = 60.0      # ignore tiny-latency apps
ERR_FLOOR_PCT = 3.0      # absolute error-rate floor for a storm
ERR_JUMP_X = 3.0         # and this many times baseline

ALERTS_FIELDS = [
    ("ts", "timestamp"), ("severity", "string"), ("rule", "string"),
    ("vm_id", "string"), ("app", "string"), ("headline", "string"),
    ("evidence_json", "string"), ("action", "string"),
]

ALERT_COLUMNS = [f[0] for f in ALERTS_FIELDS]


@dataclass
class DetectConfig:
    heap_cap_mb: float
    io_z: float
    retrans_pct: float
    latency_x: float

    @classmethod
    def from_ini(cls, cfg) -> "DetectConfig":
        return cls(
            heap_cap_mb=cfg.getfloat("detect", "heap_limit_mb", fallback=4096),
            io_z=cfg.getfloat("detect", "io_z_threshold", fallback=4),
            retrans_pct=cfg.getfloat("detect", "retrans_pct", fallback=5),
            latency_x=cfg.getfloat("detect", "latency_x", fallback=2),
        )


# --------------------------------------------------------------------------- #
# DuckDB source wiring
# --------------------------------------------------------------------------- #
_DUCK_TYPE = {"timestamp": "TIMESTAMP", "string": "VARCHAR", "float": "FLOAT"}


def duckdb_from_local(data_dir: str):
    import glob
    import duckdb
    con = duckdb.connect()
    for tbl, fields in ingest.TABLE_FIELDS.items():
        files = sorted(glob.glob(f"{data_dir}/{tbl}/**/*.parquet", recursive=True))
        if files:
            con.execute(f"CREATE VIEW {tbl} AS "
                        f"SELECT * FROM read_parquet({files!r}, union_by_name=true)")
        else:
            # Empty (e.g. a healthy fleet emits no events) -> typed empty table.
            cols = ", ".join(f"{n} {_DUCK_TYPE[t]}" for n, t in fields)
            con.execute(f"CREATE TABLE {tbl} ({cols})")
    return con


def duckdb_from_iceberg(cfg, namespace: str, insecure: bool = False):
    import duckdb
    catalog = ingest.iceberg_catalog(cfg, insecure=insecure)
    con = duckdb.connect()
    for tbl in ("vm_metrics", "app_events"):
        table = catalog.load_table((namespace, tbl))
        arrow = table.scan().to_arrow()
        con.register(tbl, arrow)
    return con


# --------------------------------------------------------------------------- #
# Detection
# --------------------------------------------------------------------------- #
def _ts_literal(dt: datetime) -> str:
    return "TIMESTAMP '%s'" % dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def evaluation_end(con, asof_min: float) -> datetime:
    """Data-end minus asof_min: the moment we pretend 'now' is."""
    tmax = con.execute("SELECT max(ts) FROM vm_metrics").fetchone()[0]
    if tmax is None:
        raise ValueError("vm_metrics is empty")
    return tmax - timedelta(minutes=asof_min)


def data_start(con) -> datetime:
    return con.execute("SELECT min(ts) FROM vm_metrics").fetchone()[0]


def _baseline_window(t_start: datetime, t_end: datetime) -> tuple[str, str]:
    """Clean reference window anchored at the healthy startup period, clamped so
    it never overlaps the recent window. Returns (lo_literal, hi_literal)."""
    hi = min(t_start + timedelta(minutes=BASELINE_REF_MIN),
             t_end - timedelta(minutes=RECENT_MIN))
    return _ts_literal(t_start), _ts_literal(hi)


def _q(con, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetch_df()


def _heap_leak(con, t_end, t_start, dc: DetectConfig) -> list[dict]:
    end = _ts_literal(t_end)
    lo = _ts_literal(t_end - timedelta(minutes=HEAP_MIN))
    df = _q(con, f"""
        WITH w AS (
          SELECT vm_id, any_value(app) AS app,
                 regr_slope(heap_mb, epoch(ts)/60.0) AS slope,
                 corr(heap_mb, epoch(ts)/60.0)       AS r,
                 arg_max(heap_mb, ts)                AS cur_heap,
                 count(*)                            AS n
          FROM vm_metrics
          WHERE ts > {lo} AND ts <= {end}
          GROUP BY vm_id
          HAVING count(*) >= 8
        )
        SELECT vm_id, app, slope, r, cur_heap,
               ({dc.heap_cap_mb} - cur_heap) / slope AS eta_min
        FROM w
        WHERE slope > {HEAP_MIN_SLOPE_MB} AND r > {HEAP_MIN_CORR}
          AND cur_heap > {dc.heap_cap_mb} * 0.5
          AND ({dc.heap_cap_mb} - cur_heap) / slope <= {HEAP_OOM_HORIZON}
    """)
    out = []
    for row in df.itertuples(index=False):
        eta = float(row.eta_min)
        sev = "P1" if eta <= HEAP_P1_ETA else "P2"
        out.append(_alert(
            t_end, sev, "heap_leak", row.vm_id, row.app,
            headline=f"Heap leak on {row.vm_id}: +{row.slope:.0f} MB/min, "
                     f"OOM ETA ~{eta:.0f} min",
            evidence={"slope_mb_min": round(float(row.slope), 2),
                      "corr": round(float(row.r), 3),
                      "cur_heap_mb": round(float(row.cur_heap), 1),
                      "heap_cap_mb": dc.heap_cap_mb,
                      "oom_eta_min": round(eta, 1)},
            action="Capture heap dump; schedule rolling restart before ETA; "
                   "check for unbounded cache/collection growth."))
    return out


def _io_spike(con, t_end, t_start, dc: DetectConfig) -> list[dict]:
    end = _ts_literal(t_end)
    rlo = _ts_literal(t_end - timedelta(minutes=RECENT_MIN))
    blo, bhi = _baseline_window(t_start, t_end)
    df = _q(con, f"""
        WITH base AS (
          SELECT vm_id, avg(disk_io_await_ms) AS b_mean,
                 stddev_samp(disk_io_await_ms) AS b_std
          FROM vm_metrics WHERE ts >= {blo} AND ts < {bhi}
          GROUP BY vm_id
        ), recent AS (
          SELECT vm_id, any_value(app) AS app,
                 avg(disk_io_await_ms) AS r_mean, arg_max(disk_iops, ts) AS cur_iops
          FROM vm_metrics WHERE ts > {rlo} AND ts <= {end}
          GROUP BY vm_id
        )
        SELECT r.vm_id, r.app, r.r_mean, b.b_mean, b.b_std, r.cur_iops,
               (r.r_mean - b.b_mean) / b.b_std AS z
        FROM recent r JOIN base b USING (vm_id)
        WHERE b.b_std > 0.01 AND (r.r_mean - b.b_mean) / b.b_std > {dc.io_z}
    """)
    out = []
    for row in df.itertuples(index=False):
        out.append(_alert(
            t_end, "P2", "io_spike", row.vm_id, row.app,
            headline=f"Disk IO saturation on {row.vm_id}: await "
                     f"{row.r_mean:.0f}ms (z={row.z:.1f})",
            evidence={"await_ms": round(float(row.r_mean), 1),
                      "baseline_ms": round(float(row.b_mean), 1),
                      "z": round(float(row.z), 2),
                      "cur_iops": round(float(row.cur_iops), 0)},
            action="Check storage backend / noisy neighbor; inspect slow queries "
                   "and disk queue depth."))
    return out


def _net_retrans(con, t_end, t_start, dc: DetectConfig) -> list[dict]:
    end = _ts_literal(t_end)
    rlo = _ts_literal(t_end - timedelta(minutes=RECENT_MIN))
    df = _q(con, f"""
        SELECT vm_id, any_value(app) AS app, avg(net_retrans_pct) AS retrans
        FROM vm_metrics WHERE ts > {rlo} AND ts <= {end}
        GROUP BY vm_id
        HAVING avg(net_retrans_pct) > {dc.retrans_pct}
    """)
    out = []
    for row in df.itertuples(index=False):
        out.append(_alert(
            t_end, "P2", "net_retrans", row.vm_id, row.app,
            headline=f"Network retransmits on {row.vm_id}: "
                     f"{row.retrans:.1f}% (> {dc.retrans_pct:.0f}%)",
            evidence={"retrans_pct": round(float(row.retrans), 2),
                      "threshold_pct": dc.retrans_pct},
            action="Check NIC/switch path, packet loss, and replication links; "
                   "look for conn_reset events."))
    return out


def _apm_latency(con, t_end, t_start, dc: DetectConfig) -> list[dict]:
    end = _ts_literal(t_end)
    rlo = _ts_literal(t_end - timedelta(minutes=RECENT_MIN))
    blo, bhi = _baseline_window(t_start, t_end)
    df = _q(con, f"""
        WITH base AS (
          SELECT app, quantile_cont(app_latency_ms, 0.95) AS b_p95
          FROM vm_metrics WHERE ts >= {blo} AND ts < {bhi} GROUP BY app
        ), recent AS (
          SELECT app, quantile_cont(app_latency_ms, 0.95) AS r_p95
          FROM vm_metrics WHERE ts > {rlo} AND ts <= {end} GROUP BY app
        )
        SELECT recent.app, r_p95, b_p95
        FROM recent JOIN base USING (app)
        WHERE r_p95 > {dc.latency_x} * b_p95 AND r_p95 > {LAT_FLOOR_MS}
    """)
    out = []
    for row in df.itertuples(index=False):
        out.append(_alert(
            t_end, "P2", "apm_latency", "", row.app,
            headline=f"Latency regression on {row.app}: p95 {row.r_p95:.0f}ms "
                     f"(>{dc.latency_x:.0f}x baseline {row.b_p95:.0f}ms)",
            evidence={"p95_ms": round(float(row.r_p95), 1),
                      "baseline_p95_ms": round(float(row.b_p95), 1),
                      "factor_x": round(float(row.r_p95) / float(row.b_p95), 2)},
            action="Correlate with downstream dependency alerts (DB/IO); check "
                   "recent deploys and GC pauses."))
    return out


def _error_storm(con, t_end, t_start, dc: DetectConfig) -> list[dict]:
    end = _ts_literal(t_end)
    rlo = _ts_literal(t_end - timedelta(minutes=RECENT_MIN))
    blo, bhi = _baseline_window(t_start, t_end)
    df = _q(con, f"""
        WITH base AS (
          SELECT vm_id, avg(error_rate_pct) AS b_err
          FROM vm_metrics WHERE ts >= {blo} AND ts < {bhi} GROUP BY vm_id
        ), recent AS (
          SELECT vm_id, any_value(app) AS app, avg(error_rate_pct) AS r_err
          FROM vm_metrics WHERE ts > {rlo} AND ts <= {end} GROUP BY vm_id
        )
        SELECT r.vm_id, r.app, r.r_err, b.b_err
        FROM recent r JOIN base b USING (vm_id)
        WHERE r.r_err > {ERR_FLOOR_PCT}
          AND r.r_err > {ERR_JUMP_X} * greatest(b.b_err, 0.1)
    """)
    out = []
    for row in df.itertuples(index=False):
        jump = float(row.r_err) / max(float(row.b_err), 0.1)
        out.append(_alert(
            t_end, "P1" if row.r_err > 8 else "P2", "error_storm", row.vm_id, row.app,
            headline=f"Error storm on {row.vm_id}: {row.r_err:.1f}% errors "
                     f"({jump:.0f}x baseline)",
            evidence={"error_rate_pct": round(float(row.r_err), 2),
                      "baseline_pct": round(float(row.b_err), 2),
                      "jump_x": round(jump, 1)},
            action="Check 5xx sources and upstream dependencies; consider "
                   "circuit-breaking / rollback."))
    return out


_RULES = [_heap_leak, _io_spike, _net_retrans, _apm_latency, _error_storm]


def _alert(t_end, severity, rule, vm_id, app, headline, evidence, action) -> dict:
    return {
        "ts": pd.Timestamp(t_end), "severity": severity, "rule": rule,
        "vm_id": vm_id or "", "app": app or "", "headline": headline,
        "evidence_json": json.dumps(evidence, sort_keys=True), "action": action,
    }


def detect(con, dc: DetectConfig, asof_min: float = 0.0) -> pd.DataFrame:
    """Run every rule as of (data end - asof_min) and return an alerts DataFrame."""
    t_end = evaluation_end(con, asof_min)
    t_start = data_start(con)
    rows: list[dict] = []
    for rule in _RULES:
        rows.extend(rule(con, t_end, t_start, dc))
    df = pd.DataFrame(rows, columns=ALERT_COLUMNS)
    if not df.empty:
        sev_order = {"P1": 0, "P2": 1, "P3": 2}
        df = (df.assign(_o=df["severity"].map(lambda s: sev_order.get(s, 9)))
                .sort_values(["_o", "rule", "vm_id"]).drop(columns="_o")
                .reset_index(drop=True))
    return df


# --------------------------------------------------------------------------- #
# Alert sinks
# --------------------------------------------------------------------------- #
def write_alerts_json(df: pd.DataFrame, path: str) -> None:
    from pathlib import Path
    records = df.copy()
    if not records.empty:
        records["ts"] = records["ts"].astype(str)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(records.to_json(orient="records", indent=2))


def write_alerts_iceberg(cfg, df: pd.DataFrame, namespace: str,
                         insecure: bool = False) -> int:
    """Append alerts to the Iceberg alerts table (created if missing)."""
    if df.empty:
        return 0
    catalog = ingest.iceberg_catalog(cfg, insecure=insecure)
    table = ingest.ensure_table(catalog, namespace, "alerts", ALERTS_FIELDS)
    batch = ingest.table_arrow_batch(table, df)
    table.append(batch)
    return len(df)


def post_webhook(url: str, df: pd.DataFrame, asof_min: float) -> None:
    """POST a Slack-format summary of the alerts to a webhook URL."""
    import requests
    if df.empty:
        text = f":white_check_mark: No alerts (asof -{asof_min:.0f}m)."
        blocks = []
    else:
        text = f":rotating_light: {len(df)} alert(s) (asof -{asof_min:.0f}m)"
        blocks = [{"severity": r.severity, "rule": r.rule,
                   "vm": r.vm_id or r.app, "headline": r.headline}
                  for r in df.itertuples(index=False)]
    payload = {"text": text,
               "attachments": [{"color": "#d00000" if not df.empty else "#2eb886",
                                "text": "\n".join(
                                    f"[{b['severity']}] {b['rule']} — {b['headline']}"
                                    for b in blocks)}]}
    requests.post(url, json=payload, timeout=10)
