"""Tests for the M3 detection engine.

Uses the generator to build a small deterministic fleet, registers it directly
into DuckDB, and asserts: incident VMs raise the expected rules (golden set),
a healthy fleet raises nothing, and the OOM ETA shrinks across an asof replay.
"""

from datetime import datetime, timezone

import duckdb
import pandas as pd
import pytest

from aiops import detect, generator as gen

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _register(con, metrics: pd.DataFrame, events: pd.DataFrame):
    con.register("vm_metrics", metrics.drop(columns=[c for c in ("dt", "hour")
                                                     if c in metrics.columns]))
    con.register("app_events", events)


@pytest.fixture
def fleet_spec():
    base = {"cpu_pct": 30, "heap_mb": 1800, "gc_pause_ms": 30, "disk_io_await_ms": 8,
            "disk_iops": 1500, "net_mbps": 100, "net_retrans_pct": 0.3,
            "app_latency_ms": 80, "error_rate_pct": 0.2}
    return {
        "sample_interval_s": 30, "noise_pct": 0.05, "sites": ["dc-east", "dc-west"],
        "apps": {
            "patient-onboarding": {"count": 4, "baseline": dict(base)},
            "postgres-db": {"count": 3, "baseline": dict(base)},
            "billing-svc": {"count": 3, "baseline": dict(base)},
            "hl7-ingest": {"count": 3, "baseline": dict(base)},
        },
    }


DC = detect.DetectConfig(heap_cap_mb=4096, io_z=4, retrans_pct=5, latency_x=2)


def test_detectors_fire_expected_rules(fleet_spec):
    scenarios = [
        {"name": "leak", "template": "heap_leak", "app": "patient-onboarding",
         "count": 2, "start_min": 5, "end_min": 120,
         "params": {"leak_mb_per_min": 25, "heap_cap_mb": 4096}},
        {"name": "io", "template": "io_saturation", "app": "postgres-db",
         "count": 1, "start_min": 40, "end_min": 120,
         "params": {"await_ramp_ms_per_min": 4, "iops_collapse_frac": 0.8}},
        {"name": "net", "template": "net_partition", "app": "hl7-ingest",
         "count": 1, "start_min": 40, "end_min": 120, "params": {"retrans_spike_pct": 8}},
        {"name": "err", "template": "error_storm", "app": "billing-svc",
         "count": 1, "start_min": 40, "end_min": 120, "params": {"error_jump_pct": 6}},
    ]
    res = gen.generate(fleet_spec, scenarios, start=START, n_samples=180, seed=1)
    con = duckdb.connect()
    _register(con, res.metrics, res.events)

    alerts = detect.detect(con, DC, asof_min=30)
    rules = set(alerts["rule"])
    # Golden set of rules for this incident mix.
    assert {"heap_leak", "io_spike", "net_retrans", "error_storm"} <= rules

    # heap_leak fired on exactly the injected VMs, with a positive finite ETA.
    heap = alerts[alerts["rule"] == "heap_leak"]
    assert set(heap["vm_id"]) == set(res.incident_vms["leak"])
    import json
    for ev in heap["evidence_json"]:
        assert 0 < json.loads(ev)["oom_eta_min"] < 120


def test_healthy_fleet_zero_alerts(fleet_spec):
    res = gen.generate(fleet_spec, [], start=START, n_samples=180, seed=2)
    con = duckdb.connect()
    _register(con, res.metrics, res.events)
    for asof in (0, 30, 60):
        alerts = detect.detect(con, DC, asof_min=asof)
        assert alerts.empty, f"healthy fleet raised alerts at asof {asof}"


def test_oom_eta_shrinks_across_replay(fleet_spec):
    scenarios = [{"name": "leak", "template": "heap_leak", "app": "patient-onboarding",
                  "count": 1, "start_min": 5, "end_min": 120,
                  "params": {"leak_mb_per_min": 20, "heap_cap_mb": 4096}}]
    res = gen.generate(fleet_spec, scenarios, start=START, n_samples=240, seed=1)
    con = duckdb.connect()
    _register(con, res.metrics, res.events)

    import json
    etas = {}
    for asof in (45, 30, 15):
        a = detect.detect(con, DC, asof_min=asof)
        h = a[a["rule"] == "heap_leak"]
        assert not h.empty, f"no heap_leak at asof {asof}"
        etas[asof] = json.loads(h.iloc[0]["evidence_json"])["oom_eta_min"]
    # Later "now" (smaller asof) => smaller ETA.
    assert etas[45] > etas[30] > etas[15]


def test_alert_contract_columns(fleet_spec):
    scenarios = [{"name": "err", "template": "error_storm", "app": "billing-svc",
                  "count": 1, "start_min": 40, "end_min": 120,
                  "params": {"error_jump_pct": 8}}]
    res = gen.generate(fleet_spec, scenarios, start=START, n_samples=180, seed=1)
    con = duckdb.connect()
    _register(con, res.metrics, res.events)
    alerts = detect.detect(con, DC, asof_min=30)
    assert list(alerts.columns) == detect.ALERT_COLUMNS
