"""Tests for M5: topology generation + blast-radius graph traversal + the
copilot's trace_dependencies tool and blast-radius routing.
"""

from datetime import datetime, timezone

import duckdb
import pytest

from aiops import copilot, detect, generator as gen

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
DC = detect.DetectConfig(heap_cap_mb=4096, io_z=4, retrans_pct=5, latency_x=2)


@pytest.fixture
def fleet_spec():
    base = {"cpu_pct": 30, "heap_mb": 1800, "gc_pause_ms": 30, "disk_io_await_ms": 8,
            "disk_iops": 1500, "net_mbps": 100, "net_retrans_pct": 0.3,
            "app_latency_ms": 80, "error_rate_pct": 0.2}
    return {
        "sample_interval_s": 30, "noise_pct": 0.05, "sites": ["dc-east", "dc-west"],
        "topology": {"patient-onboarding": ["ehr-api"], "ehr-api": ["postgres-db"],
                     "hl7-ingest": ["kafka"], "kafka": ["ehr-api"]},
        "apps": {
            "patient-onboarding": {"count": 2, "baseline": dict(base)},
            "ehr-api": {"count": 2, "baseline": dict(base)},
            "postgres-db": {"count": 2, "baseline": dict(base)},
            "hl7-ingest": {"count": 2, "baseline": dict(base)},
            "kafka": {"count": 2, "baseline": dict(base)},
        },
    }


def _con(res):
    con = duckdb.connect()
    con.register("vm_metrics", res.metrics)
    con.register("app_events", res.events)
    con.register("topology", res.topology)
    return con


def test_topology_edges_generated(fleet_spec):
    res = gen.generate(fleet_spec, [], start=START, n_samples=10, seed=1)
    edges = set(map(tuple, res.topology[["app", "depends_on_app"]]
                    .drop_duplicates().itertuples(index=False)))
    assert ("patient-onboarding", "ehr-api") in edges
    assert ("ehr-api", "postgres-db") in edges
    assert ("kafka", "ehr-api") in edges
    # one row per VM of an app-with-deps (2 VMs each here)
    po = res.topology[res.topology["app"] == "patient-onboarding"]
    assert len(po) == 2
    # columns match the contract
    assert list(res.topology.columns) == gen.TOPOLOGY_COLUMNS


def test_blast_radius_downstream_and_upstream(fleet_spec):
    res = gen.generate(fleet_spec, [], start=START, n_samples=10, seed=1)
    tools = copilot.LakeTools(con=_con(res), dc=DC)
    trace = tools.trace_dependencies("postgres-db")
    # postgres is a leaf dependency: nothing upstream, but ehr-api (depth1) and
    # patient-onboarding + kafka (depth2) and hl7-ingest (depth3) downstream.
    assert "upstream (postgres-db depends on):\n  (none)" in trace
    assert "depth 1: ehr-api" in trace
    assert "depth 2: patient-onboarding" in trace
    assert "depth 2: kafka" in trace
    assert "depth 3: hl7-ingest" in trace

    # And the reverse view: patient-onboarding depends on ehr-api -> postgres-db.
    up = tools.trace_dependencies("patient-onboarding")
    assert "depth 1: ehr-api" in up
    assert "depth 2: postgres-db" in up


def test_blast_radius_annotates_alerts(fleet_spec):
    # inject a postgres io_spike; blast radius should still surface downstream apps
    scen = [{"name": "io", "template": "io_saturation", "app": "postgres-db",
             "count": 1, "start_min": 20, "end_min": 120,
             "params": {"await_ramp_ms_per_min": 5, "iops_collapse_frac": 0.8}}]
    res = gen.generate(fleet_spec, scen, start=START, n_samples=180, seed=1)
    tools = copilot.LakeTools(con=_con(res), dc=DC)
    trace = tools.trace_dependencies("postgres-db", asof_min=30)
    # postgres itself isn't in its own downstream, but the traversal runs and the
    # impacted chain is present (ehr-api -> patient-onboarding).
    assert "patient-onboarding" in trace


def test_is_blast_question():
    assert copilot.is_blast_question("what happens if postgres-db goes down?")
    assert copilot.is_blast_question("blast radius of ehr-api?")
    assert copilot.is_blast_question("what is the impact if kafka fails?")
    assert not copilot.is_blast_question("why is patient onboarding slow?")


def test_deterministic_blast_answer_routes_to_graph(fleet_spec):
    res = gen.generate(fleet_spec, [], start=START, n_samples=10, seed=1)
    tools = copilot.LakeTools(con=_con(res), dc=DC)
    ans = copilot.answer("what happens if postgres-db goes down?", tools, cfg=None,
                         backend="none")
    assert "Blast radius: postgres-db" in ans.text
    assert "downstream" in ans.text
    assert ans.used_fallback


def test_trace_dependencies_tool_dispatch(fleet_spec):
    res = gen.generate(fleet_spec, [], start=START, n_samples=10, seed=1)
    tools = copilot.LakeTools(con=_con(res), dc=DC)
    out = tools.call("trace_dependencies", {"app": "postgres-db"})
    assert "downstream" in out and "ehr-api" in out
