"""Tests for the 25-site / multi-region extension: sites dimension generation,
per-type baseline profiles, location-based incident targeting, backward
compatibility with flat-string sites, and the copilot's location-enriched alerts.
"""

from datetime import datetime, timezone

import duckdb
import numpy as np
import pytest

from aiops import copilot, detect, generator as gen

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
DC = detect.DetectConfig(heap_cap_mb=4096, io_z=4, retrans_pct=5, latency_x=2)

BASE = {"cpu_pct": 30, "heap_mb": 1800, "gc_pause_ms": 30, "disk_io_await_ms": 8,
        "disk_iops": 1500, "net_mbps": 100, "net_retrans_pct": 0.3,
        "app_latency_ms": 80, "error_rate_pct": 0.2}


def _fleet(sites, type_profiles=None, apps=None):
    spec = {"sample_interval_s": 30, "noise_pct": 0.05, "sites": sites,
            "apps": apps or {"wms": {"count": 8, "baseline": dict(BASE)},
                             "erp": {"count": 8, "baseline": dict(BASE)}}}
    if type_profiles:
        spec["type_profiles"] = type_profiles
    return spec


RICH_SITES = [
    {"name": "na-wh-dallas", "type": "warehouse", "region": "NA"},
    {"name": "emea-plant-manchester", "type": "manufacturing", "region": "EMEA"},
    {"name": "dach-club-vienna", "type": "customer_club", "region": "DACH"},
]
PROFILES = {"warehouse": {"disk_io_await_ms": 1.5}}


def test_sites_dimension_generated():
    res = gen.generate(_fleet(RICH_SITES), [], start=START, n_samples=10, seed=1)
    assert list(res.sites.columns) == gen.SITES_COLUMNS
    d = {r.site: (r.location_type, r.region) for r in res.sites.itertuples(index=False)}
    assert d["na-wh-dallas"] == ("warehouse", "NA")
    assert d["dach-club-vienna"] == ("customer_club", "DACH")


def test_type_profiles_scale_baseline():
    res = gen.generate(_fleet(RICH_SITES, PROFILES), [], start=START, n_samples=40, seed=1)
    m = res.metrics.merge(res.sites, on="site")
    by_type = m.groupby("location_type")["disk_io_await_ms"].mean()
    # warehouse io_await is ~1.5x the (unscaled) manufacturing baseline
    assert by_type["warehouse"] > by_type["manufacturing"] * 1.3


def test_backward_compat_flat_sites():
    # legacy flat string sites -> unknown type/region, multipliers no-op
    res = gen.generate(_fleet(["dc-east", "dc-west"]), [], start=START, n_samples=10, seed=1)
    assert set(res.sites["location_type"]) == {"unknown"}
    assert set(res.metrics["site"]) == {"dc-east", "dc-west"}


def test_location_type_incident_targeting():
    scen = [{"name": "wh_leak", "template": "heap_leak", "location_type": "warehouse",
             "count": 2, "start_min": 5, "end_min": 120,
             "params": {"leak_mb_per_min": 25, "heap_cap_mb": 4096}}]
    # 4 sites, only 2 are warehouses -> the leak must land on warehouse VMs
    sites = RICH_SITES + [{"name": "apac-wh-tokyo", "type": "warehouse", "region": "APAC"}]
    res = gen.generate(_fleet(sites), start=START, n_samples=180, seed=1, scenarios=scen)
    hit = res.incident_vms["wh_leak"]
    assert len(hit) == 2
    site_of = dict(zip(res.metrics["vm_id"], res.metrics["site"]))
    wh_sites = set(res.sites[res.sites["location_type"] == "warehouse"]["site"])
    assert all(site_of[v] in wh_sites for v in hit)


def test_even_region_spread():
    # global round-robin spreads VMs ~evenly across sites (no NA over-weighting)
    res = gen.generate(_fleet(RICH_SITES), [], start=START, n_samples=5, seed=1)
    counts = (res.metrics[["vm_id", "site"]].drop_duplicates()
              .groupby("site").size())
    assert counts.max() - counts.min() <= 1     # 16 VMs over 3 sites -> 6/5/5


def test_location_question_routes_to_rollup():
    assert copilot.is_location_question("which regions are most at risk?")
    assert copilot.is_location_question("show warehouse sites")
    assert not copilot.is_location_question("why is wms slow?")
    scen = [{"name": "leak", "template": "heap_leak", "app": "wms", "count": 2,
             "start_min": 5, "end_min": 120,
             "params": {"leak_mb_per_min": 22, "heap_cap_mb": 4096}}]
    res = gen.generate(_fleet(RICH_SITES), start=START, n_samples=180, seed=1, scenarios=scen)
    con = duckdb.connect()
    con.register("vm_metrics", res.metrics)
    con.register("app_events", res.events)
    con.register("sites", res.sites)
    tools = copilot.LakeTools(con=con, dc=DC)
    out = copilot.answer("which regions are most at risk?", tools, cfg=None,
                         backend="none", asof_min=30)
    assert "risk by location" in out.text.lower()
    assert "Most at risk" in out.text


def test_copilot_get_alerts_enriched_with_location():
    scen = [{"name": "leak", "template": "heap_leak", "app": "wms", "count": 2,
             "start_min": 5, "end_min": 120,
             "params": {"leak_mb_per_min": 22, "heap_cap_mb": 4096}}]
    res = gen.generate(_fleet(RICH_SITES), start=START, n_samples=180, seed=1, scenarios=scen)
    con = duckdb.connect()
    con.register("vm_metrics", res.metrics)
    con.register("app_events", res.events)
    con.register("sites", res.sites)
    tools = copilot.LakeTools(con=con, dc=DC)
    out = tools.get_alerts(asof_min=30)
    assert "location_type" in out and "region" in out
    # at least one known site/region label appears in the enriched output
    assert any(r in out for r in ("NA", "EMEA", "DACH"))
