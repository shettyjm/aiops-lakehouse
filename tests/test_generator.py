"""Tests for the M1 telemetry generator.

Covers the acceptance checks: row counts, incident VMs show the injected
pattern, and non-incident VMs stay within baseline bands. Uses a small fleet
and short window so the suite stays fast; the vectorised code path is identical
at 2,000 VMs.
"""

from datetime import datetime, timezone

import numpy as np
import pytest

from aiops import generator as gen

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def small_fleet():
    return {
        "sample_interval_s": 30,
        "noise_pct": 0.05,
        "sites": ["dc-east", "dc-west"],
        "apps": {
            "patient-onboarding": {
                "count": 4,
                "baseline": {"cpu_pct": 35, "heap_mb": 1800, "gc_pause_ms": 40,
                             "disk_io_await_ms": 5, "disk_iops": 800, "net_mbps": 120,
                             "net_retrans_pct": 0.3, "app_latency_ms": 90,
                             "error_rate_pct": 0.2}},
            "postgres-db": {
                "count": 3,
                "baseline": {"cpu_pct": 50, "heap_mb": 800, "gc_pause_ms": 5,
                             "disk_io_await_ms": 8, "disk_iops": 2000, "net_mbps": 90,
                             "net_retrans_pct": 0.3, "app_latency_ms": 25,
                             "error_rate_pct": 0.1}},
            "generic-worker": {
                "count": 5,
                "baseline": {"cpu_pct": 20, "heap_mb": 900, "gc_pause_ms": 20,
                             "disk_io_await_ms": 4, "disk_iops": 300, "net_mbps": 40,
                             "net_retrans_pct": 0.2, "app_latency_ms": 50,
                             "error_rate_pct": 0.15}},
        },
    }


def test_row_count_and_columns(small_fleet):
    n_samples = 40
    res = gen.generate(small_fleet, [], start=START, n_samples=n_samples, seed=1)
    n_vms = 4 + 3 + 5
    assert len(res.metrics) == n_vms * n_samples
    for col in ("ts", "vm_id", "app", "site", *gen.METRICS):
        assert col in res.metrics.columns
    # Contract order / no NaN / non-negative.
    assert res.metrics[list(gen.METRICS)].notna().all().all()
    assert (res.metrics[list(gen.METRICS)] >= 0).all().all()


def test_determinism(small_fleet):
    a = gen.generate(small_fleet, [], start=START, n_samples=20, seed=7)
    b = gen.generate(small_fleet, [], start=START, n_samples=20, seed=7)
    assert a.metrics.equals(b.metrics)
    c = gen.generate(small_fleet, [], start=START, n_samples=20, seed=8)
    assert not a.metrics["heap_mb"].equals(c.metrics["heap_mb"])


def test_site_split_roundrobin(small_fleet):
    res = gen.generate(small_fleet, [], start=START, n_samples=5, seed=1)
    onboarding = res.metrics[res.metrics["app"] == "patient-onboarding"]
    sites = set(onboarding["site"].unique())
    assert sites == {"dc-east", "dc-west"}


def test_heap_leak_ramp_and_oom(small_fleet):
    scen = [{
        "name": "leak", "template": "heap_leak", "app": "patient-onboarding",
        "count": 2, "start_min": 5, "end_min": 60,
        "params": {"leak_mb_per_min": 100, "heap_cap_mb": 4096},
    }]
    n_samples = 120  # 60 min at 30s
    res = gen.generate(small_fleet, scen, start=START, n_samples=n_samples, seed=1)

    leak_vms = res.incident_vms["leak"]
    assert len(leak_vms) == 2

    m = res.metrics
    victim = m[m["vm_id"] == leak_vms[0]].sort_values("ts")
    heap = victim["heap_mb"].to_numpy()
    # Heap climbs well above baseline and is capped at the OOM ceiling.
    assert heap.max() >= 4000
    assert heap.max() <= 4096 + 1e-3
    # A non-incident onboarding VM stays near its 1800 MB baseline band.
    healthy_id = [v for v in m[m["app"] == "patient-onboarding"]["vm_id"].unique()
                  if v not in leak_vms][0]
    healthy = m[m["vm_id"] == healthy_id]
    assert healthy["heap_mb"].max() < 1800 * 1.3

    # OOM event emitted for the victims.
    ev = res.events
    ooms = ev[ev["event_type"] == "oom_kill"]
    assert set(ooms["vm_id"]).issubset(set(leak_vms))
    assert len(ooms) >= 1


def test_io_saturation_await_and_iops(small_fleet):
    scen = [{
        "name": "io", "template": "io_saturation", "app": "postgres-db",
        "count": 1, "start_min": 2, "end_min": 30,
        "params": {"await_ramp_ms_per_min": 5, "iops_collapse_frac": 0.8},
    }]
    res = gen.generate(small_fleet, scen, start=START, n_samples=60, seed=1)
    vm = res.incident_vms["io"][0]
    m = res.metrics
    victim = m[m["vm_id"] == vm].sort_values("ts")
    # await ramps far above the 8ms baseline; iops collapses below baseline.
    assert victim["disk_io_await_ms"].max() > 8 * 3
    assert victim["disk_iops"].min() < 2000 * 0.5
    assert (res.events["event_type"] == "slow_query").any()


def test_healthy_fleet_stays_in_band(small_fleet):
    """No scenarios -> every metric stays within its own noise band.

    Band = baseline +/- 6*sigma using the generator's actual noise model
    (std = max(noise_pct*base, floor)), so small-baseline metrics with an
    absolute noise floor aren't held to an unrealistic flat percentage.
    """
    res = gen.generate(small_fleet, [], start=START, n_samples=60, seed=3)
    m = res.metrics
    noise_pct = small_fleet["noise_pct"]
    for app, spec in small_fleet["apps"].items():
        sub = m[m["app"] == app]
        for metric, base in spec["baseline"].items():
            if base <= 0:
                continue
            std = max(base * noise_pct, gen._NOISE_FLOOR.get(metric, 0.0))
            hi = base + 6 * std
            lo = max(base - 6 * std, 0.0)
            assert sub[metric].max() <= hi, f"{app}.{metric} above band"
            assert sub[metric].min() >= lo, f"{app}.{metric} below band"


def test_scale_fleet_preserves_proportions_and_min_one():
    fleet = {"apps": {
        "big": {"count": 800, "baseline": {}},
        "mid": {"count": 180, "baseline": {}},
        "tiny": {"count": 20, "baseline": {}},
    }}
    total = gen.scale_fleet(fleet, 100)
    assert 90 <= total <= 110                      # ~100, rounding tolerated
    assert fleet["apps"]["big"]["count"] == 80     # 800/1000 * 100
    assert fleet["apps"]["tiny"]["count"] >= 1     # never zeroed out
    # Requesting more than present is a no-op.
    fleet2 = {"apps": {"a": {"count": 10, "baseline": {}}}}
    assert gen.scale_fleet(fleet2, 999) == 10


def test_events_contract(small_fleet):
    scen = [{
        "name": "storm", "template": "error_storm", "app": "generic-worker",
        "count": 2, "start_min": 1, "end_min": 20, "params": {"error_jump_pct": 6},
    }]
    res = gen.generate(small_fleet, scen, start=START, n_samples=50, seed=1)
    assert list(res.events.columns) == gen.EVENT_COLUMNS
    storm_vms = set(res.incident_vms["storm"])
    err = res.events[res.events["event_type"] == "http_5xx"]
    assert set(err["vm_id"]).issubset(storm_vms)
    # error_rate lifted on victims
    m = res.metrics
    victim = m[m["vm_id"] == res.incident_vms["storm"][0]]
    assert victim["error_rate_pct"].max() > 6
