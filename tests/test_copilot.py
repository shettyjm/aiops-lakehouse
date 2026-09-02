"""Tests for the M4 copilot: read-only SQL guard, get_alerts, the model-free
deterministic RCA, the fallback wrapper, and the OpenAI tool-loop wiring.

No live model is needed: the loop is exercised with a fake OpenAI client, and
the fallback is exercised by forcing the backend to raise.
"""

from datetime import datetime, timezone

import duckdb
import pytest

from aiops import copilot, detect, generator as gen

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
DC = detect.DetectConfig(heap_cap_mb=4096, io_z=4, retrans_pct=5, latency_x=2)


def _tools_with_leak():
    base = {"cpu_pct": 30, "heap_mb": 1800, "gc_pause_ms": 30, "disk_io_await_ms": 8,
            "disk_iops": 1500, "net_mbps": 100, "net_retrans_pct": 0.3,
            "app_latency_ms": 80, "error_rate_pct": 0.2}
    fleet = {"sample_interval_s": 30, "noise_pct": 0.05, "sites": ["dc-east", "dc-west"],
             "apps": {"patient-onboarding": {"count": 3, "baseline": dict(base)},
                      "ehr-api": {"count": 3, "baseline": dict(base)}}}
    scen = [{"name": "leak", "template": "heap_leak", "app": "patient-onboarding",
             "count": 2, "start_min": 5, "end_min": 120,
             "params": {"leak_mb_per_min": 22, "heap_cap_mb": 4096}}]
    res = gen.generate(fleet, scen, start=START, n_samples=240, seed=1)
    con = duckdb.connect()
    con.register("vm_metrics", res.metrics)
    con.register("app_events", res.events)
    return copilot.LakeTools(con=con, dc=DC), res


# --- run_sql guard --------------------------------------------------------- #
def test_run_sql_allows_select():
    tools, _ = _tools_with_leak()
    out = tools.run_sql("SELECT count(*) AS n FROM vm_metrics")
    assert "rows: 1" in out


@pytest.mark.parametrize("bad", [
    "DROP TABLE vm_metrics",
    "INSERT INTO vm_metrics VALUES (1)",
    "UPDATE vm_metrics SET cpu_pct=0",
    "SELECT 1; DROP TABLE vm_metrics",
    "PRAGMA database_list",
    "COPY vm_metrics TO 'x.csv'",
])
def test_run_sql_blocks_writes(bad):
    tools, _ = _tools_with_leak()
    with pytest.raises(copilot.ReadOnlySQLError):
        tools.run_sql(bad)


def test_tool_call_wraps_errors():
    tools, _ = _tools_with_leak()
    # dispatch returns an ERROR string rather than raising, so the model can react
    assert tools.call("run_sql", {"sql": "DROP TABLE x"}).startswith("ERROR")
    assert tools.call("bogus", {}).startswith("unknown tool")


# --- get_alerts ------------------------------------------------------------ #
def test_get_alerts_filters():
    tools, res = _tools_with_leak()
    all_txt = tools.get_alerts(asof_min=30)
    assert "heap_leak" in all_txt
    p1 = tools.get_alerts(severity="P1", asof_min=30)
    assert "P2" not in p1.split("\n", 1)[1] if "\n" in p1 else True


# --- deterministic RCA ----------------------------------------------------- #
def test_deterministic_answer_is_grounded():
    tools, res = _tools_with_leak()
    ans = copilot.deterministic_answer("why is patient onboarding slow?", tools, asof_min=30)
    assert "patient-onboarding" in ans
    assert "heap" in ans.lower()
    assert "OOM ETA" in ans
    # cites a leaking VM that the detector also flags
    leak_vms = res.incident_vms["leak"]
    assert any(v in ans for v in leak_vms)


def test_target_app_from_question_and_default():
    tools, _ = _tools_with_leak()
    assert copilot._target_app("what about EHR-API?", tools) == "ehr-api"
    # no app named -> falls back to worst-latency app (the leaking one)
    assert copilot._target_app("what is broken?", tools) == "patient-onboarding"


def test_answer_none_uses_deterministic():
    tools, _ = _tools_with_leak()
    res = copilot.answer("why is patient onboarding slow?", tools, cfg=None,
                         backend="none", asof_min=30)
    assert res.backend == "none" and res.used_fallback
    assert "patient-onboarding" in res.text


def test_answer_falls_back_when_backend_raises(monkeypatch):
    tools, _ = _tools_with_leak()
    monkeypatch.setattr(copilot, "_run_openai",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no endpoint")))
    res = copilot.answer("why is patient onboarding slow?", tools, cfg=_FakeCfg(),
                         backend="ollama", asof_min=30)
    assert res.used_fallback
    assert "deterministic plan" in res.text
    assert "patient-onboarding" in res.text


# --- OpenAI tool-loop wiring (fake client) --------------------------------- #
class _FakeFn:
    def __init__(self, name, args): self.name = name; self.arguments = args
class _FakeTC:
    def __init__(self, name, args): self.id = "tc1"; self.function = _FakeFn(name, args)
class _FakeMsg:
    def __init__(self, content=None, tool_calls=None):
        self.content = content; self.tool_calls = tool_calls
class _FakeResp:
    def __init__(self, msg): self.choices = [type("C", (), {"message": msg})]
class _FakeCompletions:
    def __init__(self, script): self.script = script; self.i = 0
    def create(self, **kw):
        r = self.script[self.i]; self.i += 1; return r
class _FakeClient:
    def __init__(self, script):
        self.chat = type("Chat", (), {"completions": _FakeCompletions(script)})()


class _FakeCfg:
    def get(self, section, key, fallback=None):
        return {"base_url": "http://x/v1", "api_key": "", "model_name": "m"}.get(key, fallback)


def test_openai_loop_executes_tools(monkeypatch):
    tools, _ = _tools_with_leak()
    # Script: first a tool call to run_sql, then a final text answer.
    script = [
        _FakeResp(_FakeMsg(tool_calls=[_FakeTC("run_sql",
                  '{"sql": "SELECT count(*) AS n FROM vm_metrics"}')])),
        _FakeResp(_FakeMsg(content="There are rows in the fleet.")),
    ]
    import openai
    monkeypatch.setattr(openai, "OpenAI", lambda **kw: _FakeClient(script))
    res = copilot._run_openai(_FakeCfg(), "how many rows?", tools)
    assert res.text == "There are rows in the fleet."
    assert res.tool_calls == [("run_sql", {"sql": "SELECT count(*) AS n FROM vm_metrics"})]
