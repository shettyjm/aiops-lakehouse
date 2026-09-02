"""Sovereign SRE copilot over the telemetry lake (M4).

A tool-using agent that answers plain-English questions ("why is patient
onboarding slow?") grounded in the Iceberg/parquet lake. Two read-only tools:
run_sql (DuckDB) and get_alerts (the M3 detector). Backends:

  * ollama / openshift_ai : OpenAI-compatible chat.completions with tool calling
  * claude                : Anthropic SDK
  * none                  : no model — a deterministic SQL root-cause narrative

The deterministic path is first-class: if no model is reachable, or a small
model can't produce a tool call, the copilot still answers with real numbers.
Every backend shares one system prompt: ground every claim in query results,
cite numbers, never invent data.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

import duckdb
import pandas as pd

from aiops import detect, ingest
from aiops.config import get_bool

SCHEMA_DOC = """\
Tables (DuckDB, read-only):
  vm_metrics(ts TIMESTAMP, vm_id, app, site, cpu_pct, heap_mb, gc_pause_ms,
             disk_io_await_ms, disk_iops, net_mbps, net_retrans_pct,
             app_latency_ms, error_rate_pct)
  app_events(ts, vm_id, app, site, level, event_type, message)
Apps: patient-onboarding, ehr-api, scheduler-svc, billing-svc, hl7-ingest,
      postgres-db, kafka, generic-worker. Sites: dc-east, dc-west.
Samples are every 30s. heap_mb approaches a ~4096 MB cap before an OOM."""

SYSTEM_PROMPT = f"""You are an SRE copilot for a healthcare EHR SaaS running on a \
MinIO AIStor + OpenShift lakehouse. You help on-call engineers triage fleet \
telemetry. Rules:
- Ground EVERY claim in query results. Cite concrete numbers (ms, MB, %, counts).
- Never invent data. If a query returns nothing, say so.
- Prefer the tools over guessing. Use run_sql for metrics, get_alerts for alerts.
- Be concise and operational: state the likely root cause and a recommended action.

{SCHEMA_DOC}"""

APPS = ["patient-onboarding", "ehr-api", "scheduler-svc", "billing-svc",
        "hl7-ingest", "postgres-db", "kafka", "generic-worker"]

# read_sql guard: only single read-only SELECT/WITH statements.
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|create|alter|attach|copy|pragma|install|"
    r"load|export|call|set|vacuum|analyze)\b", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# Tools
# --------------------------------------------------------------------------- #
class ReadOnlySQLError(ValueError):
    pass


@dataclass
class LakeTools:
    con: duckdb.DuckDBPyConnection
    dc: detect.DetectConfig
    max_rows: int = 50

    def run_sql(self, sql: str) -> str:
        """Execute a read-only SELECT/WITH query; return a compact text table."""
        clean = sql.strip().rstrip(";").strip()
        if ";" in clean:
            raise ReadOnlySQLError("only a single statement is allowed")
        low = clean.lower()
        if not (low.startswith("select") or low.startswith("with")):
            raise ReadOnlySQLError("only SELECT/WITH queries are allowed")
        if _FORBIDDEN.search(clean):
            raise ReadOnlySQLError("write/DDL keywords are not allowed")
        df = self.con.execute(clean).fetch_df()
        n = len(df)
        body = df.head(self.max_rows).to_string(index=False)
        note = "" if n <= self.max_rows else f"\n... ({n} rows, showing {self.max_rows})"
        return f"rows: {n}\n{body}{note}"

    def get_alerts(self, severity: str | None = None, app: str | None = None,
                   asof_min: float = 0.0) -> str:
        """Return current alerts (from the M3 detector), optionally filtered."""
        alerts = detect.detect(self.con, self.dc, asof_min=asof_min)
        if severity:
            alerts = alerts[alerts["severity"] == severity.upper()]
        if app:
            alerts = alerts[alerts["app"] == app]
        if alerts.empty:
            return "no active alerts"
        cols = ["severity", "rule", "vm_id", "app", "headline"]
        return f"{len(alerts)} alert(s)\n{alerts[cols].to_string(index=False)}"

    # dispatch used by the agent loop
    def call(self, name: str, args: dict) -> str:
        try:
            if name == "run_sql":
                return self.run_sql(args["sql"])
            if name == "get_alerts":
                return self.get_alerts(args.get("severity"), args.get("app"),
                                       float(args.get("asof_min", 0)))
            return f"unknown tool: {name}"
        except Exception as e:  # tool errors go back to the model, not the user
            return f"ERROR: {type(e).__name__}: {e}"


TOOL_DEFS = [
    {"name": "run_sql",
     "description": "Run a read-only DuckDB SELECT/WITH query over vm_metrics / "
                    "app_events and return rows. Aggregate; don't select millions of rows.",
     "parameters": {"type": "object",
                    "properties": {"sql": {"type": "string",
                                           "description": "a single SELECT/WITH query"}},
                    "required": ["sql"]}},
    {"name": "get_alerts",
     "description": "Return current detector alerts, optionally filtered by "
                    "severity (P1/P2) or app.",
     "parameters": {"type": "object",
                    "properties": {"severity": {"type": "string"},
                                   "app": {"type": "string"}},
                    "required": []}},
]


def openai_tools():
    return [{"type": "function", "function": t} for t in TOOL_DEFS]


def anthropic_tools():
    return [{"name": t["name"], "description": t["description"],
             "input_schema": t["parameters"]} for t in TOOL_DEFS]


# --------------------------------------------------------------------------- #
# Lake connection helper (mirrors detect's, plus a DetectConfig)
# --------------------------------------------------------------------------- #
def open_lake(cfg, source: str, data_dir: str, namespace: str, insecure: bool):
    if source == "iceberg":
        con = detect.duckdb_from_iceberg(cfg, namespace, insecure=insecure)
    else:
        con = detect.duckdb_from_local(data_dir)
    return LakeTools(con=con, dc=detect.DetectConfig.from_ini(cfg))


# --------------------------------------------------------------------------- #
# Deterministic (model-free) root-cause narrative — the robust fallback
# --------------------------------------------------------------------------- #
def _target_app(question: str, tools: LakeTools) -> str:
    q = question.lower()
    for app in APPS:
        if app in q or app.replace("-", " ") in q:
            return app
    # else: the app with the highest recent p95 latency
    df = tools.con.execute("""
        SELECT app, quantile_cont(app_latency_ms, 0.95) p95
        FROM vm_metrics
        WHERE ts > (SELECT max(ts) FROM vm_metrics) - INTERVAL 10 MINUTE
        GROUP BY app ORDER BY p95 DESC LIMIT 1""").fetchone()
    return df[0] if df else APPS[0]


def _gather(question: str, tools: LakeTools, asof_min: float = 0.0) -> dict:
    """Run the fixed RCA SQL plan and return the raw facts (shared by the
    deterministic narrative and the model-summarise fallback)."""
    from datetime import timedelta
    app = _target_app(question, tools)
    t_end = detect.evaluation_end(tools.con, asof_min)
    end = detect._ts_literal(t_end)
    rlo = detect._ts_literal(t_end - timedelta(minutes=10))
    t_start = detect.data_start(tools.con)
    blo, bhi = detect._baseline_window(t_start, t_end)

    lat = tools.con.execute(f"""
        SELECT
          (SELECT quantile_cont(app_latency_ms,0.95) FROM vm_metrics
             WHERE app='{app}' AND ts>{rlo} AND ts<={end}) AS p95_now,
          (SELECT quantile_cont(app_latency_ms,0.95) FROM vm_metrics
             WHERE app='{app}' AND ts>={blo} AND ts<{bhi}) AS p95_base
    """).fetchone()
    worst = tools.con.execute(f"""
        SELECT vm_id,
               regr_slope(heap_mb, epoch(ts)/60.0) heap_slope,
               arg_max(heap_mb, ts) heap_now,
               avg(gc_pause_ms) gc,
               avg(disk_io_await_ms) io_await,
               avg(app_latency_ms) latency
        FROM vm_metrics
        WHERE app='{app}' AND ts>{rlo} AND ts<={end}
        GROUP BY vm_id ORDER BY latency DESC LIMIT 3""").fetch_df()
    alerts = detect.detect(tools.con, tools.dc, asof_min=asof_min)
    app_alerts = alerts[alerts["app"] == app] if not alerts.empty else alerts
    return {"app": app, "t_end": t_end, "p95_now": (lat[0] or 0.0),
            "p95_base": (lat[1] or 0.0), "worst": worst, "app_alerts": app_alerts}


def facts_text(g: dict) -> str:
    """Compact real-data block to hand a model that wouldn't call the tools."""
    cols = ["severity", "rule", "vm_id", "app", "headline"]
    al = g["app_alerts"]
    return "\n".join([
        f"app: {g['app']}",
        f"app_latency_ms p95: now={g['p95_now']:.0f} ms, baseline={g['p95_base']:.0f} ms",
        "worst VMs (recent 10 min) — heap_slope in MB/min, heap_now in MB, "
        "gc/io_await/latency in ms:",
        g["worst"].to_string(index=False),
        "active alerts:",
        (al[cols].to_string(index=False) if not al.empty else "none"),
    ])


def deterministic_answer(question: str, tools: LakeTools, asof_min: float = 0.0) -> str:
    """A grounded RCA built purely from SQL — no model required."""
    return _narrative(_gather(question, tools, asof_min), tools.dc)


def _narrative(g: dict, dc: detect.DetectConfig) -> str:
    app, t_end = g["app"], g["t_end"]
    p95_now, p95_base = g["p95_now"], g["p95_base"]
    worst, app_alerts = g["worst"], g["app_alerts"]
    lines = [f"## {app}: root-cause summary (as of {t_end:%H:%M:%S})", ""]
    if p95_base > 0 and p95_now > p95_base * 1.3:
        lines.append(f"- **Latency is elevated**: p95 {p95_now:.0f} ms vs baseline "
                     f"{p95_base:.0f} ms ({p95_now/p95_base:.1f}x).")
    else:
        lines.append(f"- Latency p95 is {p95_now:.0f} ms "
                     f"(baseline {p95_base:.0f} ms) — near normal.")

    if not worst.empty:
        w = worst.iloc[0]
        detail = []
        if w.heap_slope > 8 and w.heap_now > dc.heap_cap_mb * 0.6:
            eta = (dc.heap_cap_mb - w.heap_now) / w.heap_slope
            detail.append(f"heap climbing +{w.heap_slope:.0f} MB/min "
                          f"(now {w.heap_now:.0f} MB, OOM ETA ~{eta:.0f} min)")
        if w.gc > 80:
            detail.append(f"GC pauses ~{w.gc:.0f} ms")
        if w.io_await > 30:
            detail.append(f"disk await ~{w.io_await:.0f} ms")
        cause = "; ".join(detail) if detail else "no single dominant metric"
        lines.append(f"- **Worst VM {w.vm_id}**: latency ~{w.latency:.0f} ms — {cause}.")

    if not app_alerts.empty:
        by = app_alerts["severity"].value_counts().to_dict()
        lines.append(f"- **Active alerts**: " +
                     ", ".join(f"{v}x {k}" for k, v in by.items()) +
                     " — " + "; ".join(app_alerts["rule"].unique()))
    else:
        lines.append("- No active alerts for this app.")

    # opinionated conclusion
    if not app_alerts.empty and (app_alerts["rule"] == "heap_leak").any():
        lines += ["", "**Likely cause:** a JVM heap leak driving GC pressure and "
                  "latency toward an OOM. **Action:** capture a heap dump and schedule "
                  "a rolling restart before the ETA."]
    elif not worst.empty and worst.iloc[0].io_await > 30:
        lines += ["", "**Likely cause:** downstream disk/IO saturation. **Action:** "
                  "check the storage backend and slow queries."]
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Model backends
# --------------------------------------------------------------------------- #
@dataclass
class AgentResult:
    text: str
    tool_calls: list = field(default_factory=list)
    backend: str = ""
    used_fallback: bool = False


class BackendError(RuntimeError):
    pass


def _run_openai(cfg, question, tools: LakeTools, model=None, max_steps=6) -> AgentResult:
    from openai import OpenAI
    base_url = cfg.get("model", "base_url")
    api_key = cfg.get("model", "api_key", fallback="") or "not-needed"
    model = model or cfg.get("model", "model_name")
    client = OpenAI(base_url=base_url, api_key=api_key)

    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": question}]
    calls = []
    for step in range(max_steps):
        # Force a tool call on the first step so small models must ground their
        # answer in the lake instead of hallucinating numbers; then let them
        # summarise. Some servers reject tool_choice="required" -> fall back.
        tc = "required" if step == 0 else "auto"
        try:
            resp = client.chat.completions.create(
                model=model, messages=messages, tools=openai_tools(),
                tool_choice=tc, temperature=0)
        except Exception:
            if tc == "required":
                resp = client.chat.completions.create(
                    model=model, messages=messages, tools=openai_tools(),
                    tool_choice="auto", temperature=0)
            else:
                raise
        msg = resp.choices[0].message
        if not msg.tool_calls:
            return AgentResult(text=msg.content or "", tool_calls=calls, backend="openai")
        messages.append({"role": "assistant", "content": msg.content or "",
                         "tool_calls": [{"id": tc.id, "type": "function",
                                         "function": {"name": tc.function.name,
                                                      "arguments": tc.function.arguments}}
                                        for tc in msg.tool_calls]})
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments or "{}")
            result = tools.call(tc.function.name, args)
            calls.append((tc.function.name, args))
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
    return AgentResult(text="(stopped: max tool steps reached)", tool_calls=calls,
                       backend="openai")


def _run_claude(cfg, question, tools: LakeTools, model="claude-sonnet-5",
                max_steps=6) -> AgentResult:
    import anthropic
    api_key = cfg.get("model", "api_key", fallback="") or None
    client = anthropic.Anthropic(api_key=api_key)   # else ANTHROPIC_API_KEY env
    messages = [{"role": "user", "content": question}]
    calls = []
    for _ in range(max_steps):
        resp = client.messages.create(model=model, system=SYSTEM_PROMPT,
                                      tools=anthropic_tools(), messages=messages,
                                      max_tokens=1024)
        if resp.stop_reason != "tool_use":
            text = "".join(b.text for b in resp.content if b.type == "text")
            return AgentResult(text=text, tool_calls=calls, backend="claude")
        messages.append({"role": "assistant", "content": resp.content})
        results = []
        for block in resp.content:
            if block.type == "tool_use":
                result = tools.call(block.name, dict(block.input))
                calls.append((block.name, dict(block.input)))
                results.append({"type": "tool_result", "tool_use_id": block.id,
                                "content": result})
        messages.append({"role": "user", "content": results})
    return AgentResult(text="(stopped: max tool steps reached)", tool_calls=calls,
                       backend="claude")


SUMMARY_SYSTEM = (
    "You are an SRE for a healthcare EHR SaaS. Summarize the query results below "
    "for the on-call engineer. Use ONLY the numbers provided — never invent data. "
    "State the likely root cause and a recommended action, concisely.")


def _summarize_with_model(cfg, backend: str, question: str, g: dict) -> str:
    """Have the model narrate REAL query results (used when it won't tool-call)."""
    prompt = f"{question}\n\nQuery results from the lake:\n{facts_text(g)}"
    if backend == "claude":
        import anthropic
        client = anthropic.Anthropic(api_key=cfg.get("model", "api_key", fallback="") or None)
        resp = client.messages.create(
            model="claude-sonnet-5", system=SUMMARY_SYSTEM, max_tokens=512,
            messages=[{"role": "user", "content": prompt}])
        return "".join(b.text for b in resp.content if b.type == "text")
    from openai import OpenAI
    client = OpenAI(base_url=cfg.get("model", "base_url"),
                    api_key=cfg.get("model", "api_key", fallback="") or "not-needed")
    resp = client.chat.completions.create(
        model=cfg.get("model", "model_name"), temperature=0,
        messages=[{"role": "system", "content": SUMMARY_SYSTEM},
                  {"role": "user", "content": prompt}])
    return resp.choices[0].message.content or ""


def answer(question: str, tools: LakeTools, cfg, backend: str,
           asof_min: float = 0.0) -> AgentResult:
    """Answer via the chosen backend, falling back to the deterministic RCA if
    the model is unreachable or produces no grounded answer."""
    if backend == "none":
        return AgentResult(text=deterministic_answer(question, tools, asof_min),
                           backend="none", used_fallback=True)
    try:
        if backend in ("ollama", "openshift_ai"):
            res = _run_openai(cfg, question, tools)
        elif backend == "claude":
            res = _run_claude(cfg, question, tools)
        else:
            raise BackendError(f"unknown backend: {backend}")
        if not res.text.strip():
            raise BackendError("empty model response")
        # Grounding guard: if the model never queried the lake, its own numbers
        # aren't trustworthy (small models hallucinate). Fetch the real rows and
        # have it SUMMARISE those instead; if that fails, use the SQL narrative.
        if not res.tool_calls:
            g = _gather(question, tools, asof_min)
            try:
                summary = _summarize_with_model(cfg, backend, question, g)
                if summary.strip():
                    return AgentResult(
                        text=summary + f"\n\n_({backend} summarised real query "
                             f"results — it didn't tool-call, so the lake was "
                             f"queried for it.)_",
                        backend=backend, used_fallback=True)
            except Exception:
                pass
            return AgentResult(
                text=_narrative(g, tools.dc) + "\n\n_(model didn't query the lake; "
                     "deterministic SQL result.)_",
                backend=backend, used_fallback=True)
        return res
    except Exception as e:
        det = deterministic_answer(question, tools, asof_min)
        return AgentResult(
            text=det + f"\n\n_(model backend '{backend}' unavailable: "
                       f"{type(e).__name__}; answered from the deterministic plan)_",
            backend=backend, used_fallback=True)
