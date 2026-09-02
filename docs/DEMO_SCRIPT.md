# Demo script — Predictive AIOps Lakehouse on AIStor + OpenShift

A ~12-minute live walkthrough. Every command below is real and verified against
the lab AIStor + the in-cluster Ollama model. Pair it with the narrative in
[`STORY_TO_TELL.md`](STORY_TO_TELL.md); the copilot internals are in
[`COPILOT_FLOW.md`](COPILOT_FLOW.md).

> Convention: **SAY** = the point to make out loud. **SHOW** = what to point at.
> `make` targets auto-use the project `.venv`. Run from the repo root.

---

## 0. Before the audience arrives (T-15 min)

Get the slow / flaky things done up front so the live run is smooth:

```bash
# 1. AIStor wired + lake reachable
./bin/01_setup.sh            # idempotent; creates telemetry-raw bucket + warehouse
make lake-info               # confirms the Iceberg tables answer

# 2. Sovereign model up on the cluster with a tool-calling model pulled
oc get pods -l app=ollama    # Running?
curl -s https://<ollama-route>/api/tags   # qwen2.5:3b-instruct present?

# 3. config.ini points at real values (gitignored):
#    [minio]/[iceberg] -> your AIStor; [model] base_url -> the Ollama Route,
#    model_name = qwen2.5:3b-instruct

# 4. (optional) pre-warm the model so the first answer isn't a cold load
make chat ARGS='"warm up" --backend ollama --asof 15' >/dev/null 2>&1 || true
```

Decide your scale: **200 VMs** streams + ingests in well under a minute and is
plenty for the story. (Everything holds at 2,000 — detection stays ~1s — but
ingest is one snapshot per chunk, so a bigger burst just takes longer.)

Optional clean slate (so row counts are exact on stage): see
[§ Reset](#reset-for-a-clean-run).

---

## 1. Frame it (0:30)

**SAY:** "2,000 VMs of an EHR fleet. The goal isn't a prettier dashboard — it's
catching an outage *before* it happens, on infrastructure we own, with the data
never leaving the cluster. Storage is MinIO AIStor; the tables are Iceberg;
detection is deterministic SQL; the copilot is a small model we host ourselves."

**SHOW:** the architecture diagram in `PLAN.md` (or just talk).

---

## 2. It's a real lakehouse, cheaply (1:30)

```bash
make gen ARGS='--max-vms 200 --stream --upload --stream-interval-s 5'
```
Streams telemetry chunks straight into `s3://telemetry-raw` on AIStor, one every
5 seconds — simulating 2,000 agents shipping without running 2,000 agents.

**SAY (the licensing math):** "2,000 VMs × 30-second samples × ~12 metrics is
~5.7M rows an hour — a few GB a day of compressed Parquet. That's the entire
cost argument versus per-GB Datadog/Splunk licensing: AIStor is the cheap, tiered
SIEM/observability store."

**SHOW:** the chunks landing — `mc ls --recursive aiops/telemetry-raw | tail`.
(Ctrl-C the stream after a few chunks, or let it finish; use
`--stream-interval-s 0` to just blast it.)

---

## 3. Governed tables + an audit trail (1:30)

```bash
make load          # raw Parquet -> Iceberg, exactly-once, one snapshot per chunk
make lake-info     # tables, row counts, last 10 snapshots, a time-travel query
```

**SAY (HIPAA / audit):** "Every append is one Iceberg snapshot — an immutable,
time-stamped audit trail. I can query the fleet *as it was* at any past snapshot.
For a healthcare SIEM, that reproducibility is the compliance story." Re-run
`make load` — **SAY** "and it's exactly-once; re-processing the same chunks adds
nothing."

**SHOW:** the snapshot list and the `time-travel: as-of snapshot … -> N rows`
line.

---

## 4. The money shot — predict the OOM 45 min early (3:00)

```bash
make replay SOURCE=iceberg
```
Runs the detector as-of 45 / 30 / 15 / 0 minutes before the data end.

**SAY (deterministic-before-AI):** "This is plain SQL — regression on the memory
trend per VM — not a model. Watch the OOM ETA shrink: ~50 → ~31 → ~12 → ~3
minutes, and the severity auto-escalates P2 → P1 as it crosses 45 minutes. The
P1 fired **45+ minutes before** the crash — *while every ordinary dashboard still
showed green*, because threshold monitoring only alarms after a red line. We
caught the **trend**. That lead time is the whole product."

**SHOW:** read the ETA column top-to-bottom.

**Honest aside if asked:** "The leak is injected, so we know the right answer and
can prove the detector nails it. Phase 2 pulls the same lever on a real VM."

---

## 5. See it — the dashboard (2:00)

```bash
make dashboard SOURCE=iceberg      # opens http://localhost:8501
```

**SHOW:**
- **Fleet grid** — 200 VMs, the leaking patient-onboarding VMs red, postgres amber.
- Drag the **"As of" slider** 45 → 30 → 15 → 0: watch the grid light up and the
  **heap chart** climb toward the red 4096 MB ceiling.

**SAY (auto-resolving alerts):** "Slide toward the end and watch billing and
postgres go back to **green** — their incidents were transient and recovered. The
board shows *live state*, not stale pages. The one alert still standing —
patient-onboarding's leak — is the one an SRE must act on." (Same Iceberg tables
as the detector; the dashboard is just the window.)

---

## 6. Ask the lake — the sovereign copilot (2:30)

```bash
make chat ARGS='"why is patient onboarding slow?" --backend ollama --asof 15 --show-tools'
```

**SHOW:** the `· tool run_sql(...)` and `· tool get_alerts(...)` lines, then the
grounded answer citing real leak rates and OOM ETAs.

**SAY (sovereignty):** "That's a 3-billion-parameter open model running **on this
OpenShift cluster** — no API key, no telemetry leaving the cluster. It didn't
memorize the fleet; it **chose to query** the same governed lake, then explained
what it found."

**SAY (trust / can't hallucinate):** "And it *can't* invent metrics. If a model
answers without querying, we throw its numbers away and either feed it the real
rows to summarize or fall back to deterministic SQL." Demonstrate the guarantee:

```bash
make chat ARGS='"why is patient onboarding slow?" --backend none --asof 15'
```
**SAY:** "Same grounded answer with **no model at all** — pure SQL. The demo never
dies, and neither does an on-call engineer's trust."

*(Optional, if you have an ANTHROPIC_API_KEY set — the "augment later" beat:)*
```bash
make chat ARGS='"why is patient onboarding slow?" --backend both --asof 15'
```
**SAY:** "Sovereign local model first, then Claude side-by-side — start sovereign,
augment when policy allows."

---

## 6b. Blast radius — the topology graph (optional, +1:00)

**SAY (from symptom to root cause):** "The copilot also knows the *service graph*.
So it can answer the question every incident bridge asks: what's the blast radius?"

```bash
make chat ARGS='"what happens if postgres-db goes down?" --backend ollama --show-tools'
```
**SHOW:** the model calling **`· tool trace_dependencies(...)`**, then the impact
tree with depths and each hop's live alerts:
```
depth 1: ehr-api, billing-svc
depth 2: patient-onboarding  ALERTS: P1:heap_leak
depth 3: hl7-ingest
```
**SAY:** "postgres-db failing ripples through ehr-api to patient-onboarding — and
that hop is *already* showing a P1. The graph ties a root-cause dependency to the
user-facing service, with real alert state on every node."

Reverse it — connect a slow service back to its failing dependency:
```bash
make chat ARGS='"what does patient-onboarding depend on?" --backend none'
```
**SHOW:** upstream → ehr-api → **postgres-db (P2:io_spike)**. **SAY:** "Same graph,
other direction: onboarding is slow because the database it depends on two hops
down is saturating."

**SAY (architecture):** "This is the customer-story graph layer — traversal over
the *same* Iceberg tables, no separate graph database. Recursive SQL by default;
`docs/puppygraph.md` shows the same queries in Cypher via PuppyGraph if you want
a dedicated engine."

*(Instant/offline variant for a guaranteed-fast answer on stage: swap
`--backend ollama` for `--backend none`.)*

---

## 7. Close (0:30)

**SAY:** "Cheap governed storage, deterministic prediction with 45 minutes of lead
time, and a self-hosted copilot that grounds every claim in the data — one
lakehouse, no data egress. Phase 2 swaps the synthetic generator for real
OpenShift Virtualization VMs shipping into the **same** Iceberg tables; nothing
downstream changes. That's when 'demo' becomes 'production'."

---

## Timing summary

| # | Beat | Command | Time |
|---|---|---|---|
| 1 | Frame | — | 0:30 |
| 2 | Stream to AIStor | `make gen ARGS='--max-vms 200 --stream --upload ...'` | 1:30 |
| 3 | Ingest + snapshots | `make load` / `make lake-info` | 1:30 |
| 4 | Replay (money shot) | `make replay SOURCE=iceberg` | 3:00 |
| 5 | Dashboard | `make dashboard SOURCE=iceberg` | 2:00 |
| 6 | Copilot (sovereign) | `make chat ARGS='... --backend ollama --show-tools'` | 2:30 |
| 6b | Blast radius (optional) | `make chat ARGS='"what happens if postgres-db goes down?" ...'` | +1:00 |
| 7 | Close | — | 0:30 |
| | **Total** | | **~11:30** (12:30 with 6b) |

---

## Talking-points bank (drop in as questions come)

- **Licensing:** ~5.7M rows/hr, a few GB/day compressed → replaces per-GB
  SIEM licensing. AIStor tiers cold data cheaply.
- **Sovereignty:** local model (Ollama today, RHOAI/vLLM Granite for the
  supported path — see `OLLAMA_DEPLOY.md`). Telemetry never leaves the cluster.
- **Deterministic-before-AI:** SQL regression/z-scores are auditable,
  reproducible, cheap at 2,000-VM scale, and run in ~1s. The model explains and
  correlates; it does **not** alert.
- **Iceberg audit trail (HIPAA):** one snapshot per append; time-travel to any
  past state; exactly-once ingestion.
- **Grounding:** the copilot cannot show an invented metric (forced tool call +
  "did it query?" guard + deterministic fallback).

---

## Reset for a clean run

Exact row counts on stage (drops + reloads the tables):

```bash
.venv/bin/python - <<'PY'
from aiops.config import load_config
from aiops import ingest
from pyiceberg.exceptions import NoSuchTableError
cfg = load_config(); cat = ingest.iceberg_catalog(cfg)
for t in ("vm_metrics","app_events","alerts"):
    try: cat.drop_table(("observability",t)); print("dropped", t)
    except NoSuchTableError: pass
client, bucket = ingest.minio_client(cfg)
for o in client.list_objects(bucket, recursive=True):
    if o.object_name.startswith(("vm_metrics/","app_events/","_manifest/")):
        client.remove_object(bucket, o.object_name)
print("cleared raw chunks + manifest")
PY
# then re-run steps 2-3
```

---

## Troubleshooting appendix

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError` running `bin/*.py` | used system `python3`, not the venv | use `make …`, or `.venv/bin/python bin/…` |
| PyIceberg write fails `AccessDenied: headers … not signed` | AIStor sticky cookie breaks SigV4 | already handled (`iceberg_catalog` blocks cookies); if you hand-roll a catalog, block cookies too |
| `mc table warehouse create` / bucket `AccessDenied` | using a data-only user | use the admin key (`aiops`), not `devuser` |
| TLS cert errors to AIStor / Ollama | self-signed lab route | pass `--insecure` (detect/ingest/copilot), or `mc --insecure` |
| Copilot answer tagged `· fallback` | model unreachable or didn't tool-call | check `base_url`/route + `ollama list`; small models fall back by design |
| Copilot cites suspiciously round numbers | a weak model hallucinating | it won't reach you — the grounding guard replaces it; prefer `qwen2.5:3b-instruct` |
| Dashboard shows stale data after a reload | cached lake pull | click **↻ Reload from lake** in the sidebar |
| `mc: command not found` / old `mc` | Iceberg Tables need mc ≥ RELEASE.2026-02-03 | update `mc` |
| Replay/copilot slow on iceberg | each pass re-scans the tables | use `SOURCE=local` for a snappier offline run |
