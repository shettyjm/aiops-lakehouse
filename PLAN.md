# Predictive AIOps Lakehouse on MinIO AIStor + OpenShift
### Build plan for Claude Code (CLI) — Phase 1 (simulated 2,000 VMs) and Phase 2 (real VMs + synthetic scale)

> **How to use this file with the Claude CLI**
> 1. `mkdir aiops-lakehouse && cd aiops-lakehouse && git init`
> 2. Save this file as `PLAN.md` in the repo root, and create a `CLAUDE.md` from the
>    "Project conventions" section below.
> 3. Run `claude` in the repo. Work **one milestone per session**: paste the milestone's
>    *Claude CLI prompt* block verbatim, review the diff, run the *Acceptance test*, commit.
> 4. Each milestone is independent enough to demo on its own — stop anywhere and you still
>    have something to show a field architect.

---

## 0. Combined architecture (what we're merging)

Three inputs merged into one design:

1. **Our working prototype** (this chat): synthetic EHR fleet → Parquet → AIStor Tables
   (Iceberg, built-in REST catalog) → deterministic DuckDB detectors with OOM-ETA
   prediction → Claude SRE copilot with `run_sql` / `get_alerts` tools.
2. **MinIO customer story pattern** (`architecureevaluate.png`): NL query → LangChain
   (intent → routing) → **PuppyGraph** (Cypher over Iceberg, graph traversal of
   service/host topology) → Iceberg-in-AIStor as source of truth → result graph →
   AI summary / root-cause / automated resolution.
3. **Your requirements**: 2,000 VMs, AIStor as the SIEM/observability lakehouse
   (replace Datadog/Splunk per-GB licensing), **self-hosted small model for data
   sovereignty** (healthcare/EHR — telemetry never leaves the cluster), OpenShift SNO
   today → OpenShift Virtualization real VMs in Phase 2.

```
                        ┌─────────────────────────── Red Hat OpenShift ───────────────────────────┐
 2,000 VMs (apps+infra) │  MinIO AIStor                     Analytics            AI               │
 ┌─────────┐  events    │  ┌──────────────┐  ┌───────────┐  ┌──────────────┐  ┌────────────────┐  │
 │ APM logs├───────────►│  │ telemetry-raw│─►│ AIStor    │─►│ Deterministic│─►│ SRE Copilot    │  │
 │ metrics │  S3 PUT /  │  │ (Parquet, S3)│  │ Tables    │  │ detectors    │  │ self-hosted LLM│  │
 │ syslog  │  agents    │  └──────────────┘  │ (Iceberg, │  │ (DuckDB SQL) │  │ (Granite/Qwen  │  │
 └─────────┘            │   SIEM store,     │  built-in  │  │ OOM ETA, io  │  │  via Ollama /  │  │
                        │   cheap, tiered   │  REST cat.)│  │ z-score, net │  │  OpenShift AI) │  │
                        │                    └───────────┘  └──────┬───────┘  └───────┬────────┘  │
                        │                          ▲               │ alerts           │           │
                        │                    ┌─────┴──────┐        ▼                  ▼           │
                        │                    │ PuppyGraph │   Slack/webhook    "why is patient    │
                        │                    │ (topology, │   40+ min BEFORE    onboarding slow?" │
                        │                    │  Cypher)   │   the outage                          │
                        │                    └────────────┘   [optional M5, customer-story layer] │
                        └─────────────────────────────────────────────────────────────────────────┘
```

Design decisions locked in:
- **Deterministic detection first, LLM on top** — SQL regression/z-scores are auditable,
  reproducible, cheap at 2,000-VM scale. The model explains/correlates; it does not alert.
- **Iceberg is the contract** — generators, detectors, graph layer, and the LLM all read
  the same tables. Phase 2 swaps the *producers* only; everything downstream is untouched.
- **Sovereignty** — default copilot backend is a local model (Ollama or OpenShift AI /
  vLLM, OpenAI-compatible API). Claude API stays as an optional `--backend claude` flag
  for quality comparison in the demo ("start sovereign, augment later").
- **Scale math to say out loud**: 2,000 VMs × 30s samples × ~12 numeric metrics
  ≈ 5.7M rows/hour ≈ 130–140M rows/day ≈ **a few GB/day of compressed Parquet**.
  That is the entire licensing argument in one sentence.

---

## Project conventions (put this in `CLAUDE.md`)

```markdown
# CLAUDE.md
- Python 3.11+, single repo, one top-level package `aiops/`; runnable scripts in `bin/`.
- Config in `config.ini` (sections: [minio], [iceberg], [model], [detect]); never hardcode
  endpoints or credentials; support `--insecure` TLS for lab routes.
- Every component must run in TWO modes: `--source local` (parquet under ./data, no
  server needed) and `--source iceberg` (AIStor Tables via PyIceberg REST catalog).
- Data contract (do not change without updating detectors AND copilot schema doc):
  vm_metrics(ts, vm_id, app, site, cpu_pct, heap_mb, gc_pause_ms, disk_io_await_ms,
             disk_iops, net_mbps, net_retrans_pct, app_latency_ms, error_rate_pct)
  app_events(ts, vm_id, app, site, level, event_type, message)
  topology(vm_id, app, site, depends_on_app)      -- added in M5
  alerts(ts, severity, rule, vm_id, app, headline, evidence_json, action)
- Libraries: pandas/pyarrow, duckdb, pyiceberg, minio, anthropic (optional), openai
  (for the OpenAI-compatible local model endpoint). No Spark, no Kafka in Phase 1.
- Tests: `pytest -q` must pass before every commit; each milestone adds its own tests.
- Keep AIStor specifics honest: Tables need server ≥ RELEASE.2026-02-02 and mc ≥
  RELEASE.2026-02-03; Iceberg REST catalog is served by AIStor itself at /_iceberg.
```

---

# PHASE 1 — Simulated 2,000 VMs, real AIStor lakehouse, sovereign model

## M0 — Scaffold + AIStor wiring (30 min)

**Goal**: repo skeleton, config, and idempotent AIStor setup script.

**Claude CLI prompt:**
```
Read PLAN.md and CLAUDE.md. Create the repo scaffold: aiops/ package, bin/, tests/,
data/, config.ini.example with sections [minio] (endpoint, access_key, secret_key,
secure, raw_bucket=telemetry-raw), [iceberg] (uri=https://<s3-route>/_iceberg,
warehouse=telemetry, namespace=observability, s3_endpoint, access_key, secret_key),
[model] (backend=ollama|openshift_ai|claude, base_url, model_name, api_key optional),
[detect] (heap_limit_mb=4096, io_z_threshold=4, retrans_pct=5, latency_x=2).
Write bin/01_setup.sh: mc alias set from config values, create raw bucket, create
Iceberg warehouse with `mc table warehouse create <alias> telemetry`, print endpoints.
Make it idempotent (safe to re-run). Add a Makefile with targets: setup, gen, load,
detect, chat, test.
```
**Acceptance**: `./bin/01_setup.sh` against the lab AIStor route creates bucket +
warehouse; re-run exits 0.

## M1 — 2,000-VM synthetic telemetry generator (half day)

**Goal**: scale our 25-VM generator to 2,000 VMs without melting a laptop, with
realistic fleet shape and injectable incident scenarios.

**Claude CLI prompt:**
```
Build aiops/generator.py + bin/02_generate.py. Requirements:
- Fleet spec in YAML (fleet.yaml): apps with counts summing to 2000 VMs across 2 sites
  (dc-east, dc-west): patient-onboarding 220, ehr-api 380, scheduler-svc 180,
  billing-svc 160, hl7-ingest 140, postgres-db 90, kafka 60, generic-worker 770.
- Vectorised with numpy (no per-row Python loops): generate per-app baseline matrices,
  add noise, then overlay incident templates. Target: 2h of 30s samples for 2000 VMs
  (~480k rows per 10-min chunk) generated in under a minute per chunk.
- Incident templates, selectable via scenarios.yaml with start/end/vm targets:
  heap_leak (linear MB/min growth + gc + latency coupling, OOM event at cap),
  io_saturation (await ramp, iops collapse, slow_query WARN events),
  net_partition (retrans spike, conn_reset ERROR events),
  error_storm (error_rate + 5xx ERROR events),
  noisy_neighbor (cpu steal on co-located generic-workers).
- Output: hive-partitioned parquet data/vm_metrics/dt=YYYY-MM-DD/hour=HH/part-*.parquet
  and same for app_events; also a --stream mode that emits a chunk every N seconds and
  uploads to s3://telemetry-raw/ via the minio client (this simulates 2000 agents
  shipping without running 2000 agents).
- Deterministic with --seed. Unit tests: row counts, incident VMs show the injected
  pattern, non-incident VMs stay in baseline bands.
```
**Acceptance**: `make gen` produces ~5.7M metric rows for 2h/2,000 VMs; spot-check one
leaking VM's heap ramp; `--stream` uploads chunks to the raw bucket visible in the
AIStor console.

### M1b — Multi-site extension: 25 globally distributed sites (customer variant)

**Context:** the target operator runs ~25 sites worldwide (≈2,000 VMs), each of a
*type* — `manufacturing | warehouse | sales_office | customer_club` — across
regions (NA/EMEA/DACH/APAC/LATAM). This makes the fleet visibly global and
multi-jurisdiction. Implemented on branch `feature/25-site-fleet` (main stays at
tag `phase1-checkpoint`).

**Design (additive, non-breaking):**
- `fleet.yaml` `sites:` become `{name, type, region}` maps (plain strings still
  work → type/region `unknown`); `type_profiles:` apply per-type baseline
  multipliers (warehouse io/heap burst; manufacturing steady; etc.).
- The `(site → location_type, region)` mapping is a separate **`sites` dimension
  table** (unpartitioned Iceberg, overwrite) — so `vm_metrics`/`app_events` keep
  their contract and the **detectors are unchanged**. Slice by location by JOINing
  on `site`.
- Incident scenarios can target by `location_type`/`region`/`site` (e.g. a
  warehouse heap leak), not only by `app`.
- The copilot's `get_alerts` is enriched with site/type/region, and a
  deterministic "which regions/location-types are most at risk?" rollup is added.

**Optional retail re-theme (not done, to protect the checkpoint's tests):** for a
SAP/e-commerce-coherent narrative, rename the healthcare apps —
`ehr-api → erp-api`, `patient-onboarding → order-checkout`,
`hl7-ingest → edi-ingest` — leaving the neutral ones (postgres-db, kafka,
billing-svc, scheduler-svc, generic-worker) as is.

**Demo beats it unlocks:** a warehouse VM's heap leak caught 45 min early; the
copilot answering *"which locations are at risk right now?"* across the fleet;
and a residency/governance line — *EU-site telemetry stays in-region; only masked
aggregates cross borders* (ties into the commerce demo's governance plane).

## M2 — Continuous Iceberg ingestion (2–3 h)

**Goal**: raw Parquet → governed AIStor Tables, append-only, snapshot per batch.

**Claude CLI prompt:**
```
Build aiops/ingest.py + bin/03_load_iceberg.py. Requirements:
- PyIceberg RestCatalog against AIStor's built-in /_iceberg endpoint (config.ini),
  ssl verify off for lab routes.
- Create namespace observability and tables vm_metrics, app_events (schema from
  CLAUDE.md, partitioned by day(ts)) if missing.
- Watch mode: poll s3://telemetry-raw for new objects (or new local chunks), append
  each exactly once (keep a processed-manifest object in S3), print snapshot id per
  append. One append per chunk = one auditable snapshot.
- bin/lake_info.py: list tables, row counts, last 10 snapshots with timestamps, and a
  time-travel example query (count rows as-of a previous snapshot).
- Tests with a mocked catalog for the manifest/idempotency logic.
```
**Acceptance**: two generator runs → `lake_info.py` shows growing row counts and
multiple snapshots; re-processing the same chunk does not duplicate rows.

## M3 — Detection engine at scale + alert delivery (half day)

**Goal**: the four detectors from the prototype, hardened for 2,000 VMs, running as a
loop, delivering alerts somewhere visible.

**Claude CLI prompt:**
```
Port the prototype detectors into aiops/detect.py + bin/04_detect.py:
- Rules: heap_leak (regr_slope+corr over trailing 30 min per vm, OOM ETA minutes),
  io_spike (z-score vs per-vm baseline), net_retrans (threshold), apm_latency
  (p95 vs baseline per app), error_storm (rate jump). Thresholds from [detect].
- Must handle 2,000 VMs: push all math into DuckDB SQL (grouped window queries),
  no per-VM Python loops. Add --asof N replay flag (evaluate as of N minutes before
  data end) — this is the demo money-shot; keep it.
- Write alerts to the Iceberg `alerts` table AND data/alerts.json; optional --webhook
  URL posts Slack-format JSON.
- bin/replay_demo.sh: runs detect at --asof 45/30/15/0 and prints a timeline showing
  the P1 firing ~40 min before the injected OOM with a shrinking ETA.
- Tests: golden-file alerts for a fixed seed; a healthy-fleet run yields zero alerts.
```
**Acceptance**: full 2,000-VM dataset scanned in seconds; `replay_demo.sh` shows the
early-warning timeline; alerts land in the Iceberg alerts table.

## M4 — Sovereign SRE copilot (self-hosted model) (half day)

**Goal**: the chatbot from the prototype, but powered by a cheap local model by
default; Claude API optional.

**Claude CLI prompt:**
```
Build aiops/copilot.py + bin/05_copilot.py:
- Tool-using agent loop with tools run_sql (read-only DuckDB over vm_metrics,
  app_events, alerts; local or iceberg source) and get_alerts.
- Backend abstraction from [model]:
  * ollama / openshift_ai: OpenAI-compatible chat.completions with tool calling
    against base_url (e.g. http://ollama:11434/v1 or the vLLM route from OpenShift AI
    model serving). Default model_name granite3-dense:8b or qwen2.5:7b-instruct —
    small enough for CPU/SNO, good enough for SQL tool calling.
  * claude: anthropic SDK, model claude-sonnet-5 (needs ANTHROPIC_API_KEY).
- Same system prompt for both (SRE for healthcare EHR SaaS, schema doc, "ground every
  claim in query results, cite numbers, never invent data").
- Add bin/deploy_ollama.yaml: Deployment+Service+Route for Ollama on the SNO cluster
  (CPU, 8Gi request, emptyDir or PVC for models) plus a one-liner to pull the model.
- CLI: python bin/05_copilot.py "why is patient onboarding slow?" --backend ollama.
- A comparison mode --backend both that answers with local model then Claude, printed
  side by side (the "sovereign now, augment later" demo beat).
- Handle small-model tool-calling failures gracefully: retry once with a simplified
  prompt; if the model still can't produce a tool call, fall back to a canned SQL plan
  (top-latency app -> its VMs' heap/gc/io -> matching alerts) and let the model only
  summarise the fetched rows. This keeps the demo robust.
```
**Acceptance**: with no internet-bound API key set, the copilot answers "why is patient
onboarding slow?" citing real numbers from the lake, running fully in-cluster.

## M5 (optional, customer-story parity) — Graph root-cause layer (half day)

**Goal**: mirror the MinIO customer-story architecture: topology graph over the same
Iceberg tables for multi-hop "what does this failure impact?" questions.

**Claude CLI prompt:**
```
Add a topology dimension and a graph query path:
- Extend the generator to emit topology (vm_id, app, site, depends_on_app) reflecting:
  patient-onboarding -> ehr-api -> postgres-db; hl7-ingest -> kafka -> ehr-api.
  Load it as an Iceberg table.
- Option A (lightweight, default): implement graph traversal as recursive CTEs in
  DuckDB exposed to the copilot as a third tool `trace_dependencies(app)` returning
  upstream/downstream apps + their active alerts. 
- Option B (customer-story faithful, docker/podman): docker-compose for PuppyGraph
  configured to read the Iceberg tables via AIStor's REST catalog; include a sample
  Cypher query file (why did service X fail -> traverse edges -> hosts with alerts).
  Document it in docs/puppygraph.md; do not make the main demo depend on it.
- Copilot answer for "what happens if postgres-db goes down?" must use the graph tool.
```
**Acceptance**: copilot correctly answers blast-radius questions using topology, e.g.
ties the postgres io_spike alert to patient-onboarding latency through ehr-api.

## M6 — Demo polish (2 h)

**Claude CLI prompt:**
```
Create docs/DEMO_SCRIPT.md: a 12-minute walkthrough with exact commands in order
(setup -> stream gen -> lake_info snapshots -> replay_demo timeline -> copilot local
model -> copilot --backend both -> optional graph question), what to say at each step
(licensing math, sovereignty, deterministic-before-AI, Iceberg audit trail for HIPAA),
and a troubleshooting appendix (TLS --insecure, stale mc, PyIceberg auth). Also add a
tiny Streamlit page bin/dashboard.py: fleet health grid, heap chart for any VM, live
alerts table — read-only over DuckDB.
```

---

# PHASE 2 — Real VMs on OpenShift Virtualization + synthetic scale

The lakehouse, detectors, and copilot **do not change**. Phase 2 replaces synthetic
producers with real ones and amplifies to scale.

## M7 — Real VM fleet (OpenShift Virtualization)

**Claude CLI prompt:**
```
Create phase2/vms/: manifests for OpenShift Virtualization (kubevirt) — 5 Fedora/RHEL
VMs via VirtualMachine CRs (small: 2 vCPU/2-4Gi): 2x app VMs running a demo Java
"patient-onboarding" service with a /leak toggle endpoint (allocates memory on a timer
— our real heap-leak), 1x postgres VM, 1x load-generator VM (hey/vegeta against the
app), 1x utility VM. Include cloud-init to install node_exporter and vector on each.
Document prerequisites: OpenShift Virtualization operator on the SNO, CPU/RAM budget.
```

## M8 — Real event shipping to the lakehouse

**Claude CLI prompt:**
```
Create phase2/ingest/:
- vector.toml for the VMs: tail journald + app logs, scrape node_exporter, batch and
  sink to S3 (AIStor endpoint, telemetry-raw bucket, parquet or ndjson gz) keyed
  vm_id/app tags matching the data contract.
- aiops/normalize.py: maps vector's node_exporter/log output into the vm_metrics /
  app_events schema (heap from JVM metrics endpoint, io await from node disk stats,
  retrans from netstat metrics), then hands off to the existing M2 ingest loop.
- The result: REAL events from 5 VMs flow into the SAME Iceberg tables.
```

## M9 — Hybrid scale + chaos + production hardening

**Claude CLI prompt:**
```
- Amplifier: run the M1 generator in --stream mode for 1,995 synthetic VMs while the 5
  real VMs ship real data — one fleet, one lakehouse, tag column `origin` real|synthetic.
- Chaos runbook phase2/chaos.md: curl the /leak endpoint on app VM, stress-ng io on
  postgres VM, tc netem packet loss on kafka path — then show detectors firing on REAL
  precursors with the same rules.
- Hardening checklist implemented where cheap: Iceberg table maintenance (compaction/
  expire snapshots via pyiceberg), ILM tiering policy on telemetry-raw, dedicated
  AIStor access keys per producer (least privilege), retention policy (e.g. 30d raw /
  13mo tables), alert routing config, and a SIZING.md extrapolating measured ingest to
  2,000 real VMs (rows/day, GB/day, projected AIStor capacity).
```

**Phase 2 exit criteria**: pull the /leak lever on a real VM in front of the audience;
the P1 heap-leak alert with OOM ETA fires from real telemetry; copilot explains it via
the local model; kill the leak; alert clears.

---

## Risks / honesty notes for the interview

- PyIceberg ↔ AIStor REST catalog auth/TLS is the most likely friction point; the lab
  fallback (`--source local`) keeps every demo step alive.
- A 7–8B local model on CPU SNO is slow (tens of seconds) and imperfect at tool
  calling — that's *why* M4 includes the canned-plan fallback and the `--backend both`
  comparison; frame it as the sovereignty/quality trade-off, not a bug.
- Synthetic data is honest as "scale + pattern realism"; Phase 2's real-VM lever is the
  answer to "but is it real?".
- This is a demo, not a SIEM replacement claim: no auth-log parsing, no detections
  content library, no compliance reporting — position as the *storage + analytics
  substrate* those teams build on, which is exactly the licensing-cost argument.
