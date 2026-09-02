# The Story to Tell

The demo narrative for the Predictive AIOps Lakehouse on MinIO AIStor + OpenShift.
Keep this as the plain-language script; the exact commands live in the milestone
acceptance tests and (later) `docs/DEMO_SCRIPT.md`.

---

## One-line pitch

> A batch snapshot of synthetic fleet telemetry, stored as governed Iceberg
> tables in AIStor, read back through the Iceberg REST catalog, with a
> deterministic SQL engine predicting out-of-memory crashes ~45 minutes ahead
> and a Streamlit dashboard visualizing it — all running against real AIStor, on
> a pipeline that's already built to stream live data.

---

## What the demo currently is

A **static (batch) snapshot** of **synthetic** 2,000-VM-scale telemetry that was
generated and loaded once, **stored in AIStor Iceberg**, **read back via the
Iceberg REST API**, and **visualized through a UI dashboard** — with the
crash **prediction** produced by a deterministic detection engine in between.

That last part is the key correction to keep straight: **the detectors predict,
the dashboard only displays.**

---

## The data flow (narrate this)

```
1. GENERATE   synthetic 2,000-VM telemetry (numpy, vectorised)   → aiops/generator.py
2. LAND       Parquet chunks → s3://telemetry-raw                 (AIStor S3 bucket)
3. INGEST     raw Parquet → Iceberg tables, 1 snapshot per chunk  → aiops/ingest.py
4. DETECT     read tables → DuckDB SQL → OOM ETA + alerts         → alerts table + alerts.json
5. VISUALIZE  read tables + alerts via REST → dashboard           → bin/dashboard.py
```

---

## Three refinements to say precisely

1. **The "predictability" is not the dashboard.** The OOM-ETA countdown is
   computed by the **deterministic detection engine** (step 4) in plain,
   auditable SQL: fit a line to the memory trend, then
   `(ceiling − current) ÷ slope = minutes to crash`. No black-box ML — that's a
   deliberate selling point: reproducible, cheap at 2,000-VM scale, explainable.
   The dashboard is just the window onto that result.

2. **"Static" describes what you're seeing, not a limit of the system.** You're
   looking at a fixed 2-hour snapshot. But the pipeline is already
   **streaming-capable**: `--stream` ships chunks every N seconds and a `--watch`
   ingest loop appends new chunks as they land. We ran it in batch for the demo.
   The **as-of slider** *simulates* time advancing so static data feels live
   (rewind-the-tape); it is not a live feed.

3. **"Read through the REST API" — be exact.** The Iceberg **REST catalog**
   serves the *metadata* (which tables/snapshots exist, schema, the list of data
   files per snapshot). The telemetry **rows** are read from the **Parquet files
   in the AIStor bucket** via S3, using that file list. REST catalog for
   metadata, S3 for data — the standard Iceberg design, both halves against the
   lab AIStor.

---

## The centerpiece: the heap-leak early-warning replay

One VM is leaking memory toward a 4,096 MB ceiling; hitting it = an out-of-memory
crash (OOM) that takes patient-onboarding down. Replaying the detector as "now"
advances toward the crash:

| as of  | severity | OOM ETA  |
|--------|----------|----------|
| T-45m  | P2       | ~50 min  |
| T-30m  | **P1**   | ~31 min  |
| T-15m  | **P1**   | ~12 min  |
| T-0m   | **P1**   | ~3 min   |

Read top-to-bottom: the **ETA shrinks toward zero** (like a car's "miles to
empty"), and severity **auto-escalates P2 → P1** once the ETA drops under 45 min
— no human tuning it in the moment. The punchline: the P1 fired **45+ minutes
before** the crash **while every ordinary dashboard still showed green**, because
traditional monitoring only alarms *after* a red line is crossed. We caught the
**trend**, not the threshold. Predictive, not reactive — "smoke, not fire."

Honest caveat to state up front: this is a **simulated** leak we injected on
purpose, so we know the right answer and can prove the detector nails it. Phase 2
pulls the same lever on a **real** VM to show it works on real telemetry.

---

## Why AIStor / the licensing argument

2,000 VMs × 30s samples × ~12 metrics ≈ 5.7M rows/hour ≈ a few GB/day of
compressed Parquet. AIStor is the cheap, tiered SIEM/observability store that
replaces per-GB Datadog/Splunk licensing; Iceberg gives one governed contract
(same tables for generators, detectors, dashboard, and the copilot), snapshots
give a HIPAA-friendly audit trail, and the model stays self-hosted for data
sovereignty (healthcare telemetry never leaves the cluster).

---

## Where it goes next

- **M4 — sovereign SRE copilot:** a self-hosted LLM answers "why is patient
  onboarding slow?" grounded in the lake (SQL tools over the same tables), with
  a `--backend both` local-vs-Claude comparison ("sovereign now, augment later").
- **Phase 2 — real VMs:** swap the synthetic generator for real OpenShift
  Virtualization VMs shipping real telemetry into the **same** Iceberg tables.
  Nothing downstream changes. That's when "static demo" becomes "live."
