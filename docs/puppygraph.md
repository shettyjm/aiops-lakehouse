# Optional graph layer: PuppyGraph over the Iceberg topology (M5, Option B)

The default blast-radius path in this repo is the copilot's **`trace_dependencies`**
tool — DuckDB recursive CTEs over the `topology` Iceberg table (Option A, always
on, no extra services). **The main demo does not depend on PuppyGraph.**

This page documents the **customer-story-faithful** alternative: [PuppyGraph](https://puppygraph.com)
reading the **same** AIStor Iceberg tables via the REST catalog and answering
graph questions in **Cypher** — mirroring the reference MinIO architecture
(NL query → graph traversal → root cause).

## Why it mirrors the customer story
- No ETL into a separate graph DB — PuppyGraph queries the Iceberg tables in
  place, the same `telemetry.observability.topology` we already load.
- Multi-hop traversal ("what fails if X goes down?") in Cypher, the classic
  graph shape, instead of SQL recursion.
- Same source of truth as the detectors, dashboard, and copilot.

## Files
- [`graph/docker-compose.yaml`](../graph/docker-compose.yaml) — run PuppyGraph locally (docker/podman).
- [`graph/schema.json`](../graph/schema.json) — maps the Iceberg `topology` table to
  a property graph (`App` / `VM` vertices; `DEPENDS_ON` / `RUNS_ON` edges).
- [`graph/blast_radius.cypher`](../graph/blast_radius.cypher) — sample queries,
  including the postgres-db → … → patient-onboarding root-cause path.

## Run it (optional)
```bash
cd graph
# fill in the AIStor route + keys in schema.json first
podman compose up -d        # or: docker compose up -d
# open the UI at http://localhost:8081 (puppygraph / puppygraph123)
# load schema.json, then run graph/blast_radius.cypher
```

Example — everything impacted if postgres-db fails:
```cypher
MATCH path = (root:App {app:'postgres-db'})<-[:DEPENDS_ON*1..5]-(impacted:App)
RETURN DISTINCT impacted.app, length(path) AS hops ORDER BY hops;
```
→ `ehr-api (1)`, `billing-svc (1)`, `patient-onboarding (2)`, `kafka (2)`,
`scheduler-svc (2)`, `hl7-ingest (3)` — the same blast radius the copilot's
`trace_dependencies` tool returns.

## Caveats (be honest in the demo)
- **Config keys vary by PuppyGraph version** — `schema.json` here is a sketch;
  verify catalog/S3 auth fields against the PuppyGraph docs for your build. The
  AIStor REST catalog uses SigV4 (`signingName=s3tables`, `region=us-east-1`),
  the same params as `mc table config … --spark`.
- **This is a parity/depth artifact**, not part of the 12-minute run. Show it if
  the audience asks "could this be a real graph engine?" — otherwise the
  `trace_dependencies` tool already answers blast-radius questions live.
