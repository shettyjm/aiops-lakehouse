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
  sites(site, location_type, region)              -- 25-site dim; JOIN on site.
      location_type: manufacturing|warehouse|sales_office|customer_club
  alerts(ts, severity, rule, vm_id, app, headline, evidence_json, action)
- vm_metrics/app_events keep their columns; site slicing is via the sites dim
  (a JOIN), so detectors are unchanged. Sites are {name, type, region} in
  fleet.yaml; per-type baseline multipliers live in fleet.yaml type_profiles.
  Scenarios may target by app and/or location_type/region/site.
- Libraries: pandas/pyarrow, duckdb, pyiceberg, minio, anthropic (optional), openai
  (for the OpenAI-compatible local model endpoint). No Spark, no Kafka in Phase 1.
- Tests: `pytest -q` must pass before every commit; each milestone adds its own tests.
- Keep AIStor specifics honest: Tables need server ≥ RELEASE.2026-02-02 and mc ≥
  RELEASE.2026-02-03; Iceberg REST catalog is served by AIStor itself at /_iceberg.