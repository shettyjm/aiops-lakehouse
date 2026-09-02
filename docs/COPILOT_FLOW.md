# How the SRE copilot works, end to end

Walkthrough of `bin/05_copilot.py` — what happens from a plain-English question
to a grounded answer. Traced with the command:

```bash
.venv/bin/python bin/05_copilot.py "why is patient onboarding slow?" \
    --backend ollama --source iceberg --show-tools
```

---

## The three layers

```
bin/05_copilot.py        CLI: parse args, pick backend, print result
   └─ aiops/copilot.py    agent loop + backends + tools + grounding safety net
        ├─ aiops/detect.py   get_alerts runs the M3 detector; opens DuckDB
        └─ aiops/ingest.py   the AIStor Iceberg REST connection (M2)
```

The copilot doesn't "know" the fleet — it's given **read-only tools to query the
same governed lake** everything else uses. Swap synthetic data for real and it
works unchanged.

---

## Step by step

### 1. CLI setup — `main()`
- `parse_args` reads the question, `--backend ollama`, `--source iceberg`, `--asof`.
- `load_config()` reads `config.ini`: `[model]` (base_url = the Ollama Route,
  model_name = qwen2.5:3b-instruct), `[iceberg]`, `[detect]`.
- `backend = "ollama"` (from the flag, else `[model] backend`).

### 2. Open the lake — `open_lake()` → `detect.duckdb_from_iceberg()`
With `--source iceberg`:
- `ingest.iceberg_catalog(cfg)` builds the PyIceberg REST connection to AIStor —
  **SigV4 auth + the cookie-blocking fix** (see `pyiceberg-aistor-cookie-fix`).
- Loads `vm_metrics` + `app_events`, **scans them to Arrow**, and `register`s
  them in an in-memory **DuckDB** connection.
- Returns a **`LakeTools`** = { DuckDB connection, a `DetectConfig` from `[detect]` }.

With `--source local` it `read_parquet`s from `./data` instead — same DuckDB
interface, no server needed (the run-anywhere fallback).

### 3. Hand off to the agent — `answer(question, tools, cfg, "ollama", asof)`
Backend isn't `none`, so it calls **`_run_openai`** (Ollama speaks the
OpenAI-compatible API; `openshift_ai`/vLLM is the same path).

### 4. What the model is given
- **System prompt** (`SYSTEM_PROMPT`): "You are an SRE for a healthcare EHR
  SaaS… ground EVERY claim in query results, cite numbers, never invent data,"
  plus the **schema doc** (columns, app names, the 4096 MB heap cap).
- **User message**: the question.
- **Two tools** as JSON schemas (`TOOL_DEFS`):
  - `run_sql(sql)` — a read-only DuckDB query
  - `get_alerts(severity?, app?)` — current detector alerts

### 5. The agent loop (the "agentic" part) — `_run_openai`
Loops up to 6 times:

- **Iteration 0** — model called with `tool_choice="required"` (force a tool call
  on the first step so a small model must ground itself). The model returned two
  tool calls:
  ```
  run_sql("SELECT ts, app_latency_ms FROM vm_metrics WHERE app='patient-onboarding' ...")
  get_alerts({app: "patient-onboarding"})
  ```
  Each is dispatched via `tools.call(name, args)`:
  - `run_sql` → `LakeTools.run_sql`: enforces single `SELECT`/`WITH` (the
    read-only guard blocks INSERT/DROP/COPY/PRAGMA/…), runs it, returns a text
    table of rows.
  - `get_alerts` → `LakeTools.get_alerts`: **runs the whole M3 detector**
    (`detect.detect`) over the lake now, filters to the app, returns the alert
    rows (leak rates, OOM ETAs).

  Both results are appended to the conversation.

- **Iteration 1** — model called with `tool_choice="auto"`. With the real data in
  context, it returns **final text** (no more tool calls) — the grounded answer.

Returns `AgentResult(text, tool_calls=[run_sql, get_alerts], backend="openai")`.

### 6. The grounding safety net — `answer()`
Before trusting the result:

- **Empty text?** → raise → deterministic fallback.
- **Zero tool calls?** (model answered *without* querying — what a weak model like
  granite-3B does) → **discard its numbers**, `_gather` the real rows via SQL, and
  have the model **summarise those real rows** (`_summarize_with_model`); if that
  call fails, return the deterministic `_narrative`.
- **Tool calls + text?** → return as-is (this run → tag `[openai]`).

Any exception (model unreachable, etc.) → `deterministic_answer`. **A wrong
number can never reach the screen.**

### 7. Print — `_emit()`
Prints the tag `[openai]`, the tool calls (`--show-tools`), then the answer.

---

## The picture

```
your question
   │
   ▼
[system prompt + schema + tools]  →  qwen2.5:3b (on your OpenShift cluster)
   │                                        │  "I should call these tools"
   │   ┌────────────────────────────────────┘
   ▼   ▼
 run_sql ─────► DuckDB ─┐
 get_alerts ─► detect ──┤► reads vm_metrics / app_events
   │                    │   (Iceberg via AIStor REST catalog, or local parquet)
   │◄── real rows ──────┘
   ▼
model writes a grounded answer
   │
   ▼
grounding guard: did it actually query?  ── no ──► summarise real rows / deterministic SQL
   │ yes
   ▼
print  [openai]  + answer
```

---

## Backends (same tools + system prompt for all)

| `--backend` | Path | Notes |
|---|---|---|
| `ollama` | `_run_openai` → OpenAI-compatible `/v1` | self-hosted; the sovereign default |
| `openshift_ai` | `_run_openai` (same) | RHOAI/vLLM route; enterprise path |
| `claude` | `_run_claude` → Anthropic SDK | needs `ANTHROPIC_API_KEY`; quality compare |
| `none` | `deterministic_answer` | no model — pure SQL root-cause narrative |
| `both` | local backend, then `claude` | side-by-side "sovereign now, augment later" |

---

## Why it's built this way

1. **Tools, not training.** The model is given read-only tools to *query* the
   fleet live, not fine-tuned on it. Real data drops in unchanged.
2. **Backend-agnostic.** ollama / OpenShift AI / Claude / none share one loop,
   one system prompt, one set of tools.
3. **Grounding is enforced, not hoped for.** Forced first tool call + the
   "did it query?" guard + deterministic fallback mean the copilot **cannot show
   an invented metric** — essential for healthcare.
4. **Same lake as everything else.** `get_alerts` runs the *identical* M3
   detector the dashboard and CLI use. The copilot is a natural-language
   front-end over the exact same governed Iceberg tables, not a side pipeline.

## Related

- Deterministic detection: the five rules live in `aiops/detect.py` (M3).
- Model deployment + the RHOAI/Granite path: [`OLLAMA_DEPLOY.md`](OLLAMA_DEPLOY.md).
- Demo narrative: [`STORY_TO_TELL.md`](STORY_TO_TELL.md).
