# Deploying the sovereign model (Ollama) on OpenShift

The M4 copilot answers grounded questions with a **self-hosted** model so
telemetry never leaves the cluster. This is the quick, CPU-only path using
Ollama. Manifest: [`bin/deploy_ollama.yaml`](../bin/deploy_ollama.yaml).

> For a production/enterprise story, prefer **Red Hat OpenShift AI (RHOAI)**
> model serving (vLLM, Red Hat–supported images, GPU-aware). The copilot already
> talks to it via the `openshift_ai` backend — same OpenAI-compatible API, just
> point `[model] base_url` at the vLLM route. Ollama is the cheap CPU demo.

---

## 1. Pre-flight checks (do these first)

```bash
# a) Is there a default StorageClass? (the model PVC needs real storage, not node disk)
oc get storageclass
#   -> expect one marked (default), e.g. gp3-csi on AWS. If none, set
#      storageClassName: in the PVC in deploy_ollama.yaml.

# b) Does the cluster allow pulling from docker.io?
oc get image.config.openshift.io/cluster -o yaml | grep -A10 registrySources
#   -> if allowedRegistries is set and excludes docker.io, mirror the image (step 4).

# c) Node headroom for a CPU 8B model (~6-7 GiB RAM at runtime)
oc adm top nodes
```

### Sizing (per the m5.4xlarge lab node)

| Resource | Ollama (8B, Q4) | Manifest req/limit | Notes |
|---|---|---|---|
| Memory | ~6–7 GiB runtime | 8Gi / 12Gi | comfortable on a 64 GiB node |
| CPU | inference-bound | 2 / 6 | **slow on CPU — tens of sec/answer** |
| Model storage | ~5 GB per model | 20Gi PVC | on a real StorageClass (EBS), not node disk |
| Image pull | ~2 GB | 2Gi/4Gi ephemeral | uses node ephemeral storage |

CPU inference is deliberately slow — that's the sovereignty/quality trade-off.
The copilot's **deterministic fallback answers instantly** regardless, so the
demo never stalls.

---

## 2. Deploy

```bash
oc new-project aiops                 # or: oc project aiops
oc apply -f bin/deploy_ollama.yaml
oc rollout status deploy/ollama --timeout=180s
oc get pods -l app=ollama
```

Pull a model into the pod (persists on the PVC):

```bash
oc exec deploy/ollama -- ollama pull granite3-dense:8b   # ~5 GB, small enough for CPU/SNO
# alternatives: qwen2.5:7b-instruct  (good SQL tool-calling)
oc exec deploy/ollama -- ollama list
```

---

## 3. Point the copilot at it

In `config.ini` `[model]`:

```ini
[model]
backend    = ollama
base_url   = http://ollama.aiops.svc.cluster.local:11434/v1   # in-cluster
model_name = granite3-dense:8b
```

(From your laptop instead of in-cluster, use the external Route:
`https://<ollama-route-host>/v1` with `--insecure` if the route is self-signed.)

Then:

```bash
bin/05_copilot.py "why is patient onboarding slow?" --backend ollama --source iceberg
bin/05_copilot.py "why is onboarding slow?" --backend both   # local model vs Claude
```

---

## 4. If docker.io is blocked or rate-limited

docker.io throttles anonymous pulls, and some clusters restrict external
registries. Mirror the image into the internal registry (or Quay) and update
`image:` in the manifest:

```bash
oc registry login
oc image mirror docker.io/ollama/ollama:latest \
    image-registry.openshift-image-registry.svc:5000/aiops/ollama:latest
# then set image: image-registry.openshift-image-registry.svc:5000/aiops/ollama:latest
```

---

## 5. Why the manifest is not a vanilla Ollama deploy

OpenShift's default **`restricted-v2` SCC** runs the pod as a **random non-root
UID**. The stock `ollama/ollama` image writes to `/root/.ollama`, but a non-root
UID can't even traverse `/root` (mode 700) — a vanilla deploy crash-loops. The
hardened manifest:

- **Mounts the model PVC at `/models`** and sets **`HOME=/models`** +
  `OLLAMA_MODELS=/models/.ollama/models`, so Ollama writes where the random UID
  can. OpenShift auto-injects an `fsGroup`, making the PVC group-writable.
- Meets restricted-v2: `allowPrivilegeEscalation: false`, `capabilities: drop
  [ALL]`, `runAsNonRoot: true`, `seccompProfile: RuntimeDefault`.
- Does **not** pin `runAsUser`/`fsGroup` — OpenShift assigns them from the
  namespace range. No `anyuid` or custom SCC needed.

---

## 6. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Pod `CrashLoopBackOff`, logs show permission denied writing models | image running as root path under restricted SCC | you're on a vanilla manifest — use the hardened one (HOME=/models) |
| Pod `Pending`, PVC unbound | no default StorageClass | set `storageClassName:` in the PVC |
| `ImagePullBackOff` | docker.io blocked or rate-limited | mirror the image (step 4) |
| Copilot slow (tens of seconds) | CPU inference of an 8B model | expected; use RHOAI+vLLM/GPU, or rely on the deterministic fallback |
| Copilot answers but tagged `· fallback` | model unreachable or couldn't tool-call | check `base_url`/Service DNS; verify `ollama list` shows the model |
| `tool_choice`/tool calls ignored by model | small model weak at tool calling | try `qwen2.5:7b-instruct`; the canned-plan fallback still answers |

---

## 7. Clean up

```bash
oc delete -f bin/deploy_ollama.yaml     # keeps the PVC's data? no — deletes PVC too
# to keep models, delete everything except the PVC.
```

---

## 8. Red Hat–certified alternative: Granite on RHOAI / vLLM (no docker.io)

Ollama-from-docker.io is the quick CPU demo. The **supported** way to run
Granite-8B on OpenShift uses Red Hat images from **`registry.redhat.io`**, not
Docker Hub — and it's what you'd show a security-conscious customer.

**The pieces:**

- **Serving runtime (the image that runs the model):** Red Hat OpenShift AI
  (RHOAI) ships a **vLLM ServingRuntime for KServe**, built and supported by Red
  Hat, pulled from `registry.redhat.io` (RHOAI adds it as a built-in
  ServingRuntime — you don't hand-write the image path). It needs a
  `registry.redhat.io` pull secret (a Red Hat account / service account token).
- **Granite weights (the model):** IBM Granite is open (Apache 2.0). Get the
  weights from **Hugging Face** `ibm-granite/granite-3.1-8b-instruct`, or as a
  Red Hat **OCI "ModelCar"** image that KServe pulls directly. **RHEL AI** ships
  Granite as its *default* model, distributed from `registry.redhat.io/rhelai1/`
  and fetched with `ilab model download` — fully Red Hat–hosted, no Docker Hub.
- **Result:** an `InferenceService` exposing an **OpenAI-compatible** `/v1`
  endpoint — exactly what our copilot's `openshift_ai` backend already speaks.

**Rough shape (RHOAI):**
1. Install the **Red Hat OpenShift AI** operator; enable KServe + the vLLM
   ServingRuntime.
2. Add the `registry.redhat.io` pull secret to the serving namespace.
3. Create an `InferenceService` for `granite-3.1-8b-instruct` (weights from an
   S3/OCI ModelCar location), on GPU if available.
4. Point `config.ini`:
   ```ini
   [model]
   backend    = openshift_ai
   base_url   = https://<inferenceservice-route>/v1
   model_name = granite-3.1-8b-instruct
   ```
   No copilot code changes — the `openshift_ai` backend is OpenAI-compatible.

> **Verify exact image tags/paths in *your* cluster** — RHOAI serving-runtime
> image versions and RHEL AI OCI paths move between releases. Use the RHOAI
> dashboard (**Settings → Serving runtimes**) or browse the
> [registry.redhat.io catalog](https://catalog.redhat.com/software/containers)
> rather than hardcoding a tag from this doc.

**When to pick which:**

| | Ollama (this manifest) | RHOAI + vLLM (Granite) |
|---|---|---|
| Image source | docker.io (community) | registry.redhat.io (certified) |
| Setup effort | one `oc apply` | RHOAI operator + InferenceService |
| Hardware | CPU-only OK (slow) | GPU-aware, much faster |
| Support | community | Red Hat supported |
| Best for | quick demo / no RHOAI | enterprise / customer-facing |
