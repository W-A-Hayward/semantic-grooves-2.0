# Running semantic-grooves on a GPU with Kubernetes Jobs

A from-scratch guide to the Phase 1 bootstrap pipeline: what we embed, how Kubernetes Jobs work, how to stand up a one-machine cluster that can use your AMD GPU, and the Go code that ties it together.

**How to read this.** Parts 1–3 are concepts (what and why). Part 4 is the architecture. Part 5 is a hands-on, copy-paste setup of the cluster. Part 6 is the Go code. Part 7 is operations and troubleshooting. **Parts 5 and 6 interleave:** steps 5.0–5.5 need no code of yours (cluster, GPU, Kafka, Ollama); at 5.6 you need the Go code and Dockerfile from Part 6, so do 5.0–5.5, then Part 6, then 5.6 onward.

> **Verification status.** Every Go file in Part 6 was compiled and `go vet`-ed in a scratch module against `client-go v0.37.1`, `kafka-go v0.4.51` and `modernc.org/sqlite v1.58.0` (the versions your `go.mod` pins, plus current client-go). The SQL in `fetch.go` was run against your real `data.sqlite3` and returns exactly 25,212 albums. **Nothing in the YAML or the Kubernetes/GPU/Ollama steps has been run** (and I could not verify that image tags such as `apache/kafka:3.9.0` or `golang:1.27` exist, which Kubernetes release first made `podFailurePolicy` stable, which I believe was 1.31, or the exact name of the fake clientset constructor in 6.7), because this machine has no cluster yet. Treat Part 5 as a careful recipe, not a tested script, and expect to debug a step or two. Throughput numbers are estimates and are labelled as such.

## Contents

1. [Where the project is today](#1-where-the-project-is-today)
2. [What we embed](#2-what-we-embed)
3. [Kubernetes, from zero to Jobs](#3-kubernetes-from-zero-to-jobs)
4. [The pipeline architecture](#4-the-pipeline-architecture)
5. [Hands-on setup](#5-hands-on-setup)
6. [The Go implementation](#6-the-go-implementation)
7. [Operating it](#7-operating-it)
8. [What comes next](#8-what-comes-next)

---

## 1. Where the project is today

Facts I measured on this machine and in the repo, because they shape every decision below:

| Fact | Value | Consequence |
|---|---|---|
| GPU | AMD Radeon RX 6800 / 6800 XT / 6900 XT class (Navi 21), 16 GB VRAM class, `/dev/kfd` present, ROCm in `/opt/rocm` | **Not NVIDIA.** Everything GPU-related uses the AMD stack: `amd.com/gpu`, the ROCm device plugin, `ollama/ollama:rocm`. Most tutorials you'll find assume `nvidia.com/gpu`; translate accordingly. |
| Cluster tooling | no `kubectl`, `kind`, `minikube`, `k3s`, `helm` | We start from zero (Part 5). |
| Docker | installed, you are in the `docker` group | Used to build images. |
| Ollama | installed as a systemd service, **currently running** | It already owns the GPU. It will fight an in-cluster Ollama for VRAM, so we stop it first. |
| Models already pulled | `qwen2.5:7b`, `qwen2.5:14b`, `llama3.1`, `llama3.2-vision:11b` | No embedding model yet; we'll pull one. |
| Data | `data/data.sqlite3`: 24,169 reviews, 25,212 albums ("tombstones"), mean review length ≈ 4,165 chars, max 22,450 | Whole reviews fit in a model context window (see 2.1). |
| Go | 1.27.0 | Matches your `go.mod`. |

### Existing issues I found (I have *not* changed your code)

1. **`server/` does not build.** `parser.go` uses `ReviewUrl` but the struct field in `fetcher.go` is `Url`. `go build` fails with `unknown field ReviewUrl`.
2. **`fetchReviews(db, 1, 30)` passes the page number as the SQL `OFFSET`**, so "page 1" skips 1 row, not 30.
3. **Every dependency in `go.mod` is marked `// indirect`.** Run `go mod tidy` and they'll be classified correctly.
4. **The query duplicates rows.** `reviews` is joined to `genre_review_map` and `artist_review_map`, both many-to-many, so a review with 3 genres comes back 3 times. Measured: the join returns **28,588 rows for 25,212 albums** (3,376 duplicates). 3,841 reviews have more than one genre and 734 have more than one artist.
5. **The unit of work is the album, not the review.** `tombstones` has a `picker_index` and 586 reviews cover more than one album (25,212 albums vs 24,169 reviews). Those albums share one review body.
6. **`parse_review` splits reviews into paragraph messages.** That's fine for a future passage-retrieval feature, but it makes it impossible to tag a whole album's mood, because each Kafka message only sees one paragraph. The pipeline below sends one message per album instead. Part 6 replaces `server/` with `cmd/ingest`.

---

## 2. What we embed

### 2.1 The unit: one album, one message

The README says the bootstrap pipeline "groups [reviews] per album into Kafka messages". Concretely, an **album** is one row of `tombstones`, identified by `review_tombstone_id`. Each message carries:

- identity: `album_id`, `review_url`
- metadata: `title`, `artists[]`, `genres[]`, `score`, `pub_date`
- the full review `body`

Why whole reviews and not paragraphs: the average review is ~4,165 characters, which is **roughly 1,000 tokens** (≈4 characters per token for English prose; my estimate). The longest is 22,450 characters, **roughly 5.6k tokens**. That fits in the tagging LLM's 8k-token context, so the LLM sees the *entire* review and there's no need to chunk it.

The embedding model is the tighter limit: Ollama's library page lists **`nomic-embed-text` at a 2K-token context**. So the raw-review vector (2.2) is built from the first 6,000 characters (≈1.5k tokens). In your data 2,062 of 24,169 reviews (8.5%) are longer than that and lose their tail in `review_vec`; 677 (2.8%) are over 8,000 characters. That's acceptable because `review_vec` is the secondary vector and `vibe_vec` comes from the LLM's reading of the *whole* review. If it bothers you, use a longer-context embedding model for `review_vec` or chunk long reviews and average the chunk vectors.

The ingest step must aggregate genres and artists into arrays (one row per album) instead of letting the SQL join multiply rows. The fixed query is in Part 6.

### 2.2 The two vectors per album

Your goal is searching by *feeling* ("hazy", "driving", "triumphant"). A raw review is a poor thing to embed for that: much of it is band history, track-by-track commentary, and name-dropping, and a query like "melancholic" has to compete with all of that. So the worker produces **two vectors**:

| Vector | Embedded text | Purpose |
|---|---|---|
| `vibe_vec` (primary) | `search_document: Mood: <moods>. Genre: <genres>. Energy: <1-5>/5. <LLM-written 1–2 sentence vibe>` | Mood search. The LLM has already distilled the review into the feeling, so the vector clusters by mood. |
| `review_vec` (secondary) | `search_document: <first 6,000 characters of the review>` | Topical / lexical-ish similarity ("albums about grief", "albums that mention field recordings"). Limited by the embedding model's 2K-token context (see 2.1). |

Design rules for the embedded text:

- **Exclude artist and album names from the vibe text.** Otherwise "Radiohead" pulls toward other Radiohead records instead of toward the mood. The system prompt tells the LLM not to mention them, and `vibeDocument` never includes them.
- **Do not embed the score.** Numbers embed poorly. Keep `score` as metadata and use it to filter or re-rank.
- **Use the model's prefixes.** `nomic-embed-text` is trained with task prefixes: documents get `search_document: `, queries get `search_query: `. Forgetting the prefix measurably hurts retrieval quality. The same string layout must be used at query time later.
- **Store the exact embedded text** (`vibe_text`) next to the vector. When you change the prompt or the model, you can tell which vectors are stale, and you can debug "why did this album match".

### 2.3 Models

| Role | Model | Why |
|---|---|---|
| Tagging LLM | `qwen2.5:7b` (already pulled, ~4.7 GB) | Good at following a JSON schema; leaves plenty of VRAM for parallel requests. `qwen2.5:14b` (~9 GB) is possible but, with 4 parallel slots, gets uncomfortably close to the VRAM limit (see below). |
| Embedding | `nomic-embed-text` (~274 MB, 768 dimensions per its model card; 2K context in Ollama) | Small and fast, well suited to short documents like `vibe_text`. Alternative: `mxbai-embed-large` (1024 dims, stronger, slower). Pick one and stick with it: **vectors from different models are not comparable.** |

VRAM budget on 16 GB, rough arithmetic (check with `ollama ps` in practice):

- `qwen2.5:7b` weights ≈ 4.7 GB.
- The KV cache grows with `num_ctx × parallel slots`. At `num_ctx=8192` and 4 slots, that's 32k tokens of cache; for a 7B model with grouped-query attention that's on the order of 2 GB.
- `nomic-embed-text` ≈ 0.3 GB.
- Total ≈ 7–8 GB, comfortable. For `qwen2.5:14b` the same arithmetic gives ≈ 9 GB weights + ≈ 6 GB cache, which is right at the edge. Start with 7b.

### 2.4 Output size and where it goes

Per album: two 768-float vectors. As float32 that's 6 KB; as JSON text more like 15–20 KB. For 25,212 albums: ~150 MB raw, **~400–500 MB as JSON in Kafka**. That's fine for a local topic. (If it ever matters, switch the value encoding to binary or Protobuf.)

The README rules out an off-the-shelf vector DB, and the index build is a later Phase 1 item. So this pipeline's output is a **Kafka topic, `embeddings`**, with `cleanup.policy=compact` so Kafka keeps the latest record per `album_id`. The next stage (index build / sharding) will be another consumer of that topic.

### 2.5 Message contracts

```
albums      key=album_id   value=Album    (input,  4 partitions, infinite retention)
embeddings  key=album_id   value=Result   (output, 4 partitions, compacted)
albums.dlq  key=album_id   value=Album    (albums we gave up on; header "error" says why)
```

The Go types (`Album`, `Tags`, `Result`) are in `internal/schema/schema.go` in Part 6.

---

## 3. Kubernetes, from zero to Jobs

### 3.1 The mental model

Kubernetes is a **control loop machine**. You don't tell it *do this*; you tell it *this is what should exist* (a declarative object), and a set of controllers continuously compare reality with that description and fix the difference.

The moving parts, on a single-node cluster all running on your machine:

| Component | Role |
|---|---|
| **API server** | The only thing anyone talks to (`kubectl`, controllers, your Go code). It validates objects and stores them. |
| **etcd** (k3s embeds SQLite instead) | The database behind the API server. |
| **Controllers** (kube-controller-manager) | Loops that make reality match objects: the Deployment controller, the **Job controller**, etc. |
| **Scheduler** | Picks a node for each unscheduled Pod based on resource requests, selectors, taints. |
| **kubelet** | The agent on each node. Starts containers (via containerd), runs probes, reports status. |
| **Container runtime** (containerd) | Actually runs the containers. |
| **Device plugin** | A DaemonSet that tells the kubelet "this node has N of resource X" (our GPU). |

Everything is an **object** with `apiVersion`, `kind`, `metadata` (name, namespace, labels), `spec` (what you want) and `status` (what is true now, written by controllers). `kubectl apply -f x.yaml` and `client.Create(...)` in Go are the same operation: send an object to the API server.

### 3.2 The objects we'll touch

| Object | What it is | Used for |
|---|---|---|
| **Namespace** | A folder for objects and a scope for RBAC | `semantic-grooves` |
| **Pod** | One or more containers sharing network and storage; the unit that is scheduled. Pods are disposable. | Everything runs in Pods, but you rarely create them directly |
| **Deployment** | "Keep N copies of this Pod running forever." Replaces failed pods. | Ollama (the GPU server) |
| **StatefulSet** | Like a Deployment but with stable names (`kafka-0`) and per-pod storage | Kafka |
| **Service** | A stable DNS name and virtual IP in front of pods | `kafka`, `ollama` |
| **Job** | "Run this Pod **to completion**, retrying on failure." | Ingest, embed workers, spawner |
| **CronJob** | Creates Jobs on a schedule | Possible later: periodic re-tagging |
| **PersistentVolumeClaim (PVC)** | A request for disk | Kafka data, Ollama models |
| **ConfigMap / Secret** | Config and credentials injected as env vars or files | Optional here |
| **ServiceAccount + Role + RoleBinding** | Identity and permissions for processes *inside* the cluster | Lets the spawner create Jobs |

Deployment vs Job is the key distinction: a Deployment's pods are *supposed to run forever* (an exit is a failure to heal); a Job's pods are *supposed to finish* (a clean exit is success, and then nothing restarts them). Your bootstrap pipeline is batch work, so Jobs. The README's online serving layer (search API, tagging service) will be Deployments later.

### 3.3 Jobs in depth

#### What a Job does

You submit a Job object containing a **pod template** and some counters. The Job controller:

1. Creates Pods from the template (labelled with `batch.kubernetes.io/job-name=<name>` and owned by the Job, so deleting the Job deletes its pods).
2. Keeps up to `parallelism` Pods running.
3. Counts successful and failed Pods.
4. Marks the Job **Complete** when the success rule is met, or **Failed** when a limit is hit.
5. Stops creating pods and leaves the finished Pods in place so you can read their logs, until the Job is deleted (manually or by `ttlSecondsAfterFinished`).

Rough timeline of `kubectl create -f job.yaml`: API server validates and stores → Job controller sees the new Job and creates Pod objects → scheduler assigns each Pod a node (checking CPU/memory/GPU fit) → kubelet pulls the image and starts the container → container runs → process exits → kubelet reports exit code → Job controller updates the Job's `status` and decides whether to create more Pods.

#### The fields that matter

| Field | Meaning | Our choice |
|---|---|---|
| `spec.template` | The pod template. **Immutable** once created. | one container, our worker image |
| `spec.template.spec.restartPolicy` | **Must** be `Never` or `OnFailure` for Jobs (never `Always`). `Never`: a failed pod stays as `Failed` for inspection and the controller creates a *new* pod. `OnFailure`: the kubelet restarts the container inside the same pod. | `Never` (required by `podFailurePolicy`, and leaves evidence behind) |
| `spec.parallelism` | Max pods running at once. | min(partitions with work, 4) |
| `spec.completions` | How many pods must succeed. **Unset** = "work queue" mode. | unset |
| `spec.completionMode` | `NonIndexed` (default; pods are interchangeable) or `Indexed` (each pod gets `JOB_COMPLETION_INDEX` 0..completions-1). | default |
| `spec.backoffLimit` | Failed pods tolerated before the Job is declared Failed. Default 6. Retries are spaced by exponential backoff (10 s, 20 s, 40 s … capped at 6 min). | 6 |
| `spec.activeDeadlineSeconds` | Wall-clock cap for the whole Job. When reached, running pods are killed and the Job fails with reason `DeadlineExceeded`. Takes precedence over `backoffLimit`. | 24 h |
| `spec.ttlSecondsAfterFinished` | Auto-delete the Job (and pods) this long after it finishes. Without it, finished Jobs pile up forever. | 1 h |
| `spec.podFailurePolicy` | Per-failure rules by exit code or pod condition: `FailJob` (give up now), `Ignore` (don't count toward `backoffLimit`), `Count`. Needs `restartPolicy: Never`. | exit 2 → `FailJob`; node drain → `Ignore` |
| `spec.suspend` | Create the Job paused; flip to false to start. Useful for queueing systems. | not used |
| `spec.selector` | Auto-generated. **Do not set it** by hand. | — |

**Job names must be unique** and a finished Job still occupies its name. That's why the Go spawner uses `GenerateName: "embed-worker-"`, and the API server appends a random suffix.

#### The three classic patterns

1. **One Job per work item.** Simple, but 25,000 Jobs would flood the API server. Not for us.
2. **Fixed completion count.** `completions: N` with interchangeable pods, or `completionMode: Indexed` where pod *i* handles shard *i* (pods read `JOB_COMPLETION_INDEX`). Good when work is pre-partitioned. Indexed Jobs map onto Kafka partitions neatly (pod *i* reads partition *i*), but kafka-go cannot combine "read exactly partition *i*" with consumer-group offset tracking, so you'd have to store your own checkpoints. That's the cost.
3. **Work queue.** `completions` **unset**, `parallelism: N`. Pods pull work from an external queue and exit when the queue is empty. The Job succeeds when *at least one pod exited 0 and all pods have terminated*; after the first success, no new pods are started. **This is what we use**, with Kafka as the queue and consumer-group offsets as the progress record.

Two catches with pattern 3.

**"Complete" does not mean "all work done".** Suppose workers A, B and C idle out and exit 0 while worker D crashes mid-partition. Once any pod has succeeded, the Job controller creates no replacements, so the Job can end `Complete` with messages still unprocessed (nothing is lost, as D's offsets were not committed, but the Job's status alone would mislead you). The spawner therefore re-checks Kafka lag after the Job finishes and fails loudly if any remains; re-running it picks up the rest.

**A Kafka consumer never "finishes" on its own.** Our worker therefore **exits cleanly after being idle for `IDLE_TIMEOUT` (60 s)**. That requires the topic to be fully populated *before* workers start, which Part 4 enforces by ordering.

#### What happens on failure

There are four layers of failure handling, from inner to outer; knowing which one is acting saves hours of debugging:

1. **In-process retries.** The worker retries a failed album 3 times with growing delays, then sends it to the DLQ and moves on, so one bad review cannot wedge a partition.
2. **Pod failure.** If the worker crashes (OOM, panic, SIGKILL) the pod ends `Failed`. The Job controller starts a replacement after a backoff; this consumes one `backoffLimit` credit. The replacement resumes from the last *committed* Kafka offset.
3. **Job failure.** More than `backoffLimit` failed pods → Job `Failed` (reason `BackoffLimitExceeded`). Or the deadline passes (`DeadlineExceeded`). Or an exit code matches a `FailJob` rule. Re-running the spawner later creates a new Job, which resumes from committed offsets, so no work is lost.
4. **Node/cluster events.** If the pod is evicted or the node drains, the pod gets a `DisruptionTarget` condition; our `Ignore` rule means that does not burn `backoffLimit`.

Useful status to read: `kubectl get job -o yaml` → `status.active`, `status.succeeded`, `status.failed`, `status.conditions[]` (types `Complete`, `Failed`; newer clusters also `SuccessCriteriaMet`, `FailureTarget`).

#### Resources: requests, limits, and the scheduler

- `requests` are what the scheduler reserves; `limits` are the cap enforced at runtime. Exceeding a **memory** limit gets the container OOM-killed (`OOMKilled`, exit 137). Exceeding a **CPU** limit only throttles.
- If no node can satisfy the requests, the pod stays `Pending` with an event like `0/1 nodes are available: Insufficient amd.com/gpu`. This is the single most common Job confusion; `kubectl describe pod` shows it.

### 3.4 GPUs in Kubernetes

Kubernetes knows only CPU and memory natively. Anything else (GPUs, FPGAs) is an **extended resource**, advertised by a **device plugin**:

1. The plugin runs as a DaemonSet (one pod per node) and talks to the kubelet over a local gRPC socket.
2. It reports "this node has N devices" under a resource name. For AMD that name is **`amd.com/gpu`** (for NVIDIA it'd be `nvidia.com/gpu`).
3. The scheduler treats it as an opaque integer: pods asking for `amd.com/gpu: 1` fit only on nodes with a free one.
4. When the pod starts, the kubelet calls the plugin's `Allocate`; the plugin answers with the device files to mount (`/dev/kfd`, `/dev/dri/renderD*`) and the container is started with access to them.

Rules that bite:

- Extended resources go under `resources.limits` and are **whole integers**. You can't ask for half a GPU. (Time-slicing/sharing exists for NVIDIA; not for your AMD card here.)
- A GPU held by one pod is invisible to others. A second pod requesting it stays `Pending`.
- A Deployment doing a rolling update briefly wants *two* pods, which deadlocks on a one-GPU node: the new pod waits for the GPU the old pod holds. Use `strategy: Recreate` for GPU Deployments (the Ollama manifest does).
- AMD needs no special container runtime (unlike NVIDIA's `nvidia-container-runtime`). The device plugin alone is enough.
- The ROCm user-space libraries must be *inside the image* (`ollama/ollama:rocm` includes them); the *kernel driver* (`amdgpu`, which you have) comes from the host.
- The RX 6800 XT is `gfx1030`, which ROCm supports natively. Some consumer cards (e.g. RX 6600/6700 series) report an unsupported architecture, and people set `HSA_OVERRIDE_GFX_VERSION=10.3.0` to make ROCm treat them as gfx1030. **You probably don't need this**; it's a troubleshooting step (7.4), not a setting to start with.

### 3.5 The design decision: how do Jobs use one GPU?

You have exactly **one** GPU. Two designs:

**(a) Shared GPU server (recommended, and what this guide builds).**
One long-running Ollama **Deployment** requests the GPU and exposes a Service. Job pods are **CPU-only clients** that call it over HTTP. Ollama queues and batches concurrent requests (`OLLAMA_NUM_PARALLEL=4`), so 4 worker pods really do make progress at once. The GPU is "shared" through Ollama's request queue.

**(b) Each Job pod requests the GPU directly.**
Each pod would run its own model and ask for `amd.com/gpu: 1`. With one GPU, a Job with `parallelism: 4` runs **one pod and leaves three `Pending`**. Parallelism silently collapses to 1, and each pod pays model-load time. Simpler to reason about, but it wastes the very thing Jobs give you.

The trade-off you accept with (a): the GPU's owner (the Ollama Deployment) is a long-lived service you must start before the Job and keep healthy. The README's architecture already points this way, since the online "tagging/embedding service" is a standing Deployment. Design (a) also lets the *online* tagging service reuse the same Ollama later.

---

## 4. The pipeline architecture

```
                        namespace: semantic-grooves
  ┌────────────────────────────────────────────────────────────────────────┐
  │                                                                        │
  │  [Job] ingest ──► Kafka topic "albums" (4 partitions, key=album_id)    │
  │  reads SQLite          │                                               │
  │  (hostPath, ro)        │ consumer group "embed-worker"                 │
  │                        ▼                                               │
  │  [Job] embed-worker ×4 (CPU pods, created by the spawner)              │
  │      │  1. POST /api/chat  (tag + vibe)   ┐                            │
  │      │  2. POST /api/embed (2 vectors)    ├──► [Deployment] ollama     │
  │      │                                    ┘     └─ 1 × amd.com/gpu     │
  │      ├──► Kafka topic "embeddings" (compacted, key=album_id)           │
  │      ├──► Kafka topic "albums.dlq" (poison / repeated failures)        │
  │      └──  commit offset on "albums" (only after the write succeeded)   │
  │                                                                        │
  │  [Job] spawner: reads group lag ──► creates the embed-worker Job ──►   │
  │               watches it until Complete/Failed                         │
  │               (ServiceAccount + Role: may create/watch Jobs)           │
  └────────────────────────────────────────────────────────────────────────┘
```

**Order of events for one full run**

1. Cluster, GPU plugin, Kafka and Ollama are up (long-lived).
2. Topics exist with the right settings.
3. `ingest` Job runs to completion → `albums` is fully populated.
4. `spawner` runs: measures per-partition lag, decides parallelism, creates the worker Job, watches it.
5. Workers drain the topic, then idle out and exit 0 → Job `Complete` → spawner re-checks lag; if it is 0 the spawner exits 0, otherwise it exits non-zero and you run it again.
6. `embeddings` now holds one record per album.

**Why partitions = 4 = `OLLAMA_NUM_PARALLEL`.** In a consumer group each partition goes to at most one consumer, so more than 4 workers on 4 partitions would idle. And Ollama only works on 4 requests at once, so more concurrency than that just lengthens its queue. Keeping those numbers aligned is the whole sizing story; change them together.

**Idempotency (what makes retries safe).** Kafka consumers give *at-least-once* delivery: a crash after publishing a result but before committing the offset makes the next worker redo that album. That's harmless because:

- The output key is `album_id`, and `embeddings` is compacted, so a duplicate replaces the earlier record instead of adding a second one.
- The LLM runs at `temperature: 0`, so the redo gives (nearly) the same answer.
- The order is **publish result, then commit offset**, never the reverse (committing first could lose an album forever on a crash).

**Backpressure.** There's nothing to build: workers only fetch the next album after finishing the current one, and Ollama's queue length is bounded by the number of workers.

---

## 5. Hands-on setup

Every command here is for your Arch machine. Lines starting with `sudo` need root. Run from the repo root unless stated.

### 5.0 Pre-flight

```bash
ls -l /dev/kfd /dev/dri/renderD*        # both should exist
id -nG | tr ' ' '\n' | grep -E 'render|video'   # you are already in both
curl -s localhost:11434/api/tags | head -c 100   # host Ollama is up (about to stop it)
```

**Stop host Ollama so it releases the GPU:**

```bash
sudo systemctl stop ollama
sudo systemctl disable ollama      # optional: stop it coming back at boot
```

(The in-cluster Ollama stores models in its own volume, so you'll pull `qwen2.5:7b` again, about 4.7 GB. Reusing the host's `~/.ollama` through a `hostPath` is possible, but couples the two installs' versions; I'd avoid it.)

### 5.1 Install k3s and kubectl

Why **k3s**: it's a single-binary Kubernetes that runs containers *directly on the host* with containerd, so passing `/dev/kfd` to a pod is straightforward. `kind` and `minikube`'s Docker driver run the "node" inside a Docker container, which adds a layer you'd have to punch the GPU device through.

```bash
curl -sfL https://get.k3s.io | sh -      # installs and starts a systemd service "k3s"
sudo systemctl status k3s --no-pager
```

(Arch also has k3s in the AUR; the official script works on systemd distros and is the least surprising.) Then give your user a kubeconfig:

```bash
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown "$USER": ~/.kube/config
sudo pacman -S kubectl           # or use: sudo k3s kubectl ...
kubectl get nodes                # one node, STATUS Ready
```

k3s ships with a `local-path` StorageClass (PVCs become folders on the host) and Traefik, which we don't use.

### 5.2 Make the cluster aware of the GPU

Install AMD's device plugin DaemonSet. The ROCm project's README documents this exact command (note it's `create`, not `apply`):

```bash
kubectl create -f https://raw.githubusercontent.com/ROCm/k8s-device-plugin/master/k8s-ds-amdgpu-dp.yaml
kubectl -n kube-system get pods | grep amdgpu      # wait for Running
```

Pinning the manifest to a commit or tag rather than `master` is wise once it works. There's also an optional "node labeller" DaemonSet (`k8s-ds-amdgpu-labeller.yaml` in the same repo) that adds GPU model labels to the node; we don't need it.

**Verify the node now advertises the GPU:**

```bash
kubectl get node -o jsonpath='{.items[0].status.allocatable}' ; echo
# expect to see "amd.com/gpu":"1" among the entries
```

If `amd.com/gpu` is missing, see 7.4.

### 5.3 Namespace

```bash
kubectl create namespace semantic-grooves
kubectl config set-context --current --namespace=semantic-grooves   # so you can drop "-n"
```

### 5.4 Kafka (single-node KRaft)

KRaft mode means Kafka manages its own metadata without ZooKeeper. The environment variables below are the set listed on Docker Hub's `apache/kafka` page for a single combined broker+controller node, with two changes for Kubernetes: the **advertised listener** must be a name pods can resolve (the page uses `localhost`, which only works inside the same container), and the data goes on a volume.

`deploy/kafka.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: semantic-grooves
spec:
  clusterIP: None            # headless: DNS returns the pod IP directly
  selector: { app: kafka }
  ports:
    - { name: broker, port: 9092 }
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
  namespace: semantic-grooves
spec:
  serviceName: kafka
  replicas: 1
  selector:
    matchLabels: { app: kafka }
  template:
    metadata:
      labels: { app: kafka }
    spec:
      securityContext:
        fsGroup: 1000        # the image runs as uid 1000; make the volume writable to it
      containers:
        - name: kafka
          image: apache/kafka:3.9.0     # pin a specific tag; Docker Hub lists "latest" as the moving one
          ports: [{ containerPort: 9092 }]
          env:
            - { name: KAFKA_NODE_ID, value: "1" }
            - { name: KAFKA_PROCESS_ROLES, value: "broker,controller" }
            - { name: KAFKA_LISTENERS, value: "PLAINTEXT://:9092,CONTROLLER://:9093" }
            # What clients are told to connect to. Must resolve from inside other pods:
            - { name: KAFKA_ADVERTISED_LISTENERS, value: "PLAINTEXT://kafka-0.kafka.semantic-grooves.svc.cluster.local:9092" }
            - { name: KAFKA_CONTROLLER_LISTENER_NAMES, value: "CONTROLLER" }
            - { name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" }
            - { name: KAFKA_CONTROLLER_QUORUM_VOTERS, value: "1@localhost:9093" }
            - { name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, value: "1" }
            - { name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, value: "1" }
            - { name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR, value: "1" }
            - { name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS, value: "0" }
            - { name: KAFKA_AUTO_CREATE_TOPICS_ENABLE, value: "false" }   # we create topics on purpose
            - { name: KAFKA_LOG_DIRS, value: "/var/lib/kafka/data" }
          readinessProbe:
            tcpSocket: { port: 9092 }
            initialDelaySeconds: 15
            periodSeconds: 5
          resources:
            requests: { cpu: 250m, memory: 768Mi }
            limits:   { memory: 1536Mi }
          volumeMounts:
            - { name: data, mountPath: /var/lib/kafka/data }
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests: { storage: 5Gi }
```

```bash
kubectl apply -f deploy/kafka.yaml
kubectl rollout status statefulset/kafka --timeout=180s
```

Create the topics. Two settings matter and are easy to miss:

- `albums` gets `retention.ms=-1`: Kafka's **default retention is 7 days**, after which unconsumed input would silently disappear, which would be nasty if a bootstrap run is paused for a week.
- `embeddings` gets `cleanup.policy=compact` (keep the latest record per key; see idempotency in Part 4).

```bash
# A function rather than a variable: zsh does not word-split "$VAR" into command + args.
topics() { kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }
topics --create --topic albums     --partitions 4 --replication-factor 1 --config retention.ms=-1
topics --create --topic embeddings --partitions 4 --replication-factor 1 --config cleanup.policy=compact
topics --create --topic albums.dlq --partitions 1 --replication-factor 1
topics --list
```

### 5.5 Ollama on the GPU

`deploy/ollama.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ollama-models
  namespace: semantic-grooves
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests: { storage: 20Gi }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ollama
  namespace: semantic-grooves
spec:
  replicas: 1
  strategy:
    type: Recreate            # a rolling update would deadlock waiting for the one GPU
  selector:
    matchLabels: { app: ollama }
  template:
    metadata:
      labels: { app: ollama }
    spec:
      containers:
        - name: ollama
          image: ollama/ollama:rocm        # the tag that bundles the ROCm (AMD) libraries
          ports: [{ containerPort: 11434 }]
          env:
            - { name: OLLAMA_NUM_PARALLEL, value: "4" }       # concurrent requests per model; match worker count
            - { name: OLLAMA_MAX_LOADED_MODELS, value: "2" }  # LLM + embedding model resident together
            - { name: OLLAMA_KEEP_ALIVE, value: "30m" }
            # Troubleshooting only (see 7.4); do not set unless Ollama falls back to CPU:
            # - { name: HSA_OVERRIDE_GFX_VERSION, value: "10.3.0" }
          resources:
            requests: { cpu: "1", memory: 4Gi }
            limits:
              memory: 12Gi
              amd.com/gpu: 1                # <-- the GPU. Extended resources live under limits.
          readinessProbe:
            httpGet: { path: /api/tags, port: 11434 }
            initialDelaySeconds: 10
            periodSeconds: 5
          volumeMounts:
            - { name: models, mountPath: /root/.ollama }
      volumes:
        - name: models
          persistentVolumeClaim: { claimName: ollama-models }
---
apiVersion: v1
kind: Service
metadata:
  name: ollama
  namespace: semantic-grooves
spec:
  selector: { app: ollama }
  ports:
    - { port: 11434, targetPort: 11434 }
```

```bash
kubectl apply -f deploy/ollama.yaml
kubectl rollout status deploy/ollama --timeout=300s

kubectl exec deploy/ollama -- ollama pull qwen2.5:7b
kubectl exec deploy/ollama -- ollama pull nomic-embed-text
```

**Prove the GPU is really used** (don't skip; CPU fallback is silent and an order of magnitude slower):

```bash
kubectl exec deploy/ollama -- ollama run qwen2.5:7b "say hi" >/dev/null
kubectl exec deploy/ollama -- ollama ps      # the PROCESSOR column should say "100% GPU"
kubectl logs deploy/ollama | grep -iE 'rocm|gpu|library' | head
```

### 5.6 Build and load the images

> **Stop here if you haven't done Part 6.** This step builds the images from the Go code and Dockerfile in 6.1–6.8. Create those files, run `go mod tidy`, then come back.

The Dockerfile is in 6.8. k3s's containerd cannot see Docker's local images, so build with Docker and import into containerd:

```bash
for c in ingest worker spawner; do
  docker build --build-arg CMD=$c -t semantic-grooves/$c:dev .
  docker save semantic-grooves/$c:dev | sudo k3s ctr images import -
done
sudo k3s ctr images ls | grep semantic-grooves
```

**Two gotchas.** (1) Use an explicit tag like `:dev`, never `:latest`: Kubernetes defaults `imagePullPolicy` to `Always` for `:latest` and would try (and fail) to pull from the internet. (2) We set `imagePullPolicy: IfNotPresent` explicitly everywhere. Skipping this step gives `ErrImagePull` / `ImagePullBackOff`.

### 5.7 First Job by hand: the ingest

Before the Go spawner exists, create a Job from YAML so you see exactly what the Go code will later generate. `deploy/ingest-job.yaml`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: ingest
  namespace: semantic-grooves
spec:
  backoffLimit: 2
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsUser: 1000        # your uid on the host (check: id -u); needed to read your files
      containers:
        - name: ingest
          image: semantic-grooves/ingest:dev
          imagePullPolicy: IfNotPresent
          args: ["-db", "/data/data.sqlite3", "-limit", "200"]     # smoke test; remove -limit for the full run
          env:
            - { name: KAFKA_BROKERS, value: "kafka.semantic-grooves.svc.cluster.local:9092" }
          volumeMounts:
            - { name: data, mountPath: /data, readOnly: true }
      volumes:
        - name: data
          hostPath:
            path: /home/griselda/Code/personal/go/semantic_grooves/data
            type: Directory
```

`hostPath` mounts a directory of the *node's* filesystem into the pod. That's acceptable on a single-node dev cluster (the node is your machine); on a multi-node cluster the pod could land on a node without the file.

```bash
kubectl apply -f deploy/ingest-job.yaml
kubectl wait --for=condition=complete job/ingest --timeout=120s
kubectl logs job/ingest
# Verify the messages arrived:
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic albums
```

You should see offsets summing to 200 across the four partitions. To do the full load later: `kubectl delete job ingest`, remove `-limit`, apply again. (Re-ingesting duplicates messages in `albums`; for a clean restart delete and recreate the topic first. Duplicates are harmless to the *output* thanks to compaction, but they cost GPU time.)

### 5.8 Second Job by hand: the workers

`deploy/worker-job.yaml`. This is the hand-written twin of what `buildJob` in 6.6 produces:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: embed-worker-manual
  namespace: semantic-grooves
spec:
  parallelism: 4                 # no "completions" -> work-queue mode
  backoffLimit: 6
  activeDeadlineSeconds: 86400
  ttlSecondsAfterFinished: 3600
  podFailurePolicy:
    rules:
      - action: FailJob          # exit code 2 = bad config; retrying is pointless
        onExitCodes: { containerName: worker, operator: In, values: [2] }
      - action: Ignore           # eviction / node drain doesn't count against backoffLimit
        onPodConditions: [{ type: DisruptionTarget, status: "True" }]
  template:
    spec:
      restartPolicy: Never       # required by podFailurePolicy
      containers:
        - name: worker
          image: semantic-grooves/worker:dev
          imagePullPolicy: IfNotPresent
          env:
            - { name: KAFKA_BROKERS, value: "kafka.semantic-grooves.svc.cluster.local:9092" }
            - { name: OLLAMA_URL,    value: "http://ollama.semantic-grooves.svc.cluster.local:11434" }
            - { name: TAG_MODEL,     value: "qwen2.5:7b" }
            - { name: EMBED_MODEL,   value: "nomic-embed-text" }
            - { name: IDLE_TIMEOUT,  value: "60s" }
          resources:
            requests: { cpu: 200m, memory: 128Mi }
            limits:   { memory: 512Mi }      # note: no GPU here; Ollama holds it
```

```bash
kubectl apply -f deploy/worker-job.yaml
kubectl get pods -l job-name=embed-worker-manual -w     # 4 pods Running
kubectl logs -f -l job-name=embed-worker-manual --prefix --tail=20
```

Watch the output topic fill:

```bash
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic embeddings
# Peek at one record (long line - the vectors):
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic embeddings --from-beginning --max-messages 1 | cut -c1-400
```

When the topic runs dry the pods print `no messages for 1m0s, exiting`, exit 0, and the Job becomes `Complete` (`kubectl get job`).

### 5.9 Permissions for the Go spawner

Inside a pod, your code authenticates to the API server as a **ServiceAccount**, and can do only what a **Role** grants. `deploy/spawner-rbac.yaml`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spawner
  namespace: semantic-grooves
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spawner
  namespace: semantic-grooves
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "list", "watch", "delete"]    # list+watch: waitForJob uses them
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spawner
  namespace: semantic-grooves
subjects:
  - { kind: ServiceAccount, name: spawner, namespace: semantic-grooves }
roleRef:
  { apiGroup: rbac.authorization.k8s.io, kind: Role, name: spawner }
```

```bash
kubectl apply -f deploy/spawner-rbac.yaml
kubectl auth can-i create jobs --as=system:serviceaccount:semantic-grooves:spawner   # -> yes
```

---

## 6. The Go implementation

### 6.1 Layout

Right now `server/` is a single `package main` with its own `go.mod`. Three programs (ingest, worker, spawner) that share message types want one module with a `cmd/` folder per binary and an `internal/` folder for shared code:

```
go.mod                      # module william-h.work/ingestion  (moved from server/; keep the name or rename later)
go.sum
Dockerfile
.dockerignore
cmd/
  ingest/    main.go fetch.go          # SQLite -> Kafka "albums"
  worker/    main.go                   # Kafka -> Ollama -> Kafka "embeddings"
  spawner/   main.go job.go lag.go     # creates + watches the worker Job
internal/
  schema/    schema.go                 # Album, Tags, Result, topic names
  ollama/    client.go                 # /api/chat and /api/embed
deploy/                                # the YAML from Part 5
```

Migration, roughly: `git mv server/go.mod server/go.sum .`, create the files below, `go mod tidy`, then delete `server/` (its job is replaced by `cmd/ingest`, which also fixes issues 1–5 from Part 1). `data/` stays gitignored. New dependencies pulled in by `go mod tidy`: `k8s.io/client-go`, `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/utils`.

### 6.2 `internal/schema/schema.go`: the contracts

The one place that defines topic names and the three payload types. If a field changes, it changes here for every program.

```go
// Package schema holds the wire types shared by ingest, worker and spawner.
package schema

import "time"

const (
	TopicAlbums     = "albums"       // input:  one message per album (tombstone)
	TopicEmbeddings = "embeddings"   // output: one message per processed album
	TopicDLQ        = "albums.dlq"   // albums we gave up on
	GroupTagger     = "embed-worker" // consumer group of the workers
)

// Album is the unit of work: one tombstone plus its review, with the
// many-to-many genre/artist rows already folded into slices.
type Album struct {
	AlbumID   string   `json:"album_id"` // tombstones.review_tombstone_id
	ReviewURL string   `json:"review_url"`
	Artists   []string `json:"artists"`
	Title     string   `json:"title"`
	Genres    []string `json:"genres"`
	Score     float64  `json:"score"`
	PubDate   string   `json:"pub_date"`
	Body      string   `json:"body"`
}

// Tags is what the LLM returns. Keep it in sync with the JSON schema in
// cmd/worker.
type Tags struct {
	Moods  []string `json:"moods"`
	Genres []string `json:"genres"`
	Energy int      `json:"energy"` // 1 (calm) .. 5 (intense)
	Vibe   string   `json:"vibe"`   // 1-2 sentences, no artist/album names
}

// Result is what the worker publishes to TopicEmbeddings.
type Result struct {
	AlbumID    string    `json:"album_id"`
	ReviewURL  string    `json:"review_url"`
	Artists    []string  `json:"artists"`
	Title      string    `json:"title"`
	Genres     []string  `json:"genres"` // from the dataset
	Score      float64   `json:"score"`
	Tags       Tags      `json:"tags"`
	VibeText   string    `json:"vibe_text"` // exact string that produced VibeVec
	VibeVec    []float32 `json:"vibe_vec"`
	ReviewVec  []float32 `json:"review_vec"`
	TagModel   string    `json:"tag_model"`
	EmbedModel string    `json:"embed_model"`
	DoneAt     time.Time `json:"done_at"`
}
```

### 6.3 `cmd/ingest`: SQLite → Kafka, one message per album

Changes versus your `server/` code, and why:

- **One row per album** via correlated subqueries (`group_concat` with the ASCII unit separator `\x1f`, which can't occur in names). This removes the 28,588-vs-25,212 row duplication.
- **Keyset pagination** (`WHERE id > ? ORDER BY id LIMIT ?`) instead of `LIMIT/OFFSET`. It is stable if the table changes and doesn't re-scan skipped rows. It also fixes the bug where the page number was used as the offset.
- **Batch writes** (`WriteMessages(ctx, msgs...)`) instead of one network round-trip per paragraph.
- **Key = `album_id`** with the `Hash` balancer: the same album always lands on the same partition.
- **`?mode=ro`** opens SQLite read-only, matching the read-only volume mount.
- **`-limit`** for smoke tests (the 200-album run in 5.7).

`cmd/ingest/fetch.go`:

```go
package main

import (
	"database/sql"
	"strings"

	"william-h.work/ingestion/internal/schema"
)

// One row per tombstone (album). Artists and genres are folded into a single
// column each, separated by the ASCII unit separator, so the JOIN fan-out
// that duplicated rows in the old query cannot happen.
const albumsQuery = `
SELECT t.review_tombstone_id
     , r.review_url
     , t.title
     , t.score
     , r.pub_date
     , r.body
     , (SELECT group_concat(a.name, char(31))
          FROM artist_review_map ar
          JOIN artists a ON a.artist_id = ar.artist_id
         WHERE ar.review_url = r.review_url)                          AS artists
     , (SELECT group_concat(g.genre, char(31))
          FROM genre_review_map g
         WHERE g.review_url = r.review_url)                           AS genres
FROM tombstones t
JOIN reviews r ON r.review_url = t.review_url
WHERE t.review_tombstone_id > ?          -- keyset pagination: stable and O(1) per page
ORDER BY t.review_tombstone_id
LIMIT ?`

func fetchAlbums(db *sql.DB, after string, limit int) ([]schema.Album, error) {
	rows, err := db.Query(albumsQuery, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var albums []schema.Album
	for rows.Next() {
		var a schema.Album
		var artists, genres sql.NullString
		if err := rows.Scan(&a.AlbumID, &a.ReviewURL, &a.Title, &a.Score,
			&a.PubDate, &a.Body, &artists, &genres); err != nil {
			return nil, err
		}
		a.Artists = splitUS(artists)
		a.Genres = splitUS(genres)
		albums = append(albums, a)
	}
	return albums, rows.Err()
}

func splitUS(s sql.NullString) []string {
	if !s.Valid || s.String == "" {
		return nil
	}
	return strings.Split(s.String, "\x1f")
}
```

`cmd/ingest/main.go`:

```go
// Command ingest reads albums out of SQLite and publishes one Kafka message
// per album. It replaces the per-paragraph fan-out in the old server/ code.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/segmentio/kafka-go"
	_ "modernc.org/sqlite"

	"william-h.work/ingestion/internal/schema"
)

func main() {
	dbPath := flag.String("db", "../data/data.sqlite3", "path to the SQLite file")
	limit := flag.Int("limit", 0, "stop after this many albums (0 = all); handy for a smoke test")
	flag.Parse()

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// mode=ro: the Job mounts the data directory read-only anyway.
	db, err := sql.Open("sqlite", "file:"+*dbPath+"?mode=ro")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(brokers, ",")...),
		Topic:        schema.TopicAlbums,
		Balancer:     &kafka.Hash{}, // same album_id -> same partition, always
		RequiredAcks: kafka.RequireAll,
		BatchSize:    100,
	}
	defer w.Close()

	const page = 500
	after, sent := "", 0
	for ctx.Err() == nil {
		n := page
		if *limit > 0 && *limit-sent < n {
			n = *limit - sent
		}
		if n == 0 {
			break
		}
		albums, err := fetchAlbums(db, after, n)
		if err != nil {
			log.Fatal(err)
		}
		if len(albums) == 0 {
			break
		}

		msgs := make([]kafka.Message, 0, len(albums))
		for _, a := range albums {
			if strings.TrimSpace(a.Body) == "" {
				continue
			}
			value, err := json.Marshal(a)
			if err != nil {
				log.Fatal(fmt.Errorf("marshal %s: %w", a.AlbumID, err))
			}
			msgs = append(msgs, kafka.Message{Key: []byte(a.AlbumID), Value: value})
		}
		if err := w.WriteMessages(ctx, msgs...); err != nil {
			log.Fatalf("publish: %v", err)
		}
		sent += len(albums)
		after = albums[len(albums)-1].AlbumID
		log.Printf("published %d albums (last: %s)", sent, after)
	}
	log.Printf("done: %d albums", sent)
}
```

### 6.4 `internal/ollama/client.go`: a tiny Ollama client

Two endpoints, plain `net/http`, no SDK dependency:

- `/api/chat` with a `format` JSON Schema → Ollama's *structured outputs* constrain decoding so the model's reply always parses (we still validate the content).
- `/api/embed` takes an **array** of inputs and returns one vector per input; we embed both texts for an album in one call.

`StatusError` keeps the HTTP code so you can later distinguish "retry" (503, connection refused) from "give up" (400).

```go
// Package ollama is a minimal client for the two Ollama endpoints we need.
package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	base string
	http *http.Client
}

func New(base string) *Client {
	// Generation on a shared GPU can queue for a while; keep the timeout generous.
	return &Client{base: base, http: &http.Client{Timeout: 5 * time.Minute}}
}

// StatusError carries the HTTP status so callers can decide whether to retry.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("ollama: HTTP %d: %s", e.Code, e.Body) }

func (c *Client) post(ctx context.Context, path string, in, out any) error {
	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return &StatusError{Code: resp.StatusCode, Body: string(body)}
	}
	return json.Unmarshal(body, out)
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatJSON asks the model for a reply constrained to schema and decodes it
// into out. schema is a JSON Schema document (Ollama "structured outputs").
func (c *Client) ChatJSON(ctx context.Context, model, system, user string, schema json.RawMessage, out any) error {
	req := map[string]any{
		"model":  model,
		"stream": false,
		"format": schema,
		"messages": []message{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
		"options": map[string]any{
			"temperature": 0,
			"num_ctx":     8192,
		},
		"keep_alive": "30m",
	}
	var resp struct {
		Message message `json:"message"`
	}
	if err := c.post(ctx, "/api/chat", req, &resp); err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(resp.Message.Content), out); err != nil {
		return fmt.Errorf("model returned invalid JSON: %w", err)
	}
	return nil
}

// Embed returns one vector per input, in order.
func (c *Client) Embed(ctx context.Context, model string, inputs []string) ([][]float32, error) {
	req := map[string]any{
		"model":      model,
		"input":      inputs,
		"truncate":   true, // safety net; callers should already stay under the model's context
		"keep_alive": "30m",
	}
	var resp struct {
		Embeddings [][]float32 `json:"embeddings"`
	}
	if err := c.post(ctx, "/api/embed", req, &resp); err != nil {
		return nil, err
	}
	if len(resp.Embeddings) != len(inputs) {
		return nil, fmt.Errorf("ollama: asked for %d embeddings, got %d", len(inputs), len(resp.Embeddings))
	}
	return resp.Embeddings, nil
}
```

### 6.5 `cmd/worker`: the Job's container

What to notice while reading it:

- **`signal.NotifyContext`**: Kubernetes stops pods with SIGTERM, then SIGKILL after 30 s. Cancelling the context lets the loop stop between albums. The in-flight album's offset is left uncommitted, so the next worker redoes it.
- **Exit codes are a contract with the Job.** Config errors exit `2` (the `podFailurePolicy` fails the whole Job immediately instead of burning six retries). Runtime errors exit `1` (the Job retries the pod). Idle completion exits `0`.
- **Idle timeout = "queue is empty".** `FetchMessage` runs under a `context.WithTimeout`; if nothing arrives for `IDLE_TIMEOUT`, the worker returns `nil`. The first fetch also includes joining the consumer group, hence the 60 s default; don't go below ~30 s.
- **Order: publish → commit.** `FetchMessage` (not `ReadMessage`) is used so the offset is committed explicitly, after the result is in Kafka (`RequireAll` acks).
- **`waitForOllama`** holds the worker until the GPU server answers `/api/tags`, so starting the Job while Ollama is still loading doesn't burn `backoffLimit`.
- **Poison handling.** Bad JSON goes straight to the DLQ (retrying can't fix it). Other failures get 3 attempts with growing delays, then the DLQ. In both cases the offset is committed, so a bad album can't wedge a partition.
- **`embedBodyChars`** (6,000) deliberately cuts the raw review before embedding, rather than leaving the server's `truncate` to do it silently.
- **`vibeDocument`** is the single place that defines the text behind `vibe_vec`. Change it deliberately: all existing vectors become stale.
- **Why the system prompt looks the way it does:** it asks for how the music *feels*, bans artist/album names in the vibe, and bounds the lists (3–6 moods, 1–3 genres). The JSON Schema repeats those bounds; the prompt explains them to the model, the schema enforces them.

```go
// Command worker consumes albums from Kafka, tags them with an LLM, embeds
// them, publishes the result, and exits once the topic has been idle for a
// while. That exit is what lets a Kubernetes Job complete.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"william-h.work/ingestion/internal/ollama"
	"william-h.work/ingestion/internal/schema"
)

const (
	exitConfig     = 2    // the Job's podFailurePolicy fails the whole Job on this
	embedBodyChars = 6000 // ~1.5k tokens; 8.5% of reviews are longer and lose their tail
)

type config struct {
	brokers    []string
	ollamaURL  string
	tagModel   string
	embedModel string
	idle       time.Duration
	maxRetries int
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() (config, error) {
	idle, err := time.ParseDuration(env("IDLE_TIMEOUT", "60s"))
	if err != nil {
		return config{}, fmt.Errorf("IDLE_TIMEOUT: %w", err)
	}
	return config{
		brokers:    strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		ollamaURL:  env("OLLAMA_URL", "http://localhost:11434"),
		tagModel:   env("TAG_MODEL", "qwen2.5:7b"),
		embedModel: env("EMBED_MODEL", "nomic-embed-text"),
		idle:       idle,
		maxRetries: 3,
	}, nil
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Print(err)
		os.Exit(exitConfig)
	}

	// SIGTERM is how Kubernetes asks a pod to stop (deletion, node drain...).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatalf("worker failed: %v", err) // exit 1 -> pod fails -> Job retries it
	}
	log.Print("worker finished cleanly")
}

func run(ctx context.Context, cfg config) error {
	p := &processor{cfg: cfg, llm: ollama.New(cfg.ollamaURL)}

	if err := p.waitForOllama(ctx); err != nil {
		return err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.brokers,
		GroupID:     schema.GroupTagger,
		Topic:       schema.TopicAlbums,
		StartOffset: kafka.FirstOffset, // only used when the group has no committed offset yet
		MinBytes:    1,
		MaxBytes:    10 << 20,
		// CommitInterval 0 (the default) makes CommitMessages synchronous.
	})
	defer reader.Close()

	// No Topic on the writer: each message names its own, so one writer
	// serves both the embeddings topic and the DLQ.
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
	defer writer.Close()

	done := 0
	for {
		// "Idle" means: nothing arrived for cfg.idle. The first fetch also
		// covers the consumer-group join, so keep IDLE_TIMEOUT >= ~30s.
		fetchCtx, cancel := context.WithTimeout(ctx, cfg.idle)
		msg, err := reader.FetchMessage(fetchCtx)
		cancel()

		switch {
		case err == nil:
		case ctx.Err() != nil:
			return ctx.Err() // told to stop: leave the in-flight offset uncommitted
		case errors.Is(err, context.DeadlineExceeded):
			log.Printf("no messages for %s, exiting after %d albums", cfg.idle, done)
			return nil
		default:
			return fmt.Errorf("fetch: %w", err)
		}

		out, err := p.handle(ctx, msg)
		if err != nil {
			return err // ctx cancelled mid-flight; do not commit
		}
		if err := writer.WriteMessages(ctx, out); err != nil {
			return fmt.Errorf("publish %s: %w", string(msg.Key), err)
		}
		// Commit only after the result is durably in Kafka. A crash between
		// the two lines re-does one album, which is harmless because the
		// output is keyed by album_id (see "Idempotency").
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		done++
		if done%25 == 0 {
			log.Printf("processed %d albums", done)
		}
	}
}

type processor struct {
	cfg config
	llm *ollama.Client
}

// waitForOllama blocks until the GPU server answers, so a Job that starts
// before Ollama is ready does not burn its backoffLimit.
func (p *processor) waitForOllama(ctx context.Context) error {
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.ollamaURL+"/api/tags", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		log.Printf("waiting for ollama at %s ...", p.cfg.ollamaURL)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// handle turns one input message into one output message: either a Result
// for TopicEmbeddings or, if the album cannot be processed, a DLQ record.
// It returns an error only when ctx was cancelled.
func (p *processor) handle(ctx context.Context, in kafka.Message) (kafka.Message, error) {
	var album schema.Album
	if err := json.Unmarshal(in.Value, &album); err != nil {
		return p.dlq(in, fmt.Errorf("bad json: %w", err)), nil // poison: retrying cannot help
	}

	var lastErr error
	for attempt := 1; attempt <= p.cfg.maxRetries; attempt++ {
		res, err := p.process(ctx, album)
		if err == nil {
			value, err := json.Marshal(res)
			if err != nil {
				return p.dlq(in, err), nil
			}
			return kafka.Message{Topic: schema.TopicEmbeddings, Key: in.Key, Value: value}, nil
		}
		if ctx.Err() != nil {
			return kafka.Message{}, ctx.Err()
		}
		lastErr = err
		log.Printf("album %s attempt %d/%d: %v", album.AlbumID, attempt, p.cfg.maxRetries, err)
		select {
		case <-ctx.Done():
			return kafka.Message{}, ctx.Err()
		case <-time.After(time.Duration(attempt*attempt) * 2 * time.Second):
		}
	}
	return p.dlq(in, lastErr), nil
}

func (p *processor) dlq(in kafka.Message, cause error) kafka.Message {
	log.Printf("sending %s to DLQ: %v", string(in.Key), cause)
	return kafka.Message{
		Topic:   schema.TopicDLQ,
		Key:     in.Key,
		Value:   in.Value,
		Headers: []kafka.Header{{Key: "error", Value: []byte(cause.Error())}},
	}
}

func (p *processor) process(ctx context.Context, a schema.Album) (schema.Result, error) {
	var tags schema.Tags
	err := p.llm.ChatJSON(ctx, p.cfg.tagModel, systemPrompt, userPrompt(a), tagsSchema, &tags)
	if err != nil {
		return schema.Result{}, fmt.Errorf("tagging: %w", err)
	}
	if len(tags.Moods) == 0 || strings.TrimSpace(tags.Vibe) == "" {
		return schema.Result{}, errors.New("tagging: model returned empty moods/vibe")
	}
	for i := range tags.Moods {
		tags.Moods[i] = strings.ToLower(strings.TrimSpace(tags.Moods[i]))
	}

	vibeText := vibeDocument(a, tags)
	// One call, two inputs -> two vectors. Both use the "document" prefix
	// that nomic-embed-text expects; queries will use "search_query: ".
	// The raw review is cut explicitly at embedBodyChars because the model's
	// context in Ollama is 2k tokens; better to choose where it ends than to
	// let the server truncate silently.
	vecs, err := p.llm.Embed(ctx, p.cfg.embedModel, []string{
		vibeText,
		"search_document: " + truncate(a.Body, embedBodyChars),
	})
	if err != nil {
		return schema.Result{}, fmt.Errorf("embedding: %w", err)
	}

	return schema.Result{
		AlbumID:    a.AlbumID,
		ReviewURL:  a.ReviewURL,
		Artists:    a.Artists,
		Title:      a.Title,
		Genres:     a.Genres,
		Score:      a.Score,
		Tags:       tags,
		VibeText:   vibeText,
		VibeVec:    vecs[0],
		ReviewVec:  vecs[1],
		TagModel:   p.cfg.tagModel,
		EmbedModel: p.cfg.embedModel,
		DoneAt:     time.Now().UTC(),
	}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") // drop a rune cut in half
}

// vibeDocument is the text that gets embedded as the album's "feel".
// Deliberately excludes artist and album names: we want vectors that cluster
// by mood, not by who made the record.
func vibeDocument(a schema.Album, t schema.Tags) string {
	return fmt.Sprintf("search_document: Mood: %s. Genre: %s. Energy: %d/5. %s",
		strings.Join(t.Moods, ", "),
		strings.Join(append(append([]string{}, a.Genres...), t.Genres...), ", "),
		t.Energy,
		strings.TrimSpace(t.Vibe),
	)
}

const systemPrompt = `You tag music album reviews for a mood-based search engine.
Describe how the music FEELS to a listener, not its history or the band's biography.
Rules:
- moods: 3 to 6 single lowercase words or short phrases (e.g. "melancholic", "hazy", "driving", "triumphant").
- genres: 1 to 3 lowercase genre names that fit the sound described.
- energy: integer 1 (very calm) to 5 (very intense).
- vibe: 1 or 2 plain sentences describing the emotional atmosphere. Do not mention the artist or album name.
Answer only with JSON matching the schema.`

func userPrompt(a schema.Album) string {
	return fmt.Sprintf("Artist: %s\nAlbum: %s\nListed genres: %s\n\nReview:\n%s",
		strings.Join(a.Artists, ", "), a.Title, strings.Join(a.Genres, ", "), truncate(a.Body, 24000))
}

// Ollama constrains decoding to this JSON Schema ("structured outputs"), so
// the reply always parses; we still validate the content.
var tagsSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "moods":  {"type": "array", "items": {"type": "string"}, "minItems": 3, "maxItems": 6},
    "genres": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 3},
    "energy": {"type": "integer", "minimum": 1, "maximum": 5},
    "vibe":   {"type": "string"}
  },
  "required": ["moods", "genres", "energy", "vibe"]
}`)
```

Sequential processing per pod is deliberate: it makes commit ordering trivial. Throughput comes from the number of pods, not goroutines per pod.

### 6.6 `cmd/spawner`: building and creating the Job in Go

`client-go` is the official Kubernetes client. The Go structs (`batchv1.Job`, `corev1.PodSpec` …) mirror the YAML field for field, so `buildJob` is a transliteration of `deploy/worker-job.yaml`. Compare them side by side.

`cmd/spawner/job.go`: pure object construction; no network, so it's trivially unit-testable:

```go
package main

import (
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

type jobConfig struct {
	Namespace   string
	Image       string
	Parallelism int32
	Brokers     string
	OllamaURL   string
	TagModel    string
	EmbedModel  string
	Deadline    time.Duration
}

// buildJob is the Go equivalent of a Job YAML manifest. Nothing here talks
// to the cluster; it only builds the object.
func buildJob(c jobConfig) *batchv1.Job {
	labels := map[string]string{
		"app.kubernetes.io/name":    "embed-worker",
		"app.kubernetes.io/part-of": "semantic-grooves",
	}

	container := corev1.Container{
		Name:            "worker", // podFailurePolicy refers to this name
		Image:           c.Image,
		ImagePullPolicy: corev1.PullIfNotPresent, // images are imported locally, never pulled
		Env: []corev1.EnvVar{
			{Name: "KAFKA_BROKERS", Value: c.Brokers},
			{Name: "OLLAMA_URL", Value: c.OllamaURL},
			{Name: "TAG_MODEL", Value: c.TagModel},
			{Name: "EMBED_MODEL", Value: c.EmbedModel},
			{Name: "IDLE_TIMEOUT", Value: "60s"},
		},
		// No GPU here: design (a). The GPU belongs to the Ollama Deployment.
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "embed-worker-", // the API server appends a random suffix
			Namespace:    c.Namespace,
			Labels:       labels,
		},
		Spec: batchv1.JobSpec{
			// No Completions: "work queue" Job. It succeeds once at least one
			// pod exited 0 and every pod has terminated.
			Parallelism:             ptr.To(c.Parallelism),
			BackoffLimit:            ptr.To(int32(6)),
			ActiveDeadlineSeconds:   ptr.To(int64(c.Deadline.Seconds())),
			TTLSecondsAfterFinished: ptr.To(int32(3600)),
			PodFailurePolicy: &batchv1.PodFailurePolicy{
				Rules: []batchv1.PodFailurePolicyRule{
					{ // exit code 2 = bad configuration: retrying is pointless
						Action: batchv1.PodFailurePolicyActionFailJob,
						OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
							ContainerName: ptr.To("worker"),
							Operator:      batchv1.PodFailurePolicyOnExitCodesOpIn,
							Values:        []int32{2},
						},
					},
					{ // node drain / preemption is not the app's fault: don't count it
						Action: batchv1.PodFailurePolicyActionIgnore,
						OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{
							{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue},
						},
					},
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever, // required by podFailurePolicy
					Containers:    []corev1.Container{container},
				},
			},
		},
	}
}

// describe is a one-line status summary for logs.
func describe(j *batchv1.Job) string {
	return fmt.Sprintf("job/%s active=%d succeeded=%d failed=%d",
		j.Name, j.Status.Active, j.Status.Succeeded, j.Status.Failed)
}
```

Notes: `ptr.To(...)` (from `k8s.io/utils/ptr`) exists because optional API fields are pointers, so "unset" and "zero" are distinguishable. `GenerateName` lets the API server pick a unique suffix.

`cmd/spawner/lag.go`: how much work is waiting? For each partition of `albums`: the log-end offset minus the group's committed offset. This is the same quantity autoscalers like KEDA use ("consumer lag").

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// partitionLag returns, per partition of topic, how many messages the group
// has not committed yet (log end offset minus committed offset).
func partitionLag(ctx context.Context, brokers []string, topic, group string) (map[int]int64, error) {
	cl := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}

	md, err := cl.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}
	if len(md.Topics) != 1 || md.Topics[0].Error != nil {
		return nil, fmt.Errorf("topic %q not available: %v", topic, md.Topics)
	}

	var ids []int
	var reqs []kafka.OffsetRequest
	for _, p := range md.Topics[0].Partitions {
		ids = append(ids, p.ID)
		// Ask for both ends. A field of PartitionOffsets stays -1 if its
		// request was not sent, which would make lag off by one.
		reqs = append(reqs, kafka.FirstOffsetOf(p.ID), kafka.LastOffsetOf(p.ID))
	}

	endResp, err := cl.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{topic: reqs},
	})
	if err != nil {
		return nil, fmt.Errorf("list offsets: %w", err)
	}
	committedResp, err := cl.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: ids},
	})
	if err != nil {
		return nil, fmt.Errorf("offset fetch: %w", err)
	}

	committed := map[int]int64{}
	for _, p := range committedResp.Topics[topic] {
		committed[p.Partition] = p.CommittedOffset // -1 when the group never committed
	}

	lag := map[int]int64{}
	for _, p := range endResp.Topics[topic] {
		from, ok := committed[p.Partition]
		if !ok || from < 0 { // the group never committed on this partition
			from = p.FirstOffset
		}
		lag[p.Partition] = p.LastOffset - from
	}
	return lag, nil
}

// backlog logs the per-partition lag and returns the total and the number of
// partitions that still have work.
func backlog(ctx context.Context, brokers []string, topic, group string) (total int64, busy int, err error) {
	lag, err := partitionLag(ctx, brokers, topic, group)
	if err != nil {
		return 0, 0, err
	}
	for p, l := range lag {
		log.Printf("partition %d: lag %d", p, l)
		total += l
		if l > 0 {
			busy++
		}
	}
	return total, busy, nil
}
```

`cmd/spawner/main.go`: ties it together. The flow is: **measure lag → pick parallelism → create Job → watch until a terminal condition → exit non-zero if the Job failed.**

- `newClientset` tries in-cluster config first (token mounted into the pod by Kubernetes) and falls back to your `~/.kube/config`, so the same binary runs in both places.
- `waitForJob` uses `watchtools.UntilWithSync`, which does a list, then a watch, and recovers if the watch connection drops. That's the robust way; a raw `Watch()` call ends silently whenever the server closes the connection (it does so periodically).
- The `Failed` check comes *after* the wait, so a failed Job produces a non-zero exit code for whatever runs the spawner.
- After a `Complete` Job it measures the lag **again** and exits non-zero if albums remain (the "Complete is not done" catch from 3.3).
- `-parallelism N` skips the Kafka lag check. Running the spawner from your laptop reaches the Kubernetes API fine, but Kafka tells clients to reconnect to `kafka-0.kafka.semantic-grooves.svc.cluster.local`, which your laptop can't resolve. Use the flag for local runs, or run the spawner in the cluster (6.9).

```go
// Command spawner measures the backlog on the albums topic and, if there is
// work, creates a Kubernetes Job sized to it, then follows the Job to the end.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	watchtools "k8s.io/client-go/tools/watch"

	"william-h.work/ingestion/internal/schema"
)

func main() {
	var (
		namespace  = flag.String("namespace", "semantic-grooves", "namespace to create the Job in")
		image      = flag.String("image", "semantic-grooves/worker:dev", "worker image")
		brokers    = flag.String("brokers", "kafka.semantic-grooves.svc.cluster.local:9092", "Kafka brokers as the WORKERS see them")
		kafkaAddr  = flag.String("spawner-brokers", "", "Kafka brokers as the SPAWNER sees them (default: same as -brokers)")
		ollamaURL  = flag.String("ollama", "http://ollama.semantic-grooves.svc.cluster.local:11434", "Ollama URL as the workers see it")
		tagModel   = flag.String("tag-model", "qwen2.5:7b", "LLM used for tagging")
		embedModel = flag.String("embed-model", "nomic-embed-text", "embedding model")
		maxWorkers = flag.Int("max-workers", 4, "upper bound on Job parallelism (keep <= OLLAMA_NUM_PARALLEL)")
		fixedPar   = flag.Int("parallelism", 0, "force this parallelism and skip the Kafka lag check (useful from your laptop, where broker DNS names do not resolve)")
		deadline   = flag.Duration("deadline", 24*time.Hour, "Job activeDeadlineSeconds")
		dryRun     = flag.Bool("dry-run", false, "print the plan, create nothing")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	if *kafkaAddr == "" {
		*kafkaAddr = *brokers
	}

	kafkaBrokers := strings.Split(*kafkaAddr, ",")

	// 1. How much work is there? (skipped when -parallelism is forced)
	parallelism := int32(*fixedPar)
	if parallelism == 0 {
		total, busy, err := backlog(ctx, kafkaBrokers, schema.TopicAlbums, schema.GroupTagger)
		if err != nil {
			log.Fatal(err)
		}
		if total == 0 {
			log.Print("nothing to do: group is caught up")
			return
		}

		// 2. One worker per partition that has work, capped. More workers
		// than partitions would just sit idle in the consumer group.
		parallelism = int32(min(busy, *maxWorkers))
		log.Printf("backlog %d messages over %d partitions -> parallelism %d", total, busy, parallelism)
	}

	job := buildJob(jobConfig{
		Namespace:   *namespace,
		Image:       *image,
		Parallelism: parallelism,
		Brokers:     *brokers,
		OllamaURL:   *ollamaURL,
		TagModel:    *tagModel,
		EmbedModel:  *embedModel,
		Deadline:    *deadline,
	})
	if *dryRun {
		log.Printf("dry run: would create a Job with parallelism %d", parallelism)
		return
	}

	// 3. Talk to the API server.
	cs, err := newClientset()
	if err != nil {
		log.Fatal(err)
	}
	created, err := cs.BatchV1().Jobs(*namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		log.Fatalf("create job: %v", err)
	}
	log.Printf("created %s", created.Name)

	// 4. Follow it until it finishes.
	final, err := waitForJob(ctx, cs, *namespace, created.Name)
	if err != nil {
		log.Fatalf("waiting for %s: %v", created.Name, err)
	}
	log.Print(describe(final))
	if jobCondition(final, batchv1.JobFailed) {
		log.Fatalf("job %s FAILED (inspect: kubectl -n %s describe job %s)", final.Name, *namespace, final.Name)
	}
	log.Printf("job %s completed", final.Name)

	// 5. Do not trust "Complete" alone. In work-queue mode the Job completes
	// once any pod exited 0 and all pods are gone: if some workers idled out
	// and another crashed mid-partition, the Job can be Complete with work
	// left. The Kafka lag is the real success signal.
	if *fixedPar == 0 {
		left, _, err := backlog(ctx, kafkaBrokers, schema.TopicAlbums, schema.GroupTagger)
		if err != nil {
			log.Fatal(err)
		}
		if left > 0 {
			log.Fatalf("job completed but %d albums are still unprocessed: run the spawner again", left)
		}
		log.Print("all albums processed")
	}
}

// newClientset works both inside a pod (service-account token, env vars set
// by the kubelet) and on your laptop (~/.kube/config or $KUBECONFIG).
func newClientset() (*kubernetes.Clientset, error) {
	cfg, err := rest.InClusterConfig()
	if errors.Is(err, rest.ErrNotInCluster) {
		path := os.Getenv("KUBECONFIG")
		if path == "" {
			home, _ := os.UserHomeDir()
			path = filepath.Join(home, ".kube", "config")
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", path)
	}
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func jobCondition(j *batchv1.Job, t batchv1.JobConditionType) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == t && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// waitForJob watches one Job object until it is Complete or Failed. The
// "UntilWithSync" helper lists, then watches, and transparently re-lists if
// the watch connection drops.
func waitForJob(ctx context.Context, cs kubernetes.Interface, ns, name string) (*batchv1.Job, error) {
	lw := cache.NewListWatchFromClient(
		cs.BatchV1().RESTClient(), "jobs", ns,
		fields.OneTermEqualSelector("metadata.name", name),
	)

	var last *batchv1.Job
	_, err := watchtools.UntilWithSync(ctx, lw, &batchv1.Job{}, nil,
		func(ev watch.Event) (bool, error) {
			if ev.Type == watch.Deleted {
				return false, fmt.Errorf("job %s was deleted", name)
			}
			j, ok := ev.Object.(*batchv1.Job)
			if !ok {
				return false, fmt.Errorf("unexpected object %T", ev.Object)
			}
			last = j
			log.Print(describe(j))
			return jobCondition(j, batchv1.JobComplete) || jobCondition(j, batchv1.JobFailed), nil
		})
	return last, err
}
```

### 6.7 Unit-testing the Job object

Because `buildJob` is pure you can assert on it without a cluster; for client-go interactions, `k8s.io/client-go/kubernetes/fake` provides an in-memory clientset (`fake.NewSimpleClientset()`; check the current constructor name in your client-go version, as it has been evolving). A first test worth having:

```go
func TestBuildJobGPUFree(t *testing.T) {
	j := buildJob(jobConfig{Namespace: "x", Image: "i", Parallelism: 4, Deadline: time.Hour})
	if j.Spec.Completions != nil {
		t.Fatal("work-queue mode requires Completions to be unset")
	}
	c := j.Spec.Template.Spec.Containers[0]
	if _, ok := c.Resources.Limits["amd.com/gpu"]; ok {
		t.Fatal("workers must not request the GPU; Ollama owns it")
	}
}
```

### 6.8 `Dockerfile`: one file for all three binaries

```dockerfile
# Build:  docker build --build-arg CMD=worker -t semantic-grooves/worker:dev .
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD
# CGO off: modernc.org/sqlite is pure Go, so the binary is fully static.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
```

`.dockerignore` (important: `data/` is hundreds of MB and would be copied into every build):

```
data
.git
deploy
docs
```

Use the Go image tag that matches your `go.mod` (`go 1.27.0`); I couldn't verify from here that a `golang:1.27` tag exists on your registry mirror, so adjust if the pull fails.

### 6.9 Running the spawner (in the cluster)

`deploy/spawner-job.yaml`: a Job whose pod runs the spawner under the `spawner` ServiceAccount from 5.9. A Job that creates a Job is perfectly normal.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  generateName: spawner-
  namespace: semantic-grooves
spec:
  backoffLimit: 0                  # the spawner is a one-shot; a human decides about re-running
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      serviceAccountName: spawner
      restartPolicy: Never
      containers:
        - name: spawner
          image: semantic-grooves/spawner:dev
          imagePullPolicy: IfNotPresent
          args: ["-max-workers", "4"]
```

Note `generateName` means you must use `kubectl create -f`, not `apply` (apply needs a fixed name).

The full run, start to finish:

```bash
kubectl delete job ingest --ignore-not-found
# edit deploy/ingest-job.yaml: remove the "-limit", "200" args, then:
kubectl apply -f deploy/ingest-job.yaml
kubectl wait --for=condition=complete job/ingest --timeout=30m

kubectl create -f deploy/spawner-job.yaml
kubectl logs -f job/$(kubectl get jobs -o name | grep spawner | tail -1 | cut -d/ -f2)
```

Expected spawner output (approximately):

```
partition 0: lag 6291
...
backlog 25212 messages over 4 partitions -> parallelism 4
created embed-worker-x7k2p
job/embed-worker-x7k2p active=4 succeeded=0 failed=0
...
job embed-worker-x7k2p completed
```

---

## 7. Operating it

### 7.1 Is it fast enough? Measure before the full run

I haven't measured your GPU, so treat this as a back-of-envelope **guess** and replace it with real data:

- Per album: ≈1,200 tokens of prompt to process (~1 s of prefill), ≈120 tokens of JSON to generate (~2 s at maybe 50–70 tokens/s on a 7B Q4 model), plus ~0.3 s embedding ≈ **3–4 s per album serially**.
- 25,212 albums × 3.5 s ≈ **24 hours** if requests ran strictly one at a time.
- Ollama batches concurrent requests, so with 4 workers expect a multiple of that throughput, perhaps 2–3× in aggregate, i.e. **roughly 8–12 hours**. Possibly better, possibly worse.

**How to measure:** run the 200-album smoke test (5.7), start the worker Job, and time it from the first log line to `exiting after 200 albums`. Then extrapolate: `(seconds / 200) × 25,212`.

If it's too slow, in order of preference: (1) a smaller tagger (`qwen2.5:3b`, `llama3.2:3b`); (2) cap the review text sent to the LLM (`truncate(a.Body, 8000)`: the opening of a review usually carries the verdict); (3) sweep `OLLAMA_NUM_PARALLEL` (2, 4, 8) while watching `ollama ps` and VRAM. An overnight run is perfectly fine for a one-time bootstrap, and because offsets are committed you can stop and resume at any time.

### 7.2 Day-to-day observation

```bash
kubectl get job,pods                                # state of everything
kubectl describe job embed-worker-xxxxx             # events: why pods were created/killed
kubectl logs -f -l job-name=embed-worker-xxxxx --prefix
kubectl logs <pod> --previous                       # logs of a crashed container (restartPolicy OnFailure)
kubectl top pods                                    # CPU/memory (k3s bundles metrics-server)
kubectl get events --sort-by=.lastTimestamp | tail -20

# Progress = committed offsets vs log end:
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group embed-worker

# Inspect rejected albums:
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic albums.dlq --from-beginning --property print.headers=true --max-messages 5 | cut -c1-300
```

### 7.3 Re-running and starting over

- **Resume / re-run**: just create another worker Job (run the spawner again). It continues from committed offsets; if lag is 0 the spawner exits with "nothing to do".
- **Re-process everything** (new prompt or model): reset the group's offsets, e.g. `kafka-consumer-groups.sh --group embed-worker --topic albums --reset-offsets --to-earliest --execute` (the group must have no active members), then run the spawner. Compaction means the new records replace the old ones per `album_id`.
- **Clean slate**: `kubectl delete job --all`, delete and recreate the topics, re-ingest.
- **Delete everything**: `kubectl delete namespace semantic-grooves` removes all objects in it (PVCs and thus the Kafka data and downloaded models too).

### 7.4 Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| `amd.com/gpu` not in the node's allocatable | Device plugin pod not running or can't see `/dev/kfd` | `kubectl -n kube-system logs ds/amdgpu-device-plugin-daemonset` (check actual name with `get ds`); confirm `ls /dev/kfd /dev/dri` on the host and that the `amdgpu` kernel module is loaded (`lsmod \| grep amdgpu`) |
| Ollama pod `Pending`, event `Insufficient amd.com/gpu` | The GPU is taken or the plugin isn't registered | `kubectl describe pod`; make sure no *old* Ollama pod still holds the GPU (Recreate strategy), and host Ollama is stopped (`nvtop`/`rocm-smi` to see who uses VRAM) |
| `ollama ps` shows `100% CPU` | Ollama didn't detect the GPU | Check `kubectl logs deploy/ollama` for ROCm discovery messages. For some consumer cards, set `HSA_OVERRIDE_GFX_VERSION=10.3.0`; the RX 6800 XT is natively gfx1030, so try this last. Confirm the container sees `/dev/kfd` (`kubectl exec deploy/ollama -- ls -l /dev/kfd /dev/dri`) |
| Ollama crashes / OOM-killed | Memory limit too low for model loading | Raise `limits.memory`, `kubectl describe pod` shows `OOMKilled` |
| Worker pods `ErrImagePull` / `ImagePullBackOff` | Image not imported into k3s, or tag is `:latest` | Redo 5.6; keep `:dev` and `imagePullPolicy: IfNotPresent` |
| Worker logs `waiting for ollama at ...` forever | Ollama not Ready or wrong Service name | `kubectl get endpoints ollama`; `kubectl exec deploy/ollama -- ollama list` |
| Worker can't connect to Kafka / `no such host` | Using `localhost:9092` inside a pod, or the advertised listener doesn't match | Brokers must be `kafka.semantic-grooves.svc.cluster.local:9092`; check `KAFKA_ADVERTISED_LISTENERS` |
| Worker exits immediately "no messages", Job completes with nothing done | Ingest hadn't finished, or the group already committed to the end | Check `kafka-consumer-groups.sh --describe`; run ingest first |
| Worker is killed after exactly `IDLE_TIMEOUT` on first start | Group join took longer than the idle window | Raise `IDLE_TIMEOUT` |
| `model "qwen2.5:7b" not found` (HTTP 404 from Ollama) | Models pulled into a different Ollama (host vs pod) | `kubectl exec deploy/ollama -- ollama list` |
| Ingest: `unable to open database file` | hostPath wrong or the pod's uid can't read it | Check the path in the Job, `type: Directory`, and `runAsUser` = your uid (yours is 1000 here). Your database is in `journal_mode=delete`, so a read-only open works; if you ever switch it to WAL, a read-only mount can fail and you'd add `&immutable=1` to the DSN |
| Job `Failed`, reason `BackoffLimitExceeded` | Workers kept crashing | `kubectl describe job`; `kubectl logs` on a Failed pod |
| Job `Failed`, reason `DeadlineExceeded` | Run took longer than `activeDeadlineSeconds` | Raise `-deadline`; just re-run, progress is kept |
| Pods `Pending` forever with 4 workers | Something requested the GPU in the worker pods (design (b)) | Workers must not have `amd.com/gpu` |
| `Error from server (Forbidden): jobs.batch is forbidden` in spawner | RBAC missing | Re-check 5.9, `serviceAccountName`, and `kubectl auth can-i` |

### 7.5 kubectl cheat sheet

```bash
kubectl get <kind> [-o wide|yaml|name] [-l label=value] [-w]
kubectl describe <kind> <name>          # the "Events:" section at the bottom is gold
kubectl logs <pod> [-f] [-c container] [--previous] [-l selector --prefix]
kubectl exec -it <pod> -- sh
kubectl apply -f file.yaml | kubectl create -f file.yaml | kubectl delete -f file.yaml
kubectl wait --for=condition=complete job/<name> --timeout=...
kubectl port-forward svc/ollama 11434:11434     # reach a Service from your laptop
kubectl api-resources                           # every kind the server knows
kubectl explain job.spec.podFailurePolicy       # built-in field documentation
```

`kubectl explain` is worth knowing: it prints the schema and docs for any field, from your cluster's actual version.

---

## 8. What comes next

This completes the Phase 1 items "ingestion service", "LLM prompt schema", "GPU worker throughput sizing", "idempotency guarantees" and "embedding generation". Remaining and adjacent:

- **Index build** (Phase 1): a consumer of `embeddings` that writes shards. Another Job; you can reuse the spawner pattern. Your custom sharding and scatter-gather work starts from this topic.
- **Mood vocabulary**: the LLM emits free-form moods. After the first run, cluster or count them (`SELECT` over the topic), pick a canonical list, and feed it back into the prompt for consistency. Plan on one re-run.
- **Autoscaling on lag (Phase 5)**: [KEDA](https://keda.sh)'s `ScaledJob` creates Jobs from a Kafka lag trigger, which is exactly what the hand-written spawner does by hand. Writing the spawner first is the right way to understand what KEDA automates.
- **Indexed Jobs variant**: `completionMode: Indexed` + `JOB_COMPLETION_INDEX` → partition index, with your own offset checkpoints, if you want strict pod-to-partition affinity.
- **Metrics (Phase 6)**: expose per-album latency and DLQ counts from the worker via a Prometheus `/metrics` endpoint.
- **Online path (Phase 3)**: new reviews arrive one at a time; the same worker code runs as a Deployment (`for {}` without the idle exit) against the same Ollama. The idle-exit is the *only* thing that makes the worker batch-shaped, so a small change (treat `IDLE_TIMEOUT=0` as "wait forever", which the current code does *not* do) turns it into the online tagging service.
- **Query side**: embed the user's query with the `search_query: ` prefix using the same `nomic-embed-text`, and rank by cosine similarity against `vibe_vec` (and optionally `review_vec`), then apply your MMR re-ranking from the Python version.
