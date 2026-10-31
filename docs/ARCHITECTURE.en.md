# Architecture

[繁體中文](ARCHITECTURE.md) | **English**

## 1. Components

| Component | Role |
|---|---|
| **Agent Hub** | The only public service. Serves the frontend, stores conversations, relays questions to agents, and streams answers back |
| **Agent** | An independent service that owns its data and retrieval logic; exposes the unified `/a2a/*` interface via agentkit and is itself stateless |
| **agentkit** | The shared contract: agent-side routes (`server.py`), a client for calling other agents (`client.py`), the delegation loop (`delegate.py`), and the LLM client (`gateway.py`) |
| **LLM Gateway** | An Anthropic Messages API–compatible model service; all agents share one configuration |
| **registry** | `agentkit/registry.py`, the single source of truth for every agent's id, port, and upstream/downstream relations |

Why a Hub layer instead of having the frontend call each agent directly:

1. **No changes to existing services**: cross-port browser calls would require CORS on every agent; same-origin relaying through the Hub avoids that.
2. **Conversations need a home**: state lives in the Hub, which sends the history to the agent each turn. Agents don't each need persistence, and the same conversation can be continued by a different agent.
3. **Unified status**: whether an agent is down, busy, or not ready is shown in one place.
4. **Only one public port**: agents are not exposed; even attachment PDFs are proxied by the Hub (`GET /hub/file/{agent}/{doc_id}`).

## 2. A2A Contract (protocol `a2a/1`)

After mounting `build_router(spec)`, each agent provides:

| Endpoint | Purpose |
|---|---|
| `GET /a2a/card` | Card: identity, capabilities, status, what it's blocked on, who it can call, example questions |
| `GET /a2a/health` | Dependency checks (hard deadline, guaranteed fast response) |
| `GET /a2a/version` | agentkit version and source hash loaded at startup |
| `GET /a2a/tools` | Anthropic tool schema and system prompt (for upstreams to mount as tools) |
| `POST /a2a/tool/{name}` | Runs one tool call, always returns `{"ok": bool, ...}` |
| `POST /a2a/search` | Unified retrieval, returns `{hits, raw}` |
| `POST /a2a/message` | Unified chat (SSE) |
| `GET /a2a/peers` | Current status of its own downstreams |

### Answer modes

`/a2a/message` is controlled by two booleans:

| `use_tools` | `delegate` | Behavior |
|---|---|---|
| false | false | Retrieval + LLM summary, no tool calls |
| true | false | Tool loop with only its own tools |
| — | true | Tool loop + downstream agents' tools |

`no_llm: true` takes precedence over the above and returns only retrieval results. The Hub also switches to this mode automatically when it detects the LLM is unavailable.

"Use my own tools" and "ask other agents" are two different permissions — the latter sends the question to another service.
When delegation is not allowed there are two safeguards: downstreams never appear in the model's tool list or system prompt, and even if the model fabricates a downstream tool name, it is rejected before execution.

### SSE event order

```
meta → results → trace* → delta* → error? → done
```

| Event | Content |
|---|---|
| `meta` | Who is answering and in which mode |
| `results` | Retrieval cards — **always sent before the LLM is called** |
| `trace` | Tool / delegation trace: step, target, arguments, success, latency |
| `delta` | Answer streamed token by token |
| `error` | LLM or downstream failure with a suggested action; `busy` / `timeout` / `cancelled` flags distinguish the cause |
| `done` | End of turn, with usage statistics |

The Hub sends an extra `hub` event first (`conv_id`, `turn_id`, `request_id`).

### Card format

Each `hits[]` entry is `{id, title, subtitle, body, score, why[], badges[], blocks[], links[], raw}`:

- `blocks[]`: three types — `kv` (label / value), `text`, `table` — so the frontend renders every agent's cards with one renderer
- `links[]`: links to source files, always proxied through the Hub
- `raw`: the original row, untouched; normalization exists only for rendering and never replaces the source data
- In `kv`, `null` means "not filled" and `0` means "filled with 0"; the two are never merged anywhere

## 3. Collaboration Between Agents

### Two calling styles

| Interface | Shape | When to use |
|---|---|---|
| `<agent>__<tool>` | Structured arguments | You know which record you need |
| `ask_<agent>` | One natural-language sentence | The question is in the other agent's domain but the right tool is unclear, or the other agent needs to delegate further |

`ask_` keeps agents that "can only chat, tools not ready yet" usable — they can still delegate to their own downstreams.

### Cycle prevention

The delegation loop only mounts the registry's `downstream` agents for the model, so the call graph is a one-way directed acyclic graph.
Upstream calls are rejected with "not your downstream". Leaf nodes (`downstream=()`) are the safest shape.

### Prefetch

Small local models often answer directly without calling any tool when "their own retrieval results happen to contain relevant part numbers", and prompting can't fix that.
So the approach is deterministic instead:

1. When the question matches a downstream's `tags` (`AgentRef.tags`), call that agent's `/a2a/search` directly first (no LLM, fast).
2. Put the results into the **question** itself, so the data is in front of the model before it speaks.
3. The downstream's cards are sent to the screen as `results` (`append: true`), each tagged with its source agent.

`tags` is therefore a functional field and should contain words users actually type.

### Loop safeguards

- **Tool execution is decided by `tool_use` blocks in the content**, not by `stop_reason` (some gateways return the wrong one).
- **Overly long tool results are halved by row count**, not truncated by characters — truncated JSON makes the model output garbage.
- **Repeated calls**: after the same tool is called twice with the same arguments, it is removed from the model's list so the remaining steps go to other sub-questions.
- **Forced answer**: if steps run out without an answer, tools are withdrawn and a plain completion turns the gathered data into an answer, so the screen is never blank.
- **Rescuing plain-text tool calls**: if the model writes a tool call or answer as JSON in text, `gateway.py` detects and restores it.

### Degradation is the norm

- Downstream offline → `{"ok": false, "offline": true}`, no exception
- Tool `ready=False` → appears on the card but is not mounted for the model
- A downstream's data limitations live in `ToolSpec.description` and are passed verbatim to the upstream model, so upstream answers carry those caveats

## 4. Hub Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /` · `/manual` · `/metrics` | Chat page, manual, metrics dashboard |
| `GET /hub/agents?fresh=1` | Cards for all agents (including offline ones) |
| `GET /hub/graph` | Call graph |
| `GET /hub/health` | Status of the Hub, LLM, each agent, database, and backups |
| `GET /hub/deployment` | agentkit version consistency check across services |
| `POST /hub/message` | Main entry: relays to an agent (SSE) and stores the conversation |
| `POST /hub/search/{agent}` | Unified retrieval (no LLM) |
| `POST /hub/tool/{agent}/{name}` | Run a tool directly |
| `GET /hub/file/{agent}/{doc_id}` | Proxy an agent's source attachment |
| `GET /hub/conversations` · `GET/PATCH/DELETE /hub/conversations/{id}` | List, read, rename, delete conversations |
| `GET /hub/conversations/{id}/metrics` | Per-turn latency and usage for one conversation |
| `GET /hub/metrics?days=7` | Metrics dashboard data |

## 5. Conversation Storage

- **Storage**: SQLite (WAL), with all access in `store.py`. Each turn has a `turn_id` and `status` (`done` / `error` / `timeout` / `cancelled`); failed or interrupted turns still keep the cards and text gathered so far.
- **Ownership**: the browser receives an httponly `hub_session` cookie on its first request; programmatic callers can send `X-Session-Id`. Listing, reading, and deleting only work on your own session. This prevents accidentally viewing others' conversations and is **not a login system**.
- **Resilience**: the database is checked on startup; a corrupted file is quarantined and a fresh database is started; online backups run and are verified automatically.

## 6. Time, Concurrency, and Cancellation

**Per-question time budget**: the clock starts when the Hub receives a question (180 s by default). Remaining time is passed down to agents, downstreams, and the LLM via the `X-A2A-Deadline-Ms` header, reserving 2 s per hop so the innermost layer expires first and explains why. On expiry, generation stops and data already gathered is kept.

**Admission control** (counted per process):

| Where | Default | When full |
|---|---|---|
| Hub chat | 10 concurrent, 8 queued, 5 s wait | SSE `error` (`busy: true`), no empty conversation created |
| Agent retrieval / tools | 4 concurrent | Same as above |
| LLM calls | 2 concurrent per process | Slot is returned as soon as the model step finishes |
| Downstream streams | 10 concurrent | Same as above |

Busy is never treated as a service failure.

**Cancellation**: the user presses Stop → the browser disconnects → the Hub cancels its stream to the agent → the agent closes the LLM request and downstream calls. No new tool calls or LLM requests are started after stopping.

**Health checks**: dependency checks have a 2.5 s hard deadline and a cache; on timeout the previous result is returned and marked stale. Only one check runs at a time (single-flight) so checks don't pile up when a database hangs.

## 7. Observability

- The Hub creates a `request_id` (`X-A2A-Request-ID`) per turn; statistics reported by downstreams are merged and de-duplicated on it.
- Each turn records total latency, time to first card / first token, queueing time, tool and downstream call counts, LLM request count, and reported token usage. Metrics never include questions, answers, or tool arguments.
- Unreported usage is recorded as `null`, not 0, and `usage_complete` indicates whether it is complete.
- The `/metrics` page shows answer count, failure rate, mean / P95 latency, agent ranking, daily volume, and the 10 slowest turns.

## 8. agentkit Version Consistency

Each agent's image bundles an agentkit snapshot. Every service records the agentkit version and the SHA-256 of all its source files (line endings normalized first) at startup and reports them via `/a2a/version`.

```bash
python agentkit/version.py --sha256                          # hash of this release
python scripts/package_agentkit.py                           # package as a reproducible ZIP
python scripts/check_deployment.py --hub-url http://127.0.0.1:8600   # check all services; exit 1 on mismatch
python scripts/check_deployment.py --agents rfq,partrag --json       # check only these
```

Results are `ok`, `mismatch` (different content), `unversioned` (no version endpoint), `unavailable` (unreachable), or `invalid` (malformed response).

## 9. Dependency Locking

`requirements.txt` (production) and `requirements-dev.txt` (development) cover Python 3.12 on Windows and Linux, pinning all direct and transitive dependencies with SHA-256 hashes (`--require-hashes`).
After editing `requirements*.in`, regenerate them with `python scripts/lock_dependencies.py`.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `LLM_BASE_URL` | `http://127.0.0.1:4000` | LLM gateway URL |
| `LLM_API_KEY` | — | Sent as `Authorization: Bearer` |
| `LLM_MODEL` / `LLM_MODEL_FALLBACK` | — | Primary model; fallback model when the primary doesn't exist |
| `LLM_TIMEOUT` | 240 | Timeout for a single LLM request (s) |
| `LLM_PROBE_TTL` / `LLM_PROBE_TIMEOUT` | 60 / 10 | Cache and timeout for LLM health probes (s) |
| `A2A_MAX_TOKENS` | `LLM_MAX_TOKENS` or 9000 | Total thinking + answer budget for the delegation loop |
| `A2A_MAX_STEPS` | 5 | Max rounds of tool calls per question |
| `A2A_MAX_STEPS_DELEGATE` | 0 (use the above) | Separate step limit for delegation mode |
| `A2A_MAX_CONSECUTIVE_FAILS` | 3 | After this many consecutive tool failures, withdraw tools and answer directly |
| `A2A_PREFETCH_CHARS` | 3000 | Max prefetched data inserted into the question |
| `A2A_<ID>_URL` | `http://127.0.0.1:<port>` | URL of a given agent |
| `A2A_HUB_PORT` | 8600 | Hub port |
| `A2A_STATUS_DEADLINE` / `A2A_STATUS_TTL` | 2.5 / 15 | Dependency check deadline and cache (s) |
| `A2A_STATUS_MAX_STALE` / `A2A_STATUS_STUCK` | 60 / 30 | Max time to reuse a stale result; time before a check is considered stuck (s) |
| `A2A_SEARCH_CONCURRENCY` / `_MAX_WAITING` / `_WAIT_S` | 4 / 8 / 5 | Concurrency, queue, and wait for agent retrieval and tools |
| `A2A_LLM_CONCURRENCY` / `_MAX_WAITING` / `_WAIT_S` | 2 / 8 / 5 | Concurrency, queue, and wait for LLM slots |
| `A2A_STREAM_CONCURRENCY` / `_MAX_WAITING` / `_WAIT_S` | 10 / 8 / 5 | Concurrency, queue, and wait for downstream stream connections |
| `A2A_DEADLINE_HOP_MARGIN_S` | 2 | Seconds reserved from the remaining time at each hop |
| `A2A_DEADLINE_MIN_LLM_S` | 5 | Don't start an LLM call with less than this many seconds left |
| `HUB_MESSAGE_BUDGET_S` | 180 | Total time limit per question (s) |
| `HUB_MESSAGE_CONCURRENCY` / `_MAX_WAITING` / `_WAIT_S` | 10 / 8 / 5 | Concurrency, queue, and wait for Hub chat |
| `HUB_DIAG_WAIT_S` | 3 | Max seconds a question waits for the probe when LLM status is unknown |
| `HUB_DB_PATH` | `data/hub.db` | Conversation database |
| `HUB_LEGACY_JSON` | `data/conversations.json` | Legacy conversation file (migrated automatically on first start) |
| `HUB_BACKUP_DIR` | `data/backups` | Backup directory |
| `HUB_BACKUP_EVERY_H` / `HUB_BACKUP_KEEP` | 24 / 14 | Backup interval (hours, 0 = disabled) and number of copies kept |
