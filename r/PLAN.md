# picotel.R Port Plan

Port picotel Python v0.3.0 (`src/picotel.py` @ `07acab6`) to R as a single-file,
minimal-dependency library under `r/`, mirroring the structure and work-package
(WP) breakdown proven on the `taylor/go-port` branch.

All implementation work is delegated to subagents running **Claude Sonnet 4.6**
(`model: sonnet`). The orchestrator coordinates, merges, verifies, and commits.

## Authoritative references for every subagent

1. `src/picotel.py` — the source of truth for behavior. Port semantics exactly;
   document any deviation forced by R.
2. `tests/test_*.py` — the behavioral contract. Port the relevant test file(s)
   for your WP to testthat.
3. The Go port for porting decisions already made:
   `git show taylor/go-port:go/picotel.go` and
   `git log taylor/go-port --grep 'WP<n>'` for your WP's rationale.
4. This file — the frozen design decisions below. Do not relitigate them.

## Deliverables

- `r/picotel.R` — single sourceable file; the vendorable artifact
- `r/tests/testthat/test-*.R` — testthat (edition 3) suite
- `r/tests/run.R` — standalone runner (`Rscript r/tests/run.R`)
- `.github/workflows/r.yml` — CI
- `r/README.md` + root `README.md` language-index entry

## Frozen design decisions

**D1 — Dependency policy.** Runtime: base R (≥ 4.0) + the `curl` package only.
Base R cannot do HTTP POST or TLS; `curl` is the ecosystem floor, has zero R
dependencies, and its options map 1:1 onto picotel's TLS env vars. Load it
lazily via `requireNamespace("curl")` at send time so `source("picotel.R")`
never fails without it. JSON encoding is hand-rolled (no jsonlite): the OTLP
value space is small (strings, bools, ints, doubles, arrays, nested attr
lists) — write `.picotel_to_json()` with correct string escaping. Test-only
deps (do not leak into picotel.R): `testthat`, `withr`, `webfakes`, `curl`.

**D2 — Namespacing.** `source()` dumps everything into the caller's
environment. Public API uses plain snake_case names; all internals are
dot-prefixed (`.picotel_*`); mutable process state (sender, circuit breaker,
caches) lives in one `.picotel_state <- new.env(parent = emptyenv())`.

**D3 — API mapping.** Snake_case S3; spans/log records are environment-backed
objects (class `picotel_span` / `picotel_log_record`) so callers can mutate
attributes inside `with_span()`.

| Python | R |
|---|---|
| `Resource(attrs)` | `picotel_resource(attributes)` |
| `InstrumentationScope` | `picotel_scope(name, version = "", attributes = NULL)` |
| `Span(...)` | `picotel_span(trace_id, name, ...)` |
| `with Span(...) as s:` | `with_span(..., expr)` — see D4 |
| `LogRecord(...)` | `picotel_log_record(body, ...)` |
| `span.send()` / `log.send()` | `span_send(span, ...)` / `log_send(log, ...)` |
| `send_spans` / `send_logs` | `send_spans(endpoint, resource, spans, scope = NULL, timeout = 2)` / `send_logs(...)` |
| `new_trace_id` / `new_span_id` / `now_ns` | same names |
| `Span.Kind` / `Span.Status` / `LogRecord.Severity` | constant lists: `SpanKind$CLIENT`, `SpanStatus$OK`, `Severity$INFO` (integer values per OTLP) |
| `TRACEPARENT` sentinel object | `TRACEPARENT <- new.env()` sentinel; detect with `identical()` |
| `PicotelConfigError` | condition class `picotel_config_error` via `stop(errorCondition(msg, class = "picotel_config_error"))` |
| `OTLPHandler` (logging) | condition handler for `globalCallingHandlers()` — see D7 |

**D4 — Context-manager analogue.** `with_span(trace_id, name, ..., expr)`
evaluates `expr` with the span bound (pass a function `function(span) ...`, or
expose the span via an argument — implementer picks the cleaner idiom and
keeps it consistent). Must mirror `Span.__enter__`/`__exit__` exactly: set
`start_time_ns` on entry if unset, set `end_time_ns` and send on exit
(including on error — use `on.exit()`/`tryCatch`), match Python's
status-on-exception behavior precisely (read `__exit__` before assuming),
never raise from the send path.

**D5 — Timestamps.** `now_ns()` returns a **double** of integer nanoseconds.
At ~1.75e18 the double ulp is 256 ns — below base R's actual clock precision
(µs) — so arithmetic like `now_ns() + 1e6` stays exact enough. Never encode
through `as.character()`/default formatting: serialize with
`sprintf("%.0f", x)` to get the exact integer string OTLP JSON requires
(`str(ns)` in Python). Same for `intValue` attribute encoding. Document the
sub-µs precision limit in r/README.md.

**D6 — Senders.** R has no threads.
- *Sync (default):* direct `curl::curl_fetch_memory()` POST, 2 s timeout;
  circuit breaker trips permanently after 5 consecutive failures
  (process-lifetime, state in `.picotel_state`), matching `_SyncSender`.
- *Async (`PICOTEL_ASYNC` truthy):* dedicated `curl::new_pool()`;
  `submit()` is non-blocking — `curl::multi_add()` + `curl::multi_run(timeout
  = 0)` to pump; cap pending at 256 with a single error-level message per
  overflow episode (mirror `_queue_full_warned`); `picotel_flush(timeout)`
  pumps until the pool drains or timeout. **Documented deviation:** transfers
  progress only during submit/flush pumps, not in a background thread; fork
  recovery (Python's `_ForkSafeLock`) is N/A — note the limitation around
  `parallel::mclapply` instead.
- Mode is decided once per process at first send (lazy init cached in
  `.picotel_state`), mirroring Python's import-time `_get_sender()`.

**D7 — Logging integration.** Port `OTLPHandler` as
`otlp_condition_handler(endpoint = NULL, resource = NULL, attributes = NULL)`
returning a handler suitable for `globalCallingHandlers(message = h, warning
= h)` plus a documented `withCallingHandlers()` usage. Severity mapping:
message→INFO(9), warning→WARN(13), error condition→ERROR(17); replicate
`OTLPHandler.emit()`'s attribute merging and `trace_id`/`span_id` extraction
(from condition fields) as closely as the conditions system allows. This is
the most interpretive WP — list every deviation in r/README.md, as the Go
port did for slog.

**D8 — Testability seam.** Config/TLS/header logic must be pure functions
returning plain values (e.g. `.picotel_tls_options(signal)` returns a named
list of curl options; the send path applies them via
`curl::handle_setopt(h, .list = opts)`). curl handles are opaque — never
design a function whose only observable effect is on a handle.

**D9 — Conventions.** One conventional commit per WP: `feat(r): …` /
`docs(r): …`, mirroring the Go branch's messages. Full suite green before
every commit. No amends. Env-var tests must save/restore via
`withr::local_envvar()`.

## Work packages

Mirrors `taylor/go-port` (WP1–WP6 + docs). Python line refs are to
`src/picotel.py`.

| WP | Scope | Port from | Tests to port |
|---|---|---|---|
| **WP1 — Scaffold** | `r/picotel.R` skeleton: full frozen public API signatures with `stop("TODO(WPn)")` bodies; *fully implement* `.picotel_prefix()`, `.picotel_env()`, `.picotel_is_disabled()`, ID/time helpers (`new_trace_id`, `new_span_id`, `now_ns` per D5), constants, `.picotel_state`, sender plumbing skeleton; `r/tests/` harness + `run.R`; `.github/workflows/r.yml` (model on `git show taylor/go-port:.github/workflows/go.yml`; r-lib/actions, R release + oldrel) | Go WP1 commit `ee48964`; py lines 44–100, 1088–1123 | new: `test-helpers.R` (IDs hex/length/uniqueness, now_ns monotone-ish & magnitude, prefix/env remapping, disabled detection) |
| **WP2 — Env config + traceparent** | `.picotel_endpoint(signal)` (precedence, `/v1/*` appending, percent-decoding semantics — check Go WP2 notes), `.picotel_resource_from_env()`, `.picotel_parse_traceparent()`, TRACEPARENT sentinel resolution, caching in `.picotel_state` | py 1124–1141, 1228–1286; Go commit `e25a44c` | `test_env_config.py` → `test-env-config.R`; `test_traceparent.py` → `test-traceparent.R` |
| **WP3 — OTLP JSON encoding + validation** | `.picotel_to_otlp_value()`, `.picotel_attributes_to_otlp()`, `.picotel_span_to_list()`, `.picotel_log_to_list()`, `.picotel_validate_span()`, hand-rolled `.picotel_to_json()` (D1). R-specific cases: NA vs NULL, length-1 vs length-n vectors (R has no scalars — a length-n vector is an OTLP array, length-1 is scalar; NA handling must be explicit), factor→string, named-list nesting | py 1287–1450; Go commit `f1c4cfb` | `test_to_otlp_value.py`, `test_attributes_to_otlp.py`, `test_span_to_dict.py` → `test-encode.R` |
| **WP4 — Transport, TLS, headers** | `.picotel_parse_headers()`, `.picotel_tls_options(signal)` per D8 (CA precedence: signal-specific > general; `PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY` never prefix-remapped, short-circuits CA but keeps mTLS; client cert/key signal-agnostic; combined-PEM support; https-only gating), `.picotel_post_json(url, body, timeout, signal)` via curl | py 1142–1227 + delivery code in send paths; Go commit `c10a2f3` | `test_env_config.py` TLS/header sections + `test_delivery.py` → `test-tls.R`, `test-transport.R` (webfakes for live HTTP; TLS via pure option-list assertions) |
| **WP5 — Senders + flush** | `.picotel_sync_submit()` with circuit breaker, async pool sender per D6, `picotel_flush(timeout)`, lazy `.picotel_get_sender()` | py 710–1086; Go commit `85558d9` (read its commit message — classification rationale carries over) | `test_async_sender.py` → `test-sender.R` (skip thread/fork cases; add pool-pump and overflow-episode cases) |
| **WP6 — Public API integration** | `send_spans`/`send_logs` (disabled-check → endpoint resolution → `picotel_config_error` when unconfigured → per-span validate/drop with logged error → payload → post), span/log constructors + `with_span` (D4) + `span_send`/`log_send` (silently skip when unconfigured — only batch senders raise), `otlp_condition_handler` (D7) | py 101–460, 461–709; Go commit `9a71533` | `test_send_spans.py`, `test_send_logs.py`, `test_high_level_apis.py`, `test_integration.py` → `test-send.R`, `test-lifecycle.R`, `test-handler.R`, `test-integration.R` |
| **WP7 — Docs** | `r/README.md` (mirror `git show taylor/go-port:go/README.md` structure: install-by-copy via curl/source, quickstart, env-var table, sending modes, **differences-from-Python table**, testing); root `README.md` Languages entry; roxygen-style comment headers on public functions | Go commit `a800a58` | — (prose; orchestrator reviews) |

## Orchestration

Single shared file ⇒ phases are sequential at merge points; parallelism only
where stub regions are disjoint, using `isolation: "worktree"`.

- **Phase A:** WP1 (one agent). Orchestrator verifies suite + CI file, commits.
  The WP1 skeleton's section markers (`# --- WP<n> ---`) define the disjoint
  regions later agents fill.
- **Phase B:** WP2 ∥ WP3 (two agents, worktrees — disjoint stub regions +
  separate test files). Orchestrator merges, runs full suite, commits each WP
  separately (WP2 then WP3). Fall back to sequential if merge conflicts.
- **Phase C:** WP4 ∥ WP5 (same pattern; WP5 treats the send fn as an opaque
  closure, so it doesn't depend on WP4).
- **Phase D:** WP6 (one agent — integrates everything).
- **Phase E:** WP7 (one agent).

Every subagent prompt must include: the WP row above, the frozen decisions
D1–D9, the authoritative-references list, the explicit instruction to read the
corresponding Go commit for prior porting decisions, and the definition of
done below.

**Definition of done (every WP):** assigned stubs implemented with no edits
outside the assigned region; ported tests cover the same cases as the Python
originals (note any intentionally skipped case in a comment with reason);
`Rscript r/tests/run.R` fully green; no new runtime dependency beyond D1.

**Orchestrator verification per phase:** run the full suite locally; eyeball
the diff for region containment and dependency creep; commit per D9; after
Phase E, run CI on the pushed branch.

## Known hazards (surface these in prompts)

- R vector semantics: `length(x) > 1` ⇒ OTLP array; length-1 ⇒ scalar; decide
  and test NA, NULL, NaN, ±Inf explicitly (Go encodes NaN/±Inf per proto3 —
  see `f1c4cfb`).
- Integer overflow: R ints are 32-bit; large counts arrive as doubles — the
  `intValue` path must accept whole-number doubles and encode via
  `sprintf("%.0f", …)`.
- `Sys.setenv("")` does not unset a variable; tests must use `Sys.unsetenv()`
  (via `withr::local_envvar(VAR = NA)`).
- curl handle reuse across sends: create fresh handles per request (circuit
  breaker + TLS options vary per signal) unless profiling says otherwise.
- The e2e suite (`tests-e2e/`) is out of scope for this branch, as it was for
  Go.

## Out of scope

Packaging picotel as a CRAN package (DESCRIPTION/NAMESPACE), gRPC, metrics,
batching, sampling — same non-goals as Python and Go.

> This plan file coordinates the subagent team during the port and should be
> dropped (or moved to a PR description) before merge.
