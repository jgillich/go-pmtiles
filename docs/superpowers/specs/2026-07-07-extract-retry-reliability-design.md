# Extract Download Retry Reliability

**Date:** 2026-07-07
**Status:** Approved (pending implementation)

## Problem

`pmtiles extract` downloads large extracts over HTTP range requests using 4
concurrent goroutines. It currently has:

- **No HTTP timeout** — `OpenBucket` hardcodes `http.DefaultClient`, which has
  no timeout. A stalled connection hangs forever.
- **No retry logic** — any single network error (EOF, connection reset,
  timeout) in any goroutine propagates to the `errgroup` and kills the entire
  extract. There is no resume; the user must restart from scratch.

Users report `Failed to extract, unexpected EOF` errors on large extracts.
These are often transient network issues that a per-range retry would recover
from without restarting the whole download.

## Goal

Make the tile-data download resilient to transient network failures by
retrying **failed ranges** (not the whole extract) with exponential backoff.
Add a configurable HTTP timeout so stalled connections fail fast and become
retryable.

## Non-goals

- Resume across process restarts (checkpoints, temp files). Out of scope.
- Retrying metadata fetches (header, root dir, leaf dirs, metadata). These
  remain single-attempt, though they now benefit from the HTTP timeout.
- Changes to `serve`, `sync`, `convert`, or other commands.

## Design

### 1. HTTP client with configurable timeout

**`bucket.go`:**

- Add a setter on `HTTPBucket`:

  ```go
  func (b *HTTPBucket) SetHTTPClient(c HTTPClient) { b.client = c }
  ```

- Change `OpenBucket` to return `&HTTPBucket{bucketURL, http.DefaultClient}`
  (pointer) for the `http`/`https` case, instead of the current value return.
  This is required so the type-assertion to `*HTTPBucket` in `Extract` can
  mutate the client. This is a safe change: `Bucket` is an interface and the
  existing method set is identical on the pointer receiver.

**`extract.go`:**

- After `OpenBucket` returns, in `Extract`, configure the client:

  ```go
  if hb, ok := bucket.(*HTTPBucket); ok {
      hb.SetHTTPClient(&http.Client{Timeout: httpTimeout})
  }
  ```

The timeout applies to all requests (header, dirs, metadata, tiles), but only
tile fetches get the retry loop. Metadata fetches remain single-attempt but
now have a timeout — a strict improvement.

### 2. Retry loop in `downloadPart`

The current `downloadPart` closure (extract.go:509-530) is split into:

- `downloadRangeOnce(or overfetchRange) error` — the existing body
  (NewRangeReader → OffsetWriter → CopyN loop → Close), unchanged in logic.
- `downloadPart(or overfetchRange) error` — a retry loop wrapping
  `downloadRangeOnce`.

```go
downloadPart := func(or overfetchRange) error {
    var lastErr error
    for attempt := 0; attempt <= maxRetries; attempt++ {
        if attempt > 0 {
            delay := backoffDuration(attempt, retryBackoff)
            logger.Printf("retrying range (src=%d, len=%d) attempt %d/%d after %v: %v",
                or.Rng.SrcOffset, or.Rng.Length, attempt, maxRetries, delay, lastErr)
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(delay):
            }
        }
        err := downloadRangeOnce(or)
        if err == nil {
            return nil
        }
        lastErr = err
        if !isRetryableError(err) {
            return err
        }
    }
    return fmt.Errorf("range src=%d len=%d failed after %d retries: %w",
        or.Rng.SrcOffset, or.Rng.Length, maxRetries, lastErr)
}
```

#### Retry classification (`isRetryableError`)

**Retryable:**

- `io.ErrUnexpectedEOF`, `io.EOF` (partial read mid-stream)
- Network errors: `net.OpError` (via `errors.As`), `*url.Error` where
  `urlErr.Timeout()` returns true, or `*url.Error` wrapping a connection-reset
  (`errors.As` to `*net.OpError` with `Op == "read"` and a `syscall.ECONNRESET`)
- HTTP 408 Request Timeout
- HTTP 5xx (500, 502, 503, 504)
- HTTP 429 Too Many Requests

**Not retryable:**

- `context.Canceled`, `context.DeadlineExceeded` (user cancelled / overall
  deadline exceeded)
- HTTP 4xx other than 408/429 (400, 401, 403, 404, 405, 416, …) — permanent
- `nil` (no error)

**Default:** any unclassified error is treated as non-retryable (fail fast
rather than retry an unknown failure mode).

Classification uses `errors.Is` / `errors.As` for wrapped errors. The function
is named `isRetryableError(err error)` per Go convention.

#### Backoff (`backoffDuration`)

Exponential with jitter:

```
delay = min(base * 2^(attempt-1) + jitter, 30s)
```

- `base` = `retryBackoff` flag (default 1s)
- `jitter` = random in `[0, base)`
- Cap at 30s to avoid very long waits

#### Progress bar on retry

The shared `bar` is wired into the copy via `io.MultiWriter`. On retry,
re-transferred bytes advance the bar past the expected total (bar may show
>100%). This is cosmetic and honest — those bytes were re-transferred over the
network. Acceptable trade-off vs. the complexity of per-range progress
reconciliation.

#### Thundering herd

With the default `--download-threads=4`, a server outage causes all 4
goroutines to fail near-simultaneously and retry in lockstep on attempt 1.
Jitter on the backoff desyncs them from attempt 2 onward. No initial jitter is
added before attempt 0 (to avoid slowing normal operation). Users targeting
flaky servers can set `--download-threads=1` to serialize retries.

#### Bounded total retry time

With `maxRetries=5` (default) and `retryBackoff=1s` (default), the maximum
added latency per range is the sum of backoffs: ~1+2+4+8+16+30 = 61s (the
final attempt is capped at 30s). For a 4-goroutine extract where one range is
the long pole, total wall time can grow by roughly one minute. This is
acceptable for large extracts and is CLI-tunable via `--max-retries` and
`--retry-backoff`.

#### Output file safety

The output file is pre-allocated via `Truncate` and the `OffsetWriter` writes
at a fixed `DstOffset`. On retry, the same range is re-written at the same
offset, cleanly overwriting the partial data from the failed attempt.

**Invariant:** a successful `downloadRangeOnce` always writes exactly
`or.Rng.Length` bytes at `DstOffset`. This holds because:

1. `NewRangeReader` requests `bytes=offset-(offset+length-1)`; a successful
   206 response returns exactly `Length` bytes (the server guarantees this for
   a valid range request).
2. `io.CopyN(..., cd.Wanted)` reads exactly `cd.Wanted` bytes or returns an
   error (triggering a retry). On success, the full `Wanted` byte count is
   written.
3. The sum of all `cd.Wanted` + `cd.Discard` in a range equals `or.Rng.Length`.

Therefore, if a failed attempt wrote `X < Length` bytes at `DstOffset`, the
successful retry writes `Length` bytes at the same `DstOffset`, overwriting
`[0, Length)` — which covers the `X` partial bytes. No stale bytes remain. No
`Truncate`-to-zero is needed between attempts.

The header is still written last (existing behavior), so a cancelled extract
fails `pmtiles verify`.

### 3. CLI flags and signature changes

**`main.go` — `Extract` struct** gains three fields:

```go
MaxRetries    int           `default:"5" help:"Maximum number of retries per failed range request during tile download (0 = no retries)"`
HTTPTimeout   time.Duration `default:"60s" help:"HTTP client timeout for range requests; 0 = no timeout"`
RetryBackoff  time.Duration `default:"1s" help:"Base delay for exponential backoff between retries (max 30s, with jitter)"`
```

Note: `maxRetries=5` means 6 total attempts (the loop runs `attempt <= maxRetries`).
A code comment documents this to prevent off-by-one drift.

**`extract.go` — `Extract()` signature** gains three params:

```go
func Extract(ctx context.Context, logger *log.Logger, bucketURL string, key string,
    minzoom int8, maxzoom int8, regionFile string, bbox string, output string,
    downloadThreads int, overfetch float32, dryRun bool,
    maxRetries int, httpTimeout time.Duration, retryBackoff time.Duration) error
```

**Call site** (`main.go:183`) updated to pass the three new values.

**Validation:** Negative `maxRetries` or `retryBackoff` are clamped to 0
inline (no retries / retry immediately). A `logger.Printf` warning is emitted
when clamping occurs, so user typos are not silently corrected.

### 4. Testing

**Unit tests (table-driven, no network):**

1. **`isRetryableError`** — covers `io.ErrUnexpectedEOF`, `io.EOF`,
   `net.OpError` (connection reset), `*url.Error` (timeout, connection reset),
   `context.Canceled`, `context.DeadlineExceeded`, HTTP 408/429/500/502/503,
   HTTP 400/401/403/404/405/416, `nil`, and an unclassified generic error
   (assert non-retryable).

2. **`backoffDuration`** — verifies exponential growth, jitter range, and 30s
   cap. E.g., base=1s: attempt 1 ∈ [1s, 2s), attempt 2 ∈ [2s, 3s), attempt 3
   ∈ [4s, 5s), ..., capped at 30s.

**Integration test (fake bucket, no real network):**

3. A fake `Bucket` implementation that fails N times then succeeds. Verify:
   - The range is retried N times and then succeeds.
   - On permanent error (e.g., 404), no retry happens.
   - On context cancellation, the loop exits promptly.
   - The output file contains the correct bytes after a successful retry
     (verifying the `OffsetWriter` overwrite works).

   This exercises the real `Extract` path via a test double, since `Bucket` is
   an interface.

4. **Partial-then-successful produces correct bytes** — the fake bucket returns
   a short read (fewer than `Length` bytes) on the first attempt, then the full
   range on the second. Assert the output file at `DstOffset` matches the
   expected bytes exactly (no stale tail bytes from the short read).

5. **HTTP client configuration** — verify that `Extract` configures the
   `HTTPBucket` client with the specified `httpTimeout`. This can be checked
   via the fake bucket: if the bucket is an `*HTTPBucket`, assert
   `hb.client.Timeout == httpTimeout` after `Extract` opens it. (Alternatively,
   assert behaviorally that a stalled response is aborted after the timeout.)

6. **Negative-value clamping** — call `Extract` with `maxRetries=-1` and
   `retryBackoff=-1s`; verify it does not panic and logs a warning.

## Files changed

| File | Change |
|------|--------|
| `pmtiles/bucket.go` | Add `SetHTTPClient`; `OpenBucket` returns `*HTTPBucket` for http case |
| `pmtiles/extract.go` | Add retry loop + `downloadRangeOnce` + `isRetryableError` + `backoffDuration`; configure HTTP client; new signature params |
| `main.go` | Three new CLI flags on `Extract` struct; pass to `Extract()` |
| `pmtiles/extract_test.go` (new) | Unit + integration tests |

## Risks

- **Progress bar >100% on retry:** cosmetic only; documented above.
- **`OpenBucket` return type change (value → pointer for HTTPBucket):** the
  `Bucket` interface is satisfied by both, and all in-repo callers use the
  interface, so this is source-compatible internally. External callers that
  type-assert to `HTTPBucket` (value) rather than `*HTTPBucket` would need to
  update their assertion. This repo has no CHANGELOG; the change is noted here
  and in the commit message. Verified by build.
- **Large ranges + 60s timeout:** very large merged ranges on slow connections
  could hit the timeout. Mitigated by being CLI-configurable; users can bump
  `--http-timeout` or reduce `--overfetch` to shrink ranges.
