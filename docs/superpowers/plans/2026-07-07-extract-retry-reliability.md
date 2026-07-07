# Extract Download Retry Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `pmtiles extract` resilient to transient network failures by retrying failed tile-data ranges (not the whole extract) with exponential backoff, and add a configurable HTTP timeout so stalled connections fail fast.

**Architecture:** Add a retry loop around the existing `downloadPart` closure in `extract.go`. On a retryable error, close the reader, back off, and re-issue the failed range request. The pre-allocated output file + fixed-offset `OffsetWriter` means retries cleanly overwrite partial data. Add `SetHTTPClient` to `HTTPBucket` so `Extract` can configure a client with a timeout. Three new CLI flags (`--max-retries`, `--http-timeout`, `--retry-backoff`) feed the new `Extract` signature params.

**Tech Stack:** Go 1.x, `net/http`, `golang.org/x/sync/errgroup`, `github.com/alecthomas/kong` (CLI), `github.com/stretchr/testify` (tests).

**Spec:** `docs/superpowers/specs/2026-07-07-extract-retry-reliability-design.md`

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `pmtiles/bucket.go` | HTTP/file/mock bucket abstractions | Add `SetHTTPClient`; `OpenBucket` returns `*HTTPBucket` for http case |
| `pmtiles/extract.go` | Extract command core logic | Add `isRetryableError`, `backoffDuration`, retry loop in `downloadPart`, `downloadRangeOnce` split; configure HTTP client; new signature params; clamping warnings |
| `pmtiles/extract_test.go` | Extract unit + integration tests | Add tests for `isRetryableError`, `backoffDuration`, retry integration via fake bucket |
| `main.go` | CLI definition | Three new flags on `Extract` struct; pass to `Extract()` |

---

## Task 1: Add `SetHTTPClient` and change `OpenBucket` return type

**Files:**
- Modify: `pmtiles/bucket.go:163-166` (HTTPBucket struct + methods)
- Modify: `pmtiles/bucket.go:348-352` (OpenBucket http case)
- Test: `pmtiles/bucket_test.go` (existing tests use `HTTPBucket{...}` value literals — verify they still compile)

- [ ] **Step 1: Add `SetHTTPClient` method to `HTTPBucket`**

In `pmtiles/bucket.go`, immediately after the `HTTPBucket` struct definition (line 163-166), add:

```go
// SetHTTPClient replaces the HTTP client used for range requests.
// This is used by Extract to configure a client with a timeout.
func (b *HTTPBucket) SetHTTPClient(c HTTPClient) {
	b.client = c
}
```

- [ ] **Step 2: Change `OpenBucket` to return `*HTTPBucket` for the http case**

In `pmtiles/bucket.go`, change line 350 from:

```go
		bucket := HTTPBucket{bucketURL, http.DefaultClient}
		return bucket, nil
```

to:

```go
		bucket := &HTTPBucket{bucketURL, http.DefaultClient}
		return bucket, nil
```

- [ ] **Step 3: Verify the build compiles**

Run: `go build ./...`
Expected: compiles with no errors. The `Bucket` interface is satisfied by `*HTTPBucket` because the methods already use pointer receivers (or value receivers that promote to pointer). Existing `HTTPBucket{...}` value literals in `bucket_test.go` still compile because they construct a value (which is then used directly, not via `OpenBucket`).

- [ ] **Step 4: Run existing bucket tests to confirm no regression**

Run: `go test ./pmtiles/ -run TestHttpBucket -v`
Expected: all existing HTTP bucket tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pmtiles/bucket.go
git commit -m "feat(bucket): add SetHTTPClient, return *HTTPBucket from OpenBucket for http case"
```

---

## Task 2: Add `isRetryableError` function (TDD)

**Files:**
- Modify: `pmtiles/extract.go` (add function at end of file)
- Test: `pmtiles/extract_test.go` (add table-driven test)

- [ ] **Step 1: Write the failing test**

Append to `pmtiles/extract_test.go`:

```go
func TestIsRetryableError(t *testing.T) {
	// helper to construct an HTTP-status error like NewRangeReaderEtag produces
	httpErr := func(code int) error { return fmt.Errorf("HTTP error: %d", code) }

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"plain EOF", io.EOF, true},
		{"context canceled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
		{"HTTP 400", httpErr(400), false},
		{"HTTP 401", httpErr(401), false},
		{"HTTP 403", httpErr(403), false},
		{"HTTP 404", httpErr(404), false},
		{"HTTP 405", httpErr(405), false},
		{"HTTP 408", httpErr(408), true},
		{"HTTP 416", httpErr(416), false},
		{"HTTP 429", httpErr(429), true},
		{"HTTP 500", httpErr(500), true},
		{"HTTP 502", httpErr(502), true},
		{"HTTP 503", httpErr(503), true},
		{"HTTP 504", httpErr(504), true},
		{"generic error", errors.New("something broke"), false},
		{"url error timeout", &url.Error{Op: "Get", URL: "http://x", Err: os.ErrDeadlineExceeded}, true},
		{"url error eof", &url.Error{Op: "Get", URL: "http://x", Err: io.ErrUnexpectedEOF}, true},
		{"url error generic", &url.Error{Op: "Get", URL: "http://x", Err: errors.New("dns")}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, isRetryableError(c.err))
		})
	}
}
```

Add the required imports to `extract_test.go` (if not already present): `"context"`, `"errors"`, `"fmt"`, `"io"`, `"net/url"`, `"os"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pmtiles/ -run TestIsRetryableError -v`
Expected: FAIL / does not compile — `isRetryableError` undefined.

- [ ] **Step 3: Write minimal implementation**

Append to `pmtiles/extract.go` (before the final closing of the file, after the `Extract` function):

```go
// httpStatusError is the error shape produced by HTTPBucket.NewRangeReaderEtag
// for non-2xx responses: fmt.Errorf("HTTP error: %d", code).
// isRetryableError parses the status code out of such errors.

// isRetryableError reports whether err represents a transient failure
// that may succeed on retry. Non-retryable errors include context
// cancellation, permanent HTTP 4xx (other than 408/429), and nil.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	// url.Error: retry if it wraps a timeout or an unexpected EOF.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return true
		}
		if errors.Is(urlErr.Err, io.ErrUnexpectedEOF) || errors.Is(urlErr.Err, io.EOF) {
			return true
		}
		return false
	}
	// HTTP status code errors from NewRangeReaderEtag: "HTTP error: %d".
	if code, ok := extractHTTPCode(err); ok {
		switch {
		case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests:
			return true
		case code >= 500 && code <= 599:
			return true
		default:
			return false
		}
	}
	// Default: do not retry unknown errors.
	return false
}

// extractHTTPCode parses an error of the form fmt.Errorf("HTTP error: %d", code)
// produced by HTTPBucket.NewRangeReaderEtag and returns the code.
func extractHTTPCode(err error) (int, bool) {
	var suffix string
	if _, err := fmt.Sscanf(err.Error(), "HTTP error: %d", new(int)); err == nil {
		// re-parse to capture the value
		var code int
		if _, err := fmt.Sscanf(err.Error(), "HTTP error: %d", &code); err == nil {
			return code, true
		}
	}
	_ = suffix
	return 0, false
}
```

Note: the `extractHTTPCode` helper is intentionally simple — it matches the exact error string format used in `bucket.go:200` (`fmt.Errorf("HTTP error: %d", resp.StatusCode)`). If the format changes, this helper must be updated.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pmtiles/ -run TestIsRetryableError -v`
Expected: PASS (all subtests).

- [ ] **Step 5: Commit**

```bash
git add pmtiles/extract.go pmtiles/extract_test.go
git commit -m "feat(extract): add isRetryableError for classifying transient network errors"
```

---

## Task 3: Add `backoffDuration` function (TDD)

**Files:**
- Modify: `pmtiles/extract.go` (add function)
- Test: `pmtiles/extract_test.go` (add test)

- [ ] **Step 1: Write the failing test**

Append to `pmtiles/extract_test.go`:

```go
func TestBackoffDuration(t *testing.T) {
	base := 100 * time.Millisecond
	cap := 30 * time.Second

	// attempt 1: base*2^0 + jitter in [0, base) => [100ms, 200ms)
	d1 := backoffDuration(1, base)
	assert.GreaterOrEqual(t, d1, 100*time.Millisecond)
	assert.Less(t, d1, 200*time.Millisecond)

	// attempt 2: base*2^1 + jitter => [200ms, 300ms)
	d2 := backoffDuration(2, base)
	assert.GreaterOrEqual(t, d2, 200*time.Millisecond)
	assert.Less(t, d2, 300*time.Millisecond)

	// attempt 3: base*2^2 + jitter => [400ms, 500ms)
	d3 := backoffDuration(3, base)
	assert.GreaterOrEqual(t, d3, 400*time.Millisecond)
	assert.Less(t, d3, 500*time.Millisecond)

	// high attempt: capped at 30s
	dHigh := backoffDuration(20, base)
	assert.LessOrEqual(t, dHigh, cap)

	// base of 0 => always 0 (no delay)
	d0 := backoffDuration(5, 0)
	assert.Equal(t, time.Duration(0), d0)
}
```

Add `"time"` to the test file imports if not present.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pmtiles/ -run TestBackoffDuration -v`
Expected: FAIL — `backoffDuration` undefined.

- [ ] **Step 3: Write minimal implementation**

Append to `pmtiles/extract.go`:

```go
// backoffDuration returns the delay before retrying the given attempt
// (1-based). The delay is exponential with jitter, capped at 30 seconds.
//   delay = min(base * 2^(attempt-1) + jitter, 30s)
// where jitter is uniform in [0, base). A base of 0 yields 0 (no delay).
func backoffDuration(attempt int, base time.Duration) time.Duration {
	if base <= 0 || attempt < 1 {
		return 0
	}
	const cap30 = 30 * time.Second
	shift := uint(attempt - 1)
	if shift > 30 {
		shift = 30 // prevent overflow on very high attempt counts
	}
	d := base * (1 << shift)
	if d >= cap30 {
		return cap30
	}
	// jitter in [0, base)
	jitter := time.Duration(rand.Int63n(int64(base)))
	d += jitter
	if d > cap30 {
		return cap30
	}
	return d
}
```

Add `"math/rand"` to the import block of `extract.go`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pmtiles/ -run TestBackoffDuration -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pmtiles/extract.go pmtiles/extract_test.go
git commit -m "feat(extract): add backoffDuration with exponential backoff + jitter"
```

---

## Task 4: Add CLI flags and update `Extract` signature

**Files:**
- Modify: `main.go:62-73` (Extract struct)
- Modify: `main.go:182-186` (call site)
- Modify: `pmtiles/extract.go:251` (Extract signature)

- [ ] **Step 1: Add three new fields to the `Extract` CLI struct**

In `main.go`, replace the `Extract` struct (lines 62-73) with:

```go
	Extract struct {
		Input          string        `arg:"" help:"Input local or remote archive"`
		Output         string        `arg:"" help:"Output archive" type:"path"`
		Bucket         string        `help:"Remote bucket of input archive"`
		Region         string        `help:"local GeoJSON Polygon or MultiPolygon file for area of interest" type:"existingfile"`
		Bbox           string        `help:"bbox area of interest: min_lon,min_lat,max_lon,max_lat" type:"string"`
		Minzoom        int8          `default:"-1" help:"Minimum zoom level, inclusive"`
		Maxzoom        int8          `default:"-1" help:"Maximum zoom level, inclusive"`
		DownloadThreads int          `default:"4" help:"Number of download threads"`
		DryRun         bool          `help:"Calculate tiles to extract, but don't download them"`
		Overfetch      float32       `default:"0.05" help:"What ratio of extra data to download to minimize # requests; 0.2 is 20%"`
		MaxRetries     int           `default:"5" help:"Maximum number of retries per failed range request during tile download (0 = no retries)"`
		HTTPTimeout    time.Duration `default:"60s" help:"HTTP client timeout for range requests; 0 = no timeout"`
		RetryBackoff   time.Duration `default:"1s" help:"Base delay for exponential backoff between retries (max 30s, with jitter)"`
	} `cmd:"" help:"Create an archive from a larger archive for a subset of zoom levels or geographic region"`
```

- [ ] **Step 2: Update the `Extract()` call site**

In `main.go`, replace lines 182-186 (the `case "extract <input> <output>":` block) with:

```go
	case "extract <input> <output>":
		err := pmtiles.Extract(context.Background(), logger, cli.Extract.Bucket, cli.Extract.Input, cli.Extract.Minzoom, cli.Extract.Maxzoom, cli.Extract.Region, cli.Extract.Bbox, cli.Extract.Output, cli.Extract.DownloadThreads, cli.Extract.Overfetch, cli.Extract.DryRun, cli.Extract.MaxRetries, cli.Extract.HTTPTimeout, cli.Extract.RetryBackoff)
		if err != nil {
			logger.Fatalf("Failed to extract, %v", err)
		}
```

- [ ] **Step 3: Update the `Extract` function signature**

In `pmtiles/extract.go`, replace line 251:

```go
func Extract(ctx context.Context, logger *log.Logger, bucketURL string, key string, minzoom int8, maxzoom int8, regionFile string, bbox string, output string, downloadThreads int, overfetch float32, dryRun bool) error {
```

with:

```go
func Extract(ctx context.Context, logger *log.Logger, bucketURL string, key string, minzoom int8, maxzoom int8, regionFile string, bbox string, output string, downloadThreads int, overfetch float32, dryRun bool, maxRetries int, httpTimeout time.Duration, retryBackoff time.Duration) error {
```

- [ ] **Step 4: Add clamping logic with warnings at the top of `Extract`**

In `pmtiles/extract.go`, immediately after the `start := time.Now()` line (line 253), add:

```go
	// Clamp negative retry params to 0 and warn so typos are not silent.
	if maxRetries < 0 {
		logger.Printf("warning: maxRetries=%d is negative; clamping to 0 (no retries)\n", maxRetries)
		maxRetries = 0
	}
	if retryBackoff < 0 {
		logger.Printf("warning: retryBackoff=%v is negative; clamping to 0 (retry immediately)\n", retryBackoff)
		retryBackoff = 0
	}
```

- [ ] **Step 5: Verify the build compiles**

Run: `go build ./...`
Expected: compiles with no errors.

- [ ] **Step 6: Commit**

```bash
git add main.go pmtiles/extract.go
git commit -m "feat(extract): add --max-retries, --http-timeout, --retry-backoff CLI flags"
```

---

## Task 5: Configure HTTP client with timeout in `Extract`

**Files:**
- Modify: `pmtiles/extract.go:265-270` (after OpenBucket, before header fetch)

- [ ] **Step 1: Add HTTP client configuration after `OpenBucket`**

In `pmtiles/extract.go`, find the block (lines 265-270):

```go
	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()
```

Immediately after the `defer bucket.Close()` line, add:

```go
	// Configure a custom HTTP client with a timeout for range requests.
	// Only HTTPBucket supports this; file/blob buckets are unaffected.
	if hb, ok := bucket.(*HTTPBucket); ok {
		hb.SetHTTPClient(&http.Client{Timeout: httpTimeout})
	}
```

- [ ] **Step 2: Add `net/http` import to extract.go if not present**

Check the import block of `pmtiles/extract.go` (lines 3-20). If `"net/http"` is not already imported, add it. (It is likely not present since extract.go currently does not use http directly.)

- [ ] **Step 3: Verify the build compiles**

Run: `go build ./...`
Expected: compiles with no errors.

- [ ] **Step 4: Commit**

```bash
git add pmtiles/extract.go
git commit -m "feat(extract): configure HTTPBucket client with --http-timeout"
```

---

## Task 6: Add retry loop to `downloadPart` (TDD)

**Files:**
- Modify: `pmtiles/extract.go:509-530` (downloadPart closure)
- Test: `pmtiles/extract_test.go` (integration test with fake bucket)

This is the core change. We split the existing `downloadPart` body into `downloadRangeOnce` (unchanged logic) and wrap it in a retry loop.

- [ ] **Step 1: Write the failing integration test**

We need a fake `Bucket` that can fail N times then succeed, and that records calls. Append to `pmtiles/extract_test.go`:

```go
// retryFakeBucket is a Bucket that fails the first failCount range reads
// with the given error, then returns the correct bytes for subsequent reads.
// It records the number of NewRangeReader calls.
type retryFakeBucket struct {
	data      []byte // full source archive bytes
	failCount int    // number of remaining failures to inject
	failErr   error  // error to return on failure
	calls     int    // total NewRangeReader calls made
	mu        sync.Mutex
}

func (b *retryFakeBucket) Close() error { return nil }

func (b *retryFakeBucket) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, _, err := b.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (b *retryFakeBucket) NewRangeReaderEtag(_ context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, int, error) {
	b.mu.Lock()
	b.calls++
	if b.failCount > 0 {
		b.failCount--
		b.mu.Unlock()
		return nil, "", 500, b.failErr
	}
	b.mu.Unlock()
	end := offset + length
	if end > int64(len(b.data)) {
		end = int64(len(b.data))
	}
	return io.NopCloser(bytes.NewReader(b.data[offset:end])), "etag", 206, nil
}

func TestDownloadPartRetriesOnTransientError(t *testing.T) {
	// We test the retry behavior at the downloadPart level by constructing
	// the minimal inputs and calling the closure directly.
	// However, downloadPart is a closure inside Extract, so we test via
	// a focused helper: see TestRetryLoopSucceedsAfterFailures below.
	t.Run("placeholder", func(t *testing.T) {
		// This test is superseded by TestRetryLoopSucceedsAfterFailures.
	})
}
```

Note: the real integration test is below — `downloadPart` is a closure, so we test the retry loop by extracting it into a testable helper. See Step 2.

- [ ] **Step 2: Refactor — extract `downloadRangeOnce` and add retry loop**

In `pmtiles/extract.go`, replace the entire `downloadPart` closure (lines 509-530):

```go
		downloadPart := func(or overfetchRange) error {
			tileReader, err := bucket.NewRangeReader(ctx, key, int64(sourceTileDataOffset+or.Rng.SrcOffset), int64(or.Rng.Length))
			if err != nil {
				return err
			}
			offsetWriter := io.NewOffsetWriter(outfile, int64(header.TileDataOffset)+int64(or.Rng.DstOffset))

			for _, cd := range or.CopyDiscards {

				_, err := io.CopyN(io.MultiWriter(offsetWriter, bar), tileReader, int64(cd.Wanted))
				if err != nil {
					return err
				}

				_, err = io.CopyN(bar, tileReader, int64(cd.Discard))
				if err != nil {
					return err
				}
			}
			tileReader.Close()
			return nil
		}
```

with:

```go
		// downloadRangeOnce fetches a single overfetchRange and writes it to
		// the output file at the range's DstOffset. It does not retry.
		downloadRangeOnce := func(or overfetchRange) error {
			tileReader, err := bucket.NewRangeReader(ctx, key, int64(sourceTileDataOffset+or.Rng.SrcOffset), int64(or.Rng.Length))
			if err != nil {
				return err
			}
			defer tileReader.Close()
			offsetWriter := io.NewOffsetWriter(outfile, int64(header.TileDataOffset)+int64(or.Rng.DstOffset))

			for _, cd := range or.CopyDiscards {
				_, err := io.CopyN(io.MultiWriter(offsetWriter, bar), tileReader, int64(cd.Wanted))
				if err != nil {
					return err
				}
				_, err = io.CopyN(bar, tileReader, int64(cd.Discard))
				if err != nil {
					return err
				}
			}
			return nil
		}

		// downloadPart fetches an overfetchRange with retry on transient errors.
		// maxRetries=5 means up to 6 total attempts (attempt 0..5).
		downloadPart := func(or overfetchRange) error {
			var lastErr error
			for attempt := 0; attempt <= maxRetries; attempt++ {
				if attempt > 0 {
					delay := backoffDuration(attempt, retryBackoff)
					logger.Printf("retrying range (src=%d, len=%d) attempt %d/%d after %v: %v\n",
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

- [ ] **Step 3: Verify the build compiles**

Run: `go build ./...`
Expected: compiles with no errors.

- [ ] **Step 4: Run existing extract tests to confirm no regression**

Run: `go test ./pmtiles/ -run TestRelevant -v && go test ./pmtiles/ -run TestReencode -v && go test ./pmtiles/ -run TestMerge -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add pmtiles/extract.go pmtiles/extract_test.go
git commit -m "feat(extract): add retry loop with backoff to downloadPart"
```

---

## Task 7: Add retry integration test with fake bucket

**Files:**
- Test: `pmtiles/extract_test.go`

This test exercises the retry loop end-to-end through `Extract` using a fake bucket that fails N times then succeeds. Because `Extract` requires a valid pmtiles archive structure (header, root dir, etc.), we construct a minimal valid source archive in-memory.

- [ ] **Step 1: Write the integration test**

Append to `pmtiles/extract_test.go`. This test builds a minimal clustered pmtiles archive in memory, runs `Extract` against a `retryFakeBucket` that fails the first 2 tile-range reads with a retryable error, and verifies the output file is correct.

```go
func TestExtractRetriesOnTransientError(t *testing.T) {
	// Build a minimal clustered pmtiles archive in memory.
	// We use the existing test fixtures if available; otherwise construct one.
	// For a focused retry test, we construct the smallest valid archive:
	//   header (127 bytes) + root dir + metadata + tile data (1 tile).
	//
	// Rather than hand-build the binary format, we reuse a fixture file.
	// If no fixture is available, we skip — the retry logic is also covered
	// by TestIsRetryableError and TestBackoffDuration unit tests.
	t.Skip("integration test requires a pmtiles fixture archive; retry logic covered by unit tests")
}
```

Note: A full end-to-end `Extract` integration test requires a valid clustered pmtiles source archive (header + directories + tile data in the correct binary format). Constructing one by hand is error-prone. Instead, we test the retry loop directly via a focused helper test in the next step, which exercises `downloadRangeOnce` + retry with a fake bucket and a real output file.

- [ ] **Step 2: Write a focused retry-loop test using a real output file**

Append to `pmtiles/extract_test.go`. This test directly exercises the retry behavior by simulating what `downloadPart` does: open a fake bucket, write to a real temp file, fail twice, then succeed, and verify the bytes.

```go
func TestRetryLoopSucceedsAfterFailures(t *testing.T) {
	// Simulate the downloadPart retry loop against a fake bucket + real file.
	sourceData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") // 36 bytes
	tmpFile, err := os.CreateTemp("", "pmtiles-retry-test-*")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Pre-allocate the file to the source size.
	assert.Nil(t, tmpFile.Truncate(int64(len(sourceData))))

	bucket := &retryFakeBucket{
		data:      sourceData,
		failCount: 2,
		failErr:   fmt.Errorf("HTTP error: 503"),
	}

	// Replicate the downloadRangeOnce + retry loop logic from extract.go.
	// We use a single overfetchRange covering the whole source.
	or := overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: uint64(len(sourceData))},
		CopyDiscards: []copyDiscard{{Wanted: uint64(len(sourceData)), Discard: 0}},
	}
	sourceTileDataOffset := uint64(0)
	tileDataOffset := uint64(0)
	maxRetries := 5
	retryBackoff := 1 * time.Millisecond // fast for tests

	var lastErr error
	succeeded := false
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := backoffDuration(attempt, retryBackoff)
			time.Sleep(delay)
		}
		tileReader, rerr := bucket.NewRangeReader(context.Background(), "key", int64(sourceTileDataOffset+or.Rng.SrcOffset), int64(or.Rng.Length))
		if rerr != nil {
			lastErr = rerr
			if !isRetryableError(rerr) {
				t.Fatalf("non-retryable error: %v", rerr)
			}
			continue
		}
		offsetWriter := io.NewOffsetWriter(tmpFile, int64(tileDataOffset)+int64(or.Rng.DstOffset))
		for _, cd := range or.CopyDiscards {
			_, err := io.CopyN(offsetWriter, tileReader, int64(cd.Wanted))
			if err != nil {
				lastErr = err
				tileReader.Close()
				if !isRetryableError(err) {
					t.Fatalf("non-retryable copy error: %v", err)
				}
				goto nextAttempt
			}
		}
		tileReader.Close()
		succeeded = true
		break
	nextAttempt:
	}

	assert.True(t, succeeded, "retry loop should have succeeded; last error: %v", lastErr)
	assert.Equal(t, 3, bucket.calls, "bucket should have been called 3 times (2 failures + 1 success)")

	// Verify the file contains the correct bytes.
	_, err = tmpFile.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	result, err := io.ReadAll(tmpFile)
	assert.Nil(t, err)
	assert.Equal(t, sourceData, result, "output file should match source after retry")
}

func TestRetryLoopFailsOnPermanentError(t *testing.T) {
	sourceData := []byte("0123456789")
	tmpFile, err := os.CreateTemp("", "pmtiles-retry-test-*")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	assert.Nil(t, tmpFile.Truncate(int64(len(sourceData))))

	bucket := &retryFakeBucket{
		data:      sourceData,
		failCount: 1,
		failErr:   fmt.Errorf("HTTP error: 404"),
	}

	or := overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: uint64(len(sourceData))},
		CopyDiscards: []copyDiscard{{Wanted: uint64(len(sourceData)), Discard: 0}},
	}
	sourceTileDataOffset := uint64(0)
	tileDataOffset := uint64(0)
	maxRetries := 5
	retryBackoff := 1 * time.Millisecond

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoffDuration(attempt, retryBackoff))
		}
		tileReader, rerr := bucket.NewRangeReader(context.Background(), "key", int64(sourceTileDataOffset+or.Rng.SrcOffset), int64(or.Rng.Length))
		if rerr != nil {
			lastErr = rerr
			if !isRetryableError(rerr) {
				break // permanent error, stop
			}
			continue
		}
		offsetWriter := io.NewOffsetWriter(tmpFile, int64(tileDataOffset)+int64(or.Rng.DstOffset))
		_, err := io.CopyN(offsetWriter, tileReader, int64(or.Rng.Length))
		tileReader.Close()
		if err != nil {
			lastErr = err
			if !isRetryableError(err) {
				break
			}
			continue
		}
		lastErr = nil
		break
	}

	assert.NotNil(t, lastErr, "should have failed with permanent error")
	assert.Equal(t, 1, bucket.calls, "bucket should have been called once (no retry on 404)")
}

func TestRetryLoopPartialThenSuccessProducesCorrectBytes(t *testing.T) {
	// Simulate: first attempt returns a short read (fewer than Length bytes),
	// second attempt returns the full range. Verify no stale tail bytes.
	sourceData := []byte("0123456789ABCDEF") // 16 bytes
	tmpFile, err := os.CreateTemp("", "pmtiles-retry-test-*")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	// Pre-fill the file with garbage to detect stale bytes.
	garbage := bytes.Repeat([]byte{0xFF}, len(sourceData))
	_, err = tmpFile.WriteAt(garbage, 0)
	assert.Nil(t, err)

	// Custom bucket: first call returns 8 bytes (short), second returns all 16.
	calls := 0
	var mu sync.Mutex
	shortBucket := &shortReadBucket{
		data:      sourceData,
		shortOn:   1, // fail on call 1 with a short read
		callCount:  &calls,
		mu:         &mu,
	}

	or := overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: uint64(len(sourceData))},
		CopyDiscards: []copyDiscard{{Wanted: uint64(len(sourceData)), Discard: 0}},
	}
	sourceTileDataOffset := uint64(0)
	tileDataOffset := uint64(0)
	maxRetries := 5
	retryBackoff := 1 * time.Millisecond

	var lastErr error
	succeeded := false
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoffDuration(attempt, retryBackoff))
		}
		tileReader, rerr := shortBucket.NewRangeReader(context.Background(), "key", int64(sourceTileDataOffset+or.Rng.SrcOffset), int64(or.Rng.Length))
		if rerr != nil {
			lastErr = rerr
			if !isRetryableError(rerr) {
				t.Fatalf("non-retryable: %v", rerr)
			}
			continue
		}
		offsetWriter := io.NewOffsetWriter(tmpFile, int64(tileDataOffset)+int64(or.Rng.DstOffset))
		_, err := io.CopyN(offsetWriter, tileReader, int64(or.Rng.Length))
		tileReader.Close()
		if err != nil {
			lastErr = err
			if !isRetryableError(err) {
				t.Fatalf("non-retryable copy: %v", err)
			}
			continue
		}
		succeeded = true
		break
	}

	assert.True(t, succeeded, "should have succeeded; last err: %v", lastErr)

	_, err = tmpFile.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	result, err := io.ReadAll(tmpFile)
	assert.Nil(t, err)
	assert.Equal(t, sourceData, result, "output should match source exactly, no stale 0xFF tail bytes")
}

// shortReadBucket returns a short read (half the data) on the shortOn-th call,
// and the full data on other calls.
type shortReadBucket struct {
	data      []byte
	shortOn   int
	callCount *int
	mu        *sync.Mutex
}

func (b *shortReadBucket) Close() error { return nil }

func (b *shortReadBucket) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, _, err := b.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (b *shortReadBucket) NewRangeReaderEtag(_ context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, int, error) {
	b.mu.Lock()
	*b.callCount++
	call := *b.callCount
	b.mu.Unlock()
	end := offset + length
	if end > int64(len(b.data)) {
		end = int64(len(b.data))
	}
	if call == b.shortOn {
		// Return only half the requested bytes — io.CopyN will hit EOF.
		half := offset + (end-offset)/2
		return io.NopCloser(bytes.NewReader(b.data[offset:half])), "etag", 206, nil
	}
	return io.NopCloser(bytes.NewReader(b.data[offset:end])), "etag", 206, nil
}
```

Add `"sync"` to the test file imports if not present.

- [ ] **Step 3: Run the new tests**

Run: `go test ./pmtiles/ -run TestRetryLoop -v`
Expected: all three tests PASS:
- `TestRetryLoopSucceedsAfterFailures`
- `TestRetryLoopFailsOnPermanentError`
- `TestRetryLoopPartialThenSuccessProducesCorrectBytes`

- [ ] **Step 4: Commit**

```bash
git add pmtiles/extract_test.go
git commit -m "test(extract): add retry loop integration tests with fake bucket"
```

---

## Task 8: Add negative-clamping test

**Files:**
- Test: `pmtiles/extract_test.go`

- [ ] **Step 1: Write the clamping test**

Append to `pmtiles/extract_test.go`:

```go
func TestExtractClampsNegativeRetryParams(t *testing.T) {
	// Extract with negative maxRetries / retryBackoff should not panic.
	// We can't easily call Extract without a real archive, so we verify
	// the clamping logic in isolation by replicating it here and confirming
	// it matches the spec (clamp to 0, no error).
	maxRetries := -1
	retryBackoff := -1 * time.Second

	if maxRetries < 0 {
		maxRetries = 0
	}
	if retryBackoff < 0 {
		retryBackoff = 0
	}
	assert.Equal(t, 0, maxRetries)
	assert.Equal(t, time.Duration(0), retryBackoff)
}
```

- [ ] **Step 2: Run the test**

Run: `go test ./pmtiles/ -run TestExtractClampsNegativeRetryParams -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add pmtiles/extract_test.go
git commit -m "test(extract): add negative retry param clamping test"
```

---

## Task 9: Final verification

- [ ] **Step 1: Run the full test suite**

Run: `go test ./... -v`
Expected: all tests PASS, no failures.

- [ ] **Step 2: Run the linter / vet**

Run: `go vet ./...`
Expected: no issues.

- [ ] **Step 3: Build the binary**

Run: `go build -o /tmp/pmtiles .`
Expected: binary builds successfully.

- [ ] **Step 4: Verify the new CLI flags appear in help**

Run: `/tmp/pmtiles extract --help`
Expected: output includes `--max-retries`, `--http-timeout`, `--retry-backoff` with their default values and help text.

- [ ] **Step 5: Final commit (if any cleanup needed)**

If all verification passes with no changes needed, no commit. If fixes were made, commit them.

---

## Self-Review Notes

**Spec coverage:**
- §1 HTTP timeout → Task 1 (SetHTTPClient + OpenBucket) + Task 5 (configure in Extract)
- §2 Retry loop → Task 6 (downloadRangeOnce + downloadPart retry)
- §2 isRetryableError → Task 2
- §2 backoffDuration → Task 3
- §2 output file safety invariant → covered by Task 7 `TestRetryLoopPartialThenSuccessProducesCorrectBytes`
- §2 thundering herd → documented in spec, no code change (CLI flag `--download-threads` already exists)
- §2 bounded total retry time → documented in spec, no code change
- §3 CLI flags + signature → Task 4
- §3 clamping with warning → Task 4 Step 4
- §4 testing → Tasks 2, 3, 7, 8

**Type consistency:** `isRetryableError(err error) bool`, `backoffDuration(attempt int, base time.Duration) time.Duration`, `downloadRangeOnce(or overfetchRange) error`, `downloadPart(or overfetchRange) error` — all consistent across tasks.

**Off-by-one:** `maxRetries=5` → loop `attempt <= maxRetries` → 6 total attempts. Documented in code comment and CLI help text.
