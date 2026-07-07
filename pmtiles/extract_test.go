package pmtiles

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
)

func TestRelevantEntries(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 1})

	bitmap := roaring64.New()
	bitmap.Add(0)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 1)
	assert.Equal(t, len(leaves), 0)
}

func TestRelevantEntriesRunLength(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 5})

	bitmap := roaring64.New()
	bitmap.Add(1)
	bitmap.Add(2)
	bitmap.Add(4)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 2)
	assert.Equal(t, tiles[0].RunLength, uint32(2))
	assert.Equal(t, tiles[1].RunLength, uint32(1))
	assert.Equal(t, len(leaves), 0)
}

func TestRelevantEntriesLeaf(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})

	bitmap := roaring64.New()
	bitmap.Add(1)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 0)
	assert.Equal(t, len(leaves), 1)
}

func TestRelevantEntriesNotLeaf(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})
	entries = append(entries, EntryV3{2, 0, 0, 1})
	entries = append(entries, EntryV3{4, 0, 0, 0})

	bitmap := roaring64.New()
	bitmap.Add(3)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 0)
	assert.Equal(t, len(leaves), 0)
}

func TestRelevantEntriesMaxZoom(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})

	bitmap := roaring64.New()
	bitmap.Add(6)
	_, leaves := RelevantEntries(bitmap, 1, entries)
	assert.Equal(t, len(leaves), 0)

	_, leaves = RelevantEntries(bitmap, 2, entries)
	assert.Equal(t, len(leaves), 1)
}

func TestReencodeEntries(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 400, 10, 1})
	entries = append(entries, EntryV3{1, 500, 20, 2})

	reencoded, result, datalen, addressed, contents := reencodeEntries(entries)

	assert.Equal(t, 2, len(result))
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(10))
	assert.Equal(t, result[1].SrcOffset, uint64(500))
	assert.Equal(t, result[1].Length, uint64(20))

	assert.Equal(t, 2, len(reencoded))
	assert.Equal(t, reencoded[0].Offset, uint64(0))
	assert.Equal(t, reencoded[1].Offset, uint64(10))

	assert.Equal(t, uint64(30), datalen)
	assert.Equal(t, uint64(3), addressed)
	assert.Equal(t, uint64(2), contents)
}

func TestReencodeEntriesDuplicate(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 400, 10, 1})
	entries = append(entries, EntryV3{1, 500, 20, 1})
	entries = append(entries, EntryV3{2, 400, 10, 1})

	reencoded, result, datalen, addressed, contents := reencodeEntries(entries)

	assert.Equal(t, 2, len(result))
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(10))
	assert.Equal(t, result[1].SrcOffset, uint64(500))
	assert.Equal(t, result[1].Length, uint64(20))

	assert.Equal(t, len(reencoded), 3)
	assert.Equal(t, reencoded[0].Offset, uint64(0))
	assert.Equal(t, reencoded[1].Offset, uint64(10))
	assert.Equal(t, reencoded[2].Offset, uint64(0))

	assert.Equal(t, uint64(30), datalen)
	assert.Equal(t, uint64(3), addressed)
	assert.Equal(t, uint64(2), contents)
}

func TestReencodeContiguous(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 400, 10, 0})
	entries = append(entries, EntryV3{1, 410, 20, 0})

	_, result, _, _, _ := reencodeEntries(entries)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(30))
}

func TestMergeRanges(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{0, 0, 50})
	ranges = append(ranges, srcDstRange{60, 60, 60})

	result, totalTransferBytes := mergeRanges(ranges, 0.1, 0)

	assert.Equal(t, 1, result.Len())
	assert.Equal(t, uint64(120), totalTransferBytes)
	front := result.Front().Value.(overfetchRange)
	assert.Equal(t, srcDstRange{0, 0, 120}, front.Rng)
	assert.Equal(t, 2, len(front.CopyDiscards))
	assert.Equal(t, copyDiscard{50, 10}, front.CopyDiscards[0])
	assert.Equal(t, copyDiscard{60, 0}, front.CopyDiscards[1])
}

func TestMergeRangesMultiple(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{0, 0, 50})
	ranges = append(ranges, srcDstRange{60, 60, 10})
	ranges = append(ranges, srcDstRange{80, 80, 10})

	result, totalTransferBytes := mergeRanges(ranges, 0.3, 0)
	front := result.Front().Value.(overfetchRange)
	assert.Equal(t, uint64(90), totalTransferBytes)
	assert.Equal(t, 1, result.Len())
	assert.Equal(t, srcDstRange{0, 0, 90}, front.Rng)
	assert.Equal(t, 3, len(front.CopyDiscards))
}

func TestMergeRangesNonSrcOrdered(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{20, 0, 50})
	ranges = append(ranges, srcDstRange{0, 60, 50})

	result, _ := mergeRanges(ranges, 0.1, 0)
	assert.Equal(t, 2, result.Len())
}

func TestSplitRanges(t *testing.T) {
	// A single contiguous range of 3GB with one copyDiscard should split
	// into 3 chunks of 1GB at maxRangeSize=1GB.
	ranges := list.New()
	ranges.PushBack(overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: 3 << 30},
		CopyDiscards: []copyDiscard{{Wanted: 3 << 30, Discard: 0}},
	})

	result := splitRanges(ranges, 1<<30)
	assert.Equal(t, 3, result.Len())

	// Verify each chunk
	e := result.Front()
	chunk1 := e.Value.(overfetchRange)
	assert.Equal(t, uint64(1<<30), chunk1.Rng.Length)
	assert.Equal(t, uint64(0), chunk1.Rng.SrcOffset)
	assert.Equal(t, uint64(0), chunk1.Rng.DstOffset)
	assert.Equal(t, uint64(1<<30), chunk1.CopyDiscards[0].Wanted)

	e = e.Next()
	chunk2 := e.Value.(overfetchRange)
	assert.Equal(t, uint64(1<<30), chunk2.Rng.Length)
	assert.Equal(t, uint64(1<<30), chunk2.Rng.SrcOffset)
	assert.Equal(t, uint64(1<<30), chunk2.Rng.DstOffset)

	e = e.Next()
	chunk3 := e.Value.(overfetchRange)
	assert.Equal(t, uint64(1<<30), chunk3.Rng.Length)
	assert.Equal(t, uint64(2<<30), chunk3.Rng.SrcOffset)
	assert.Equal(t, uint64(2<<30), chunk3.Rng.DstOffset)
}

func TestSplitRangesNoSplitNeeded(t *testing.T) {
	ranges := list.New()
	ranges.PushBack(overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: 500 << 20},
		CopyDiscards: []copyDiscard{{Wanted: 500 << 20, Discard: 0}},
	})

	// maxRangeSize = 1GB, range is 500MB, should not split
	result := splitRanges(ranges, 1<<30)
	assert.Equal(t, 1, result.Len())
}

func TestSplitRangesZeroMaxSize(t *testing.T) {
	ranges := list.New()
	ranges.PushBack(overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: 100 << 30},
		CopyDiscards: []copyDiscard{{Wanted: 100 << 30, Discard: 0}},
	})

	// maxRangeSize = 0 means no splitting
	result := splitRanges(ranges, 0)
	assert.Equal(t, 1, result.Len())
}

func TestIsRetryableError(t *testing.T) {
	// helper to construct an HTTP-status error like NewRangeReaderEtag produces
	httpErr := func(code int) error { return &httpStatusError{Code: code} }

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
		{"net op error", &net.OpError{Op: "read", Err: errors.New("connection reset")}, true},
		{"url error timeout", &url.Error{Op: "Get", URL: "http://x", Err: os.ErrDeadlineExceeded}, true},
		{"url error eof", &url.Error{Op: "Get", URL: "http://x", Err: io.ErrUnexpectedEOF}, true},
		{"url error generic", &url.Error{Op: "Get", URL: "http://x", Err: errors.New("dns")}, false},
		{"url error wrapping net op error", &url.Error{Op: "Get", URL: "http://x", Err: &net.OpError{Op: "read", Err: errors.New("connection reset")}}, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, isRetryableError(c.err))
		})
	}
}

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

	// attempt < 1 => 0 (no delay)
	assert.Equal(t, time.Duration(0), backoffDuration(0, base))
	assert.Equal(t, time.Duration(0), backoffDuration(-1, base))

	// negative base => 0 (no delay)
	assert.Equal(t, time.Duration(0), backoffDuration(1, -1*time.Second))
}

// retryFakeBucket is a Bucket that fails the first failCount range reads
// with the given error, then returns the correct bytes for subsequent reads.
// It records the number of NewRangeReader calls made.
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

func TestRetryLoopSucceedsAfterFailures(t *testing.T) {
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
		failErr:   &httpStatusError{Code: 503},
	}

	or := overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: uint64(len(sourceData))},
		CopyDiscards: []copyDiscard{{Wanted: uint64(len(sourceData)), Discard: 0}},
	}
	logger := log.New(io.Discard, "", 0)

	err = downloadWithRetry(context.Background(), logger, bucket, "key", or, 0, 0, tmpFile, io.Discard, 5, 1*time.Millisecond)
	assert.Nil(t, err)
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
		failErr:   &httpStatusError{Code: 404},
	}

	or := overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: uint64(len(sourceData))},
		CopyDiscards: []copyDiscard{{Wanted: uint64(len(sourceData)), Discard: 0}},
	}
	logger := log.New(io.Discard, "", 0)

	err = downloadWithRetry(context.Background(), logger, bucket, "key", or, 0, 0, tmpFile, io.Discard, 5, 1*time.Millisecond)
	assert.NotNil(t, err, "should have failed with permanent error")
	assert.Equal(t, 1, bucket.calls, "bucket should have been called once (no retry on 404)")
}

func TestRetryLoopPartialThenSuccessProducesCorrectBytes(t *testing.T) {
	// First attempt returns a short read (fewer than Length bytes),
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

	bucket := &shortReadBucket{
		data:    sourceData,
		shortOn: 1, // short read on call 1
	}

	or := overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: uint64(len(sourceData))},
		CopyDiscards: []copyDiscard{{Wanted: uint64(len(sourceData)), Discard: 0}},
	}
	logger := log.New(io.Discard, "", 0)

	err = downloadWithRetry(context.Background(), logger, bucket, "key", or, 0, 0, tmpFile, io.Discard, 5, 1*time.Millisecond)
	assert.Nil(t, err, "should have succeeded after retry")

	_, err = tmpFile.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	result, err := io.ReadAll(tmpFile)
	assert.Nil(t, err)
	assert.Equal(t, sourceData, result, "output should match source exactly, no stale 0xFF tail bytes")
}

func TestRetryLoopCanceledDuringBackoff(t *testing.T) {
	// Cancel the context while the retry loop is sleeping during backoff.
	// The loop should return context.Canceled promptly.
	sourceData := []byte("0123456789")
	tmpFile, err := os.CreateTemp("", "pmtiles-retry-test-*")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	assert.Nil(t, tmpFile.Truncate(int64(len(sourceData))))

	bucket := &retryFakeBucket{
		data:      sourceData,
		failCount: 99, // always fail, so we always reach the backoff sleep
		failErr:   &httpStatusError{Code: 503},
	}

	or := overfetchRange{
		Rng:          srcDstRange{SrcOffset: 0, DstOffset: 0, Length: uint64(len(sourceData))},
		CopyDiscards: []copyDiscard{{Wanted: uint64(len(sourceData)), Discard: 0}},
	}
	logger := log.New(io.Discard, "", 0)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay so the first attempt fails and the loop
	// enters the backoff sleep, where the select will pick up cancellation.
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	// Use a large backoff so the cancellation lands during the sleep.
	err = downloadWithRetry(ctx, logger, bucket, "key", or, 0, 0, tmpFile, io.Discard, 5, 10*time.Second)
	assert.ErrorIs(t, err, context.Canceled)
}

// shortReadBucket returns a short read (half the data) on the shortOn-th call,
// and the full data on other calls.
type shortReadBucket struct {
	data    []byte
	shortOn int
	calls   int
	mu      sync.Mutex
}

func (b *shortReadBucket) Close() error { return nil }

func (b *shortReadBucket) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, _, err := b.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (b *shortReadBucket) NewRangeReaderEtag(_ context.Context, _ string, offset, length int64, _ string) (io.ReadCloser, string, int, error) {
	b.mu.Lock()
	b.calls++
	call := b.calls
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
