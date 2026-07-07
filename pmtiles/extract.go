package pmtiles

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dustin/go-humanize"
	"github.com/paulmach/orb"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"
)

type srcDstRange struct {
	SrcOffset uint64
	DstOffset uint64
	Length    uint64
}

// RelevantEntries finds the intersection of a bitmap and a directory
// return sorted slice of entries, and slice of all leaf entries
// any runlengths > 1 will be "trimmed" to the relevance bitmap
func RelevantEntries(bitmap *roaring64.Bitmap, maxzoom uint8, dir []EntryV3) ([]EntryV3, []EntryV3) {
	lastTile := ZxyToID(maxzoom+1, 0, 0)
	leaves := make([]EntryV3, 0)
	tiles := make([]EntryV3, 0)
	for idx, entry := range dir {
		if entry.RunLength == 0 {
			tmp := roaring64.New()

			// if this is the last thing in the directory, it needs to be bounded
			if idx == len(dir)-1 {
				tmp.AddRange(entry.TileID, lastTile)
			} else {
				tmp.AddRange(entry.TileID, dir[idx+1].TileID)
			}

			if bitmap.Intersects(tmp) {
				leaves = append(leaves, entry)
			}
		} else if entry.RunLength == 1 {
			if bitmap.Contains(entry.TileID) {
				tiles = append(tiles, entry)
			}
		} else {
			// runlength > 1
			currentID := entry.TileID
			currentRunLength := uint32(0)
			for y := entry.TileID; y < entry.TileID+uint64(entry.RunLength); y++ {
				if bitmap.Contains(y) {
					if currentRunLength == 0 {
						currentRunLength = 1
						currentID = y
					} else {
						currentRunLength++
					}
				} else {
					if currentRunLength > 0 {
						tiles = append(tiles, EntryV3{currentID, entry.Offset, entry.Length, currentRunLength})
					}
					currentRunLength = 0
				}
			}
			if currentRunLength > 0 {
				tiles = append(tiles, EntryV3{currentID, entry.Offset, entry.Length, currentRunLength})
			}
		}
	}
	return tiles, leaves
}

// Given a tile entries for a Source archive, sorted in TileID order,
// return:
// * Re-encoded tile-entries, with their offsets changed to contiguous (clustered) order in a new archive.
// * SrcDstRange: slice of offsets in the source archive, offset in the new archive, and length.
//   - Each range is one or more tiles
//   - the output must not have contiguous entries
//   - It is sorted by new offsets, but not necessarily by source offsets
//
// * The total size of the tile section in the new archive
// * The # of addressed tiles (sum over RunLength)
// * # the number of unique offsets ("tile contents")
//   - this might not be the last SrcDstRange new_offset + length, it's the highest offset (can be in the middle)
func reencodeEntries(dir []EntryV3) ([]EntryV3, []srcDstRange, uint64, uint64, uint64) {
	reencoded := make([]EntryV3, 0, len(dir))
	seenOffsets := make(map[uint64]uint64)
	ranges := make([]srcDstRange, 0)
	addressedTiles := uint64(0)

	dstOffset := uint64(0)
	for _, entry := range dir {
		if val, ok := seenOffsets[entry.Offset]; ok {
			reencoded = append(reencoded, EntryV3{entry.TileID, val, entry.Length, entry.RunLength})
		} else {
			if len(ranges) > 0 {
				lastRange := ranges[len(ranges)-1]
				if lastRange.SrcOffset+lastRange.Length == entry.Offset {
					ranges[len(ranges)-1].Length += uint64(entry.Length)
				} else {
					ranges = append(ranges, srcDstRange{entry.Offset, dstOffset, uint64(entry.Length)})
				}
			} else {
				ranges = append(ranges, srcDstRange{entry.Offset, dstOffset, uint64(entry.Length)})
			}

			reencoded = append(reencoded, EntryV3{entry.TileID, dstOffset, entry.Length, entry.RunLength})
			seenOffsets[entry.Offset] = dstOffset
			dstOffset += uint64(entry.Length)
		}

		addressedTiles += uint64(entry.RunLength)
	}
	return reencoded, ranges, dstOffset, addressedTiles, uint64(len(seenOffsets))
}

// "want the next N bytes, then discard N bytes"
type copyDiscard struct {
	Wanted  uint64
	Discard uint64
}

type overfetchRange struct {
	Rng          srcDstRange
	CopyDiscards []copyDiscard
}

// A single request, where only some of the bytes
// in the requested range we want
type overfetchListItem struct {
	Rng          srcDstRange
	CopyDiscards []copyDiscard
	BytesToNext  uint64 // the "priority"
	prev         *overfetchListItem
	next         *overfetchListItem
	index        int
}

// mergeRanges takes a slice of SrcDstRanges, that:
// * is non-contiguous, and is sorted by DstOffset
// * an Overfetch parameter
//   - overfetch = 0.2 means we can request an extra 20%
//   - overfetch = 1.00 means we can double our total transfer size
// * a maxRangeSize parameter
//   - ranges are never merged beyond this size, so each request is
//     independently retryable without re-downloading huge amounts of data
//
// Return a list of OverfetchRanges
//
//	Each OverfetchRange is one or more input ranges
//	input ranges are merged in order of smallest byte distance to next range
//	until the overfetch budget is consumed or the max range size is reached.
//	The list is sorted by Length
func mergeRanges(ranges []srcDstRange, overfetch float32, maxRangeSize uint64) (*list.List, uint64) {
	totalSize := 0

	shortest := make([]*overfetchListItem, len(ranges))

	// create the heap items
	for i, rng := range ranges {
		var bytesToNext int64
		if i == len(ranges)-1 {
			bytesToNext = math.MaxInt64
		} else {
			bytesToNext = int64(ranges[i+1].SrcOffset) - (int64(rng.SrcOffset) + int64(rng.Length))
			if bytesToNext < 0 {
				bytesToNext = math.MaxInt64
			}
		}

		shortest[i] = &overfetchListItem{
			Rng:          rng,
			BytesToNext:  uint64(bytesToNext),
			CopyDiscards: []copyDiscard{{uint64(rng.Length), 0}},
		}
		totalSize += int(rng.Length)
	}

	// make the list doubly-linked
	for i, item := range shortest {
		if i > 0 {
			item.prev = shortest[i-1]
		}
		if i < len(shortest)-1 {
			item.next = shortest[i+1]
		}
	}

	overfetchBudget := int(float32(totalSize) * overfetch)

	// sort by ascending distance to next range
	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].BytesToNext < shortest[j].BytesToNext
	})

	// while we haven't consumed the budget, merge ranges
	for (len(shortest) > 1) && (overfetchBudget-int(shortest[0].BytesToNext) >= 0) {
		item := shortest[0]

		// skip merge if it would exceed the max range size
		newLength := item.Rng.Length + item.BytesToNext + item.next.Rng.Length
		if maxRangeSize > 0 && newLength > maxRangeSize {
			shortest = shortest[1:]
			continue
		}

		// merge this item into item.next
		item.next.Rng = srcDstRange{item.Rng.SrcOffset, item.Rng.DstOffset, newLength}
		item.next.prev = item.prev
		if item.prev != nil {
			item.prev.next = item.next
		}
		item.CopyDiscards[len(item.CopyDiscards)-1].Discard = item.BytesToNext
		item.next.CopyDiscards = append(item.CopyDiscards, item.next.CopyDiscards...)

		shortest = shortest[1:]

		overfetchBudget -= int(item.BytesToNext)
	}

	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].Rng.Length > shortest[j].Rng.Length
	})

	totalBytes := uint64(0)
	result := list.New()
	for _, x := range shortest {
		result.PushBack(overfetchRange{
			Rng:          x.Rng,
			CopyDiscards: x.CopyDiscards,
		})
		totalBytes += x.Rng.Length
	}

	return result, totalBytes
}

// splitRanges breaks any overfetchRange larger than maxRangeSize into
// multiple chunks. Each chunk is a separate overfetchRange with a single
// copyDiscard, so it can be downloaded and retried independently.
// maxRangeSize of 0 means no splitting.
func splitRanges(ranges *list.List, maxRangeSize uint64) *list.List {
	if maxRangeSize == 0 {
		return ranges
	}
	result := list.New()
	for e := ranges.Front(); e != nil; e = e.Next() {
		or := e.Value.(overfetchRange)
		if or.Rng.Length <= maxRangeSize {
			result.PushBack(or)
			continue
		}
		// Walk the CopyDiscards, emitting chunks of at most maxRangeSize.
		remaining := maxRangeSize
		srcOff := or.Rng.SrcOffset
		dstOff := or.Rng.DstOffset
		chunkCd := copyDiscard{}
		chunkStarted := false
		for _, cd := range or.CopyDiscards {
			wanted := cd.Wanted
			discard := cd.Discard
			for wanted > 0 || discard > 0 {
				if remaining == maxRangeSize {
					// starting a new chunk
					chunkStarted = true
				}
				takeW := wanted
				if takeW > remaining {
					takeW = remaining
				}
				chunkCd.Wanted += takeW
				remaining -= takeW
				wanted -= takeW
				srcOff += takeW
				dstOff += takeW
				if remaining == 0 {
					result.PushBack(overfetchRange{
						Rng:          srcDstRange{srcOff - chunkCd.Wanted - chunkCd.Discard, dstOff - chunkCd.Wanted - chunkCd.Discard, chunkCd.Wanted + chunkCd.Discard},
						CopyDiscards: []copyDiscard{chunkCd},
					})
					chunkCd = copyDiscard{}
					remaining = maxRangeSize
					chunkStarted = false
				}
				if wanted == 0 && discard > 0 {
					takeD := discard
					if takeD > remaining {
						takeD = remaining
					}
					chunkCd.Discard += takeD
					remaining -= takeD
					discard -= takeD
					srcOff += takeD
					if remaining == 0 {
						result.PushBack(overfetchRange{
							Rng:          srcDstRange{srcOff - chunkCd.Wanted - chunkCd.Discard, dstOff - chunkCd.Wanted, chunkCd.Wanted + chunkCd.Discard},
							CopyDiscards: []copyDiscard{chunkCd},
						})
						chunkCd = copyDiscard{}
						remaining = maxRangeSize
						chunkStarted = false
					}
				}
			}
		}
		if chunkStarted {
			result.PushBack(overfetchRange{
				Rng:          srcDstRange{srcOff - chunkCd.Wanted - chunkCd.Discard, dstOff - chunkCd.Wanted, chunkCd.Wanted + chunkCd.Discard},
				CopyDiscards: []copyDiscard{chunkCd},
			})
		}
	}
	return result
}
// 1. Get the root directory (check that it is clustered)
// 2. Turn the input geometry into a relevance bitmap (using min(maxzoom, headermaxzoom))
// 3. Get all relevant level 1 directories (if any)
// 4. Get all relevant level 2 directories (usually none)
// 5. With the existing directory + relevance bitmap, construct
//   - a new total directory (root + leaf directories)
//   - a sorted slice of byte ranges in the old file required
//
// 6. Merge requested ranges using an overfetch parametter
// 7. write the modified header
// 8. write the root directory.
// 9. get and write the metadata.
// 10. write the leaf directories (if any)
// 11. Get all tiles, and write directly to the output.
func Extract(ctx context.Context, logger *log.Logger, bucketURL string, key string, minzoom int8, maxzoom int8, regionFile string, bbox string, output string, downloadThreads int, overfetch float32, dryRun bool, maxRetries int, retryBackoff time.Duration, maxRangeSize uint64) error {
	// 1. fetch the header
	start := time.Now()

	// Clamp negative retry params to 0 and warn so typos are not silent.
	if maxRetries < 0 {
		logger.Printf("warning: maxRetries=%d is negative; clamping to 0 (no retries)\n", maxRetries)
		maxRetries = 0
	}
	if retryBackoff < 0 {
		logger.Printf("warning: retryBackoff=%v is negative; clamping to 0 (retry immediately)\n", retryBackoff)
		retryBackoff = 0
	}

	if ctx == nil {
		ctx = context.Background()
	}

	bucketURL, key, err := NormalizeBucketKey(bucketURL, "", key)

	if err != nil {
		return err
	}

	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, key, 0, HeaderV3LenBytes)

	if err != nil {
		return fmt.Errorf("Failed to create range reader for %s, %w", key, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	r.Close()

	header, err := DeserializeHeader(b[0:HeaderV3LenBytes])

	if !header.Clustered {
		return fmt.Errorf("source archive must be clustered for extracts")
	}

	sourceMetadataOffset := header.MetadataOffset
	sourceTileDataOffset := header.TileDataOffset

	if minzoom == -1 || int8(header.MinZoom) > minzoom {
		minzoom = int8(header.MinZoom)
	}

	if maxzoom == -1 || int8(header.MaxZoom) < maxzoom {
		maxzoom = int8(header.MaxZoom)
	}

	if minzoom > maxzoom {
		return fmt.Errorf("minzoom cannot be greater than maxzoom")
	}

	var relevantSet *roaring64.Bitmap
	if regionFile != "" || bbox != "" {
		if regionFile != "" && bbox != "" {
			return fmt.Errorf("only one of region and bbox can be specified")
		}

		var multipolygon orb.MultiPolygon

		if regionFile != "" {
			dat, _ := ioutil.ReadFile(regionFile)
			multipolygon, err = UnmarshalRegion(dat)

			if err != nil {
				return err
			}
		} else {
			multipolygon, err = BboxRegion(bbox)
			if err != nil {
				return err
			}
		}

		// 2. construct a relevance bitmap

		bound := multipolygon.Bound()

		boundarySet, interiorSet := bitmapMultiPolygon(uint8(maxzoom), multipolygon)
		relevantSet = boundarySet
		relevantSet.Or(interiorSet)
		generalizeOr(relevantSet, uint8(minzoom))

		header.MinLonE7 = int32(bound.Left() * 10000000)
		header.MinLatE7 = int32(bound.Bottom() * 10000000)
		header.MaxLonE7 = int32(bound.Right() * 10000000)
		header.MaxLatE7 = int32(bound.Top() * 10000000)
		header.CenterLonE7 = int32(bound.Center().X() * 10000000)
		header.CenterLatE7 = int32(bound.Center().Y() * 10000000)
	} else {
		relevantSet = roaring64.New()
		relevantSet.AddRange(ZxyToID(uint8(minzoom), 0, 0), ZxyToID(uint8(maxzoom)+1, 0, 0))
	}

	// 3. get relevant entries from root
	dirOffset := header.RootOffset
	dirLength := header.RootLength

	rootReader, err := bucket.NewRangeReader(ctx, key, int64(dirOffset), int64(dirLength))
	if err != nil {
		return err
	}
	defer rootReader.Close()
	rootBytes, err := io.ReadAll(rootReader)
	if err != nil {
		return err
	}

	rootDir := DeserializeEntries(bytes.NewBuffer(rootBytes), header.InternalCompression)

	tileEntries, leaves := RelevantEntries(relevantSet, uint8(maxzoom), rootDir)

	// 4. get all relevant leaf entries

	leafRanges := make([]srcDstRange, 0)
	for _, leaf := range leaves {
		leafRanges = append(leafRanges, srcDstRange{header.LeafDirectoryOffset + leaf.Offset, 0, uint64(leaf.Length)})
	}

	overfetchLeaves, _ := mergeRanges(leafRanges, overfetch, maxRangeSize)
	overfetchLeaves = splitRanges(overfetchLeaves, maxRangeSize)
	numOverfetchLeaves := overfetchLeaves.Len()
	logger.Printf("fetching %d dirs, %d chunks, %d requests\n", len(leaves), len(leafRanges), overfetchLeaves.Len())

	for {
		if overfetchLeaves.Len() == 0 {
			break
		}
		or := overfetchLeaves.Remove(overfetchLeaves.Front()).(overfetchRange)

		desc := fmt.Sprintf("leaf dir (src=%d, len=%d)", or.Rng.SrcOffset, or.Rng.Length)
		err := withRetry(ctx, logger, maxRetries, retryBackoff, desc, func() error {
			chunkReader, err := bucket.NewRangeReader(ctx, key, int64(or.Rng.SrcOffset), int64(or.Rng.Length))
			if err != nil {
				return err
			}
			defer chunkReader.Close()

			for _, cd := range or.CopyDiscards {
				leafBytes := make([]byte, cd.Wanted)
				_, err := io.ReadFull(chunkReader, leafBytes)
				if err != nil {
					return err
				}
				leafdir := DeserializeEntries(bytes.NewBuffer(leafBytes), header.InternalCompression)
				newEntries, newLeaves := RelevantEntries(relevantSet, uint8(maxzoom), leafdir)

				if len(newLeaves) > 0 {
					panic("This doesn't support leaf level 2+.")
				}
				tileEntries = append(tileEntries, newEntries...)

				_, err = io.CopyN(io.Discard, chunkReader, int64(cd.Discard))
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	sort.Slice(tileEntries, func(i, j int) bool {
		return tileEntries[i].TileID < tileEntries[j].TileID
	})

	logger.Printf("Region tiles %d, result tile entries %d\n", relevantSet.GetCardinality(), len(tileEntries))

	// 6. create the new header and chunk list
	// we now need to re-encode this entry list using cumulative offsets
	reencoded, tileParts, tiledataLength, addressedTiles, tileContents := reencodeEntries(tileEntries)

	overfetchRanges, totalBytes := mergeRanges(tileParts, overfetch, maxRangeSize)
	overfetchRanges = splitRanges(overfetchRanges, maxRangeSize)

	numOverfetchRanges := overfetchRanges.Len()
	logger.Printf("fetching %d tiles, %d chunks, %d requests\n", len(reencoded), len(tileParts), overfetchRanges.Len())

	// TODO: takes up too much RAM
	// construct the directories
	newRootBytes, newLeavesBytes, _ := BuildDirectories(reencoded, 16384-HeaderV3LenBytes, Gzip)

	// 7. write the modified header
	header.RootOffset = HeaderV3LenBytes
	header.RootLength = uint64(len(newRootBytes))
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.LeafDirectoryLength = uint64(len(newLeavesBytes))
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength

	header.TileDataLength = tiledataLength
	header.AddressedTilesCount = addressedTiles
	header.TileEntriesCount = uint64(len(tileEntries))
	header.TileContentsCount = tileContents

	header.MaxZoom = uint8(maxzoom)
	header.MinZoom = uint8(minzoom)
	if header.CenterZoom < header.MinZoom {
		header.CenterZoom = header.MinZoom
	} else if header.CenterZoom > header.MaxZoom {
		header.CenterZoom = header.MaxZoom
	}

	headerBytes := SerializeHeader(header)

	totalActualBytes := uint64(0)
	for _, x := range tileParts {
		totalActualBytes += x.Length
	}

	if !dryRun {

		outfile, err := os.Create(output)
		defer outfile.Close()

		if err != nil {
			return err
		}

		// set the file size and write empty space for the header for now
		// see comment below
		outfile.Truncate(HeaderV3LenBytes + int64(len(newRootBytes)) + int64(header.MetadataLength) + int64(len(newLeavesBytes)) + int64(totalActualBytes))
		_, err = outfile.Write(make([]byte, HeaderV3LenBytes))
		if err != nil {
			return err
		}

		// 8. write the root directory
		_, err = outfile.Write(newRootBytes)
		if err != nil {
			return err
		}

		// 9. get and write the metadata
		metadataReader, err := bucket.NewRangeReader(ctx, key, int64(sourceMetadataOffset), int64(header.MetadataLength))
		if err != nil {
			return err
		}
		metadataBytes, err := io.ReadAll(metadataReader)
		defer metadataReader.Close()
		if err != nil {
			return err
		}

		_, err = outfile.Write(metadataBytes)
		if err != nil {
			return err
		}

		// 10. write the leaf directories
		_, err = outfile.Write(newLeavesBytes)
		if err != nil {
			return err
		}

		bar := defaultBytesProgressbar(
			logger,
			int64(totalBytes),
			"fetching chunks",
		)

		var mu sync.Mutex

		// Use the errgroup's derived context so a permanent error in one
		// worker cancels sibling workers promptly.
		errs, ctx := errgroup.WithContext(ctx)

		// downloadPart fetches an overfetchRange with retry on transient errors.
		// maxRetries=5 means up to 6 total attempts (attempt 0..5).
		downloadPart := func(or overfetchRange) error {
			return downloadWithRetry(ctx, logger, bucket, key, or, sourceTileDataOffset, header.TileDataOffset, outfile, bar, maxRetries, retryBackoff)
		}

		for i := 0; i < downloadThreads; i++ {
			workBack := (i == 0 && downloadThreads > 1)
			errs.Go(func() error {
				done := false
				var or overfetchRange
				for {
					mu.Lock()
					if overfetchRanges.Len() == 0 {
						done = true
					} else {
						if workBack {
							or = overfetchRanges.Remove(overfetchRanges.Back()).(overfetchRange)
						} else {
							or = overfetchRanges.Remove(overfetchRanges.Front()).(overfetchRange)
						}
					}
					mu.Unlock()
					if done {
						return nil
					}
					err := downloadPart(or)
					if err != nil {
						return err
					}
				}
			})
		}

		err = errs.Wait()
		if err != nil {
			return err
		}

		// Write the header when finishing,
		// otherwise a extract cancelled during the tile download section
		// will appear valid with "pmtiles verify"
		// even though the tile contents are corrupted.
		outfile.Seek(0, io.SeekStart)
		_, err = outfile.Write(headerBytes)
		if err != nil {
			return err
		}
	}

	logger.Printf("Completed in %v with %v download threads (%v tiles/s).\n", time.Since(start), downloadThreads, float64(len(reencoded))/float64(time.Since(start).Seconds()))
	totalRequests := 2                  // header + root
	totalRequests += numOverfetchLeaves // leaves
	totalRequests++                     // metadata
	totalRequests += numOverfetchRanges
	logger.Printf("Extract required %d total requests.\n", totalRequests)
	logger.Printf("Extract transferred %s (overfetch %v) for an archive size of %s\n", humanize.Bytes(totalBytes), overfetch, humanize.Bytes(totalActualBytes))

	return nil
}

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
	// net.OpError: retry transient network errors (connection reset, etc.).
	// This is intentionally permissive — retrying a permanent error only
	// costs a few attempts, while failing fast on a transient one loses the
	// whole extract.
	var netErr *net.OpError
	if errors.As(err, &netErr) {
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
		// url.Error wrapping a net.OpError (e.g. connection reset) is retryable.
		var innerNetErr *net.OpError
		if errors.As(urlErr.Err, &innerNetErr) {
			return true
		}
		return false
	}
	// Typed HTTP status errors from NewRangeReaderEtag.
	var httpErr *httpStatusError
	if errors.As(err, &httpErr) {
		switch {
		case httpErr.Code == http.StatusRequestTimeout, httpErr.Code == http.StatusTooManyRequests:
			return true
		case httpErr.Code >= 500 && httpErr.Code <= 599:
			return true
		default:
			return false
		}
	}
	// Default: do not retry unknown errors.
	return false
}

// backoffDuration returns the delay before retrying the given attempt
// (1-based). The delay is exponential with jitter, capped at 30 seconds.
//
//	delay = min(base * 2^(attempt-1) + jitter, 30s)
//
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

// withRetry calls fn up to maxRetries+1 times, retrying on transient errors
// with exponential backoff + jitter. It logs retry attempts and respects
// context cancellation during backoff. Non-retryable errors fail fast.
func withRetry(ctx context.Context, logger *log.Logger, maxRetries int, retryBackoff time.Duration, desc string, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := backoffDuration(attempt, retryBackoff)
			logger.Printf("retrying %s attempt %d/%d after %v: %v\n", desc, attempt, maxRetries, delay, lastErr)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryableError(err) {
			return err
		}
	}
	return fmt.Errorf("%s failed after %d retries: %w", desc, maxRetries, lastErr)
}

// downloadRangeOnce fetches a single overfetchRange and writes it to the
// output file at the range's DstOffset. It does not retry.
func downloadRangeOnce(ctx context.Context, bucket Bucket, key string, or overfetchRange, sourceTileDataOffset uint64, tileDataOffset uint64, outfile *os.File, bar io.Writer) error {
	tileReader, err := bucket.NewRangeReader(ctx, key, int64(sourceTileDataOffset+or.Rng.SrcOffset), int64(or.Rng.Length))
	if err != nil {
		return err
	}
	defer tileReader.Close()
	offsetWriter := io.NewOffsetWriter(outfile, int64(tileDataOffset)+int64(or.Rng.DstOffset))

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

// downloadWithRetry fetches an overfetchRange with retry on transient errors.
// maxRetries=5 means up to 6 total attempts (attempt 0..5).
func downloadWithRetry(ctx context.Context, logger *log.Logger, bucket Bucket, key string, or overfetchRange, sourceTileDataOffset uint64, tileDataOffset uint64, outfile *os.File, bar io.Writer, maxRetries int, retryBackoff time.Duration) error {
	desc := fmt.Sprintf("range (src=%d, len=%d)", or.Rng.SrcOffset, or.Rng.Length)
	return withRetry(ctx, logger, maxRetries, retryBackoff, desc, func() error {
		return downloadRangeOnce(ctx, bucket, key, or, sourceTileDataOffset, tileDataOffset, outfile, bar)
	})
}
