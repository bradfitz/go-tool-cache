package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"go.uber.org/atomic"
)

type putWork struct {
	actionID string
	outputID string
	size     int64
	diskPath string
}

type DiskAsyncS3Cache struct {
	Counts
	log         *slog.Logger
	diskCache   *DiskCache
	s3Client    s3Client
	bucketName  string
	s3Prefix    string
	remoteWork  chan putWork
	remoteWG    *sync.WaitGroup
	HistS3GetMS *hdrhistogram.Histogram
	// total time spent in S3 gets
	SumS3Get    atomic.Duration
	HistS3PutMS *hdrhistogram.Histogram
	// total time spent in S3 puts
	SumS3Put            atomic.Duration
	HistS3GetBytesPerMS *hdrhistogram.Histogram
	HistS3PutBytesPerMS *hdrhistogram.Histogram
	nWorkers            int
}

const (
	outputIDMetadataKey = "outputid"
)

type s3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

func NewDiskAsyncS3Cache(diskCache *DiskCache, client s3Client, bucketName string, s3Prefix string, queueLen int, nWorkers int) *DiskAsyncS3Cache {
	return &DiskAsyncS3Cache{
		log:        slog.Default().WithGroup("DiskAsyncS3"),
		remoteWork: make(chan putWork, queueLen),
		remoteWG:   &sync.WaitGroup{},
		nWorkers:   nWorkers,
		s3Client:   client,
		bucketName: bucketName,
		// NOTE: previous incarnations contained the GOARCH and GOOS in the cache key, but I'm pretty sure they need not be separate
		s3Prefix: s3Prefix,
		// note: we initialize remoteWG in Start
		// TODO: instead of making wrappers, just integrate Counts with the real things
		diskCache: diskCache,
		// TODO: tune; these are guesses
		HistS3GetMS: hdrhistogram.New(1, 1_000_000_000, 3),
		HistS3PutMS: hdrhistogram.New(1, 1_000_000_000, 3),
		// 1 GB/s would be a lot
		HistS3GetBytesPerMS: hdrhistogram.New(1, 1_000_000_000, 3),
		HistS3PutBytesPerMS: hdrhistogram.New(1, 1_000_000_000, 3),
	}
}

func (c *DiskAsyncS3Cache) Start(ctx context.Context) error {
	err := c.diskCache.Start(ctx)
	if err != nil {
		return fmt.Errorf("local cache start failed: %w", err)
	}

	c.log.Info("probing remote cache")
	probeStr := "_probe"
	err = c.s3Put(ctx, probeStr, probeStr, int64(len([]byte(probeStr))), bytes.NewReader([]byte(probeStr)))
	if err != nil {
		c.diskCache.Close()
		return fmt.Errorf("remote cache probe put failed: %w", err)
	}
	_, sz, _, err := c.s3Get(ctx, probeStr)
	if err != nil {
		c.diskCache.Close()
		return fmt.Errorf("remote cache probe get failed: %w", err)
	}
	if sz != int64(len([]byte(probeStr))) {
		c.diskCache.Close()
		return fmt.Errorf("remote cache probe get size mismatch: expected %d, got %d", len([]byte(probeStr)), sz)
	}
	c.log.Info("probe success")

	c.remoteWG.Add(c.nWorkers)
	for i := 0; i < c.nWorkers; i++ {
		go func() {
			defer c.remoteWG.Done()
			for {
				select {
				case w, ok := <-c.remoteWork:
					if !ok {
						c.log.Info("s3 worker done by closed work channel")
						return
					}
					c.log.Debug("s3 put", "actionID", w.actionID, "outputID", w.outputID, "size", w.size, "diskPath", w.diskPath)
					var r io.Reader
					if w.size == 0 {
						r = bytes.NewReader(nil)
					} else {
						f, err := os.Open(w.diskPath)
						// TODO: currently we just log errors, but maybe we want a mode that fails
						if err != nil {
							// TODO: not sure if this shouuld be counted in Counts; those are for s3
							c.log.Error("opening file for remote", "path", w.diskPath, "err", err)
							continue
						}
						defer f.Close()
						r = f
					}
					// TODO: not 100% on the lifetime of this context; is it until everything is started? or until Close? we may want a separate Context for workers so that they can be stopped before all work is done (i.e., on Close)
					err := c.s3Put(ctx, w.actionID, w.outputID, w.size, r)
					if err != nil {
						c.log.Info("putting to remote", "actionID", w.actionID, "outputID", w.outputID, "err", err)
						continue
					}
				case <-ctx.Done():
					c.log.Info("s3 worker done by ctx.Done")
					return
				}
			}
		}()
	}

	return nil
}

func (c *DiskAsyncS3Cache) s3Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) error {
	c.Counts.puts.Add(1)
	if size == 0 {
		body = bytes.NewReader(nil)
	}
	c.log.Debug("s3 put", "actionID", actionID, "outputID", outputID, "size", size)
	actionKey := c.actionKey(actionID)
	start := time.Now()
	_, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &c.bucketName,
		Key:           &actionKey,
		Body:          body,
		ContentLength: &size,
		Metadata: map[string]string{
			outputIDMetadataKey: outputID,
		},
	})
	dur := time.Since(start)
	if err != nil {
		c.Counts.putErrors.Add(1)
		return err
	}
	// TODO: I'm assuming these are safe from multiple goroutines
	c.HistS3PutMS.RecordValue(dur.Milliseconds())
	c.HistS3PutBytesPerMS.RecordValue(size / dur.Milliseconds())
	c.SumS3Put.Add(dur)
	return nil
}

func (c *DiskAsyncS3Cache) s3Get(ctx context.Context, actionID string) (string, int64, io.ReadCloser, error) {
	c.log.Debug("s3 get", "actionID", actionID)
	c.Counts.gets.Add(1)
	actionKey := c.actionKey(actionID)
	start := time.Now()
	outputResult, getOutputErr := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucketName,
		Key:    &actionKey,
	})
	dur := time.Since(start)
	if isS3NotFoundError(getOutputErr) {
		c.Counts.misses.Add(1)
		// TODO: count miss
		return "", 0, nil, nil
	} else if getOutputErr != nil {
		c.Counts.getErrors.Add(1)
		return "", 0, nil, fmt.Errorf("unexpected S3 get for %s:  %v", actionKey, getOutputErr)
	}
	size := *outputResult.ContentLength
	outputID, ok := outputResult.Metadata[outputIDMetadataKey]
	if !ok || outputID == "" {
		c.Counts.getErrors.Add(1)
		return "", 0, nil, fmt.Errorf("outputId not found in metadata")
	}
	c.HistS3GetMS.RecordValue(dur.Milliseconds())
	c.log.Debug(fmt.Sprintf("bytes per ms: %d bytes / %d ms = %d B/ms", size, dur.Milliseconds(), size/dur.Milliseconds()))
	c.HistS3GetBytesPerMS.RecordValue(size / dur.Milliseconds())
	c.SumS3Get.Add(dur)
	c.Counts.hits.Add(1)
	return outputID, size, outputResult.Body, nil
}

func (c *DiskAsyncS3Cache) Get(ctx context.Context, actionID string) (string, string, error) {
	c.log.Debug("get", "actionID", actionID)
	outputID, diskPath, err := c.diskCache.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}
	outputID, size, output, err := c.s3Get(ctx, actionID)
	if err != nil {
		return "", "", err
	}
	// TODO: document when/why this happens
	if outputID == "" {
		return "", "", nil
	}
	diskPath, err = c.diskCache.Put(ctx, actionID, outputID, size, output)
	if err != nil {
		return "", "", err
	}
	return outputID, diskPath, nil
}

// TODO: there's a problem when the disk and s3 get out of sync: if the disk has a file that the s3 doesn't, it will never get put to s3. This is maybe fine, since eventually the disk cache will be cleared?
func (c *DiskAsyncS3Cache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (string, error) {
	c.log.Debug("put", "actionID", actionID, "outputID", outputID, "size", size)
	// special case for empty files, nead empty reader
	if size == 0 {
		body = bytes.NewReader(nil)
	}

	// TODO: restore metrics
	diskPath, err := c.diskCache.Put(ctx, actionID, outputID, size, body)
	if err != nil {
		return "", fmt.Errorf("local cache put failed: %w", err)
	}
	c.remoteWork <- putWork{
		actionID: actionID,
		outputID: outputID,
		size:     size,
		diskPath: diskPath,
	}
	return diskPath, nil
}

func (c *DiskAsyncS3Cache) Close() error {
	c.log.Info("close")
	var errAll error
	if err := c.diskCache.Close(); err != nil {
		errAll = errors.Join(fmt.Errorf("local cache stop failed: %w", err), errAll)
	}
	// TODO: this means we wait till all the remote workers finish; we may want to just abandon the rest of the work (or offer a mode)
	close(c.remoteWork)
	c.log.Info("waiting for s3 workers to finish")
	c.remoteWG.Wait()
	return errAll
}

func (c *DiskAsyncS3Cache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/%s", c.s3Prefix, actionID)
}

func isS3NotFoundError(err error) bool {
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			code := ae.ErrorCode()
			if code == "NoSuchKey" {
				return true
			}
			if code == "AccessDenied" {
				// technically if sig doesn't match, it is unknown whether found or not
				return !strings.Contains(ae.Error(), "SignatureDoesNotMatch")
			}
			return false
		}
	}
	return false
}
