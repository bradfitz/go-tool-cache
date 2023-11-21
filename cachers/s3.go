package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	OutputIdKey = "outputid"
)

type S3Cache struct {
	Bucket string
	cfg    *aws.Config
	// diskCache is where to write the output files to local disk, as required by the
	// cache protocol.
	diskCache *DiskCache

	prefix string
	// verbose optionally specifies whether to log verbose messages.
	verbose bool

	s3Client              *s3.Client
	bytesDownloaded       int64
	bytesUploaded         int64
	downloadCount         int64
	uploadCount           int64
	avgBytesDownloadSpeed float64
	avgBytesUploadSpeed   float64
	uploadStatsChan       chan Stats
	FinishedUploadStats   chan bool
	done                  chan bool
	downloadStatsChan     chan Stats
	close                 sync.Once
}

type Stats struct {
	Bytes int64
	Speed float64
}

func NewS3Cache(bucketName string, cfg *aws.Config, cacheKey string, disk *DiskCache, verbose bool) *S3Cache {
	// get current architecture
	arc := runtime.GOARCH
	// get current operating system
	os := runtime.GOOS
	prefix := fmt.Sprintf("cache/%s/%s/%s", cacheKey, arc, os)
	log.Printf("S3Cache: configured to s3://%s/%s", bucketName, prefix)
	cache := &S3Cache{
		Bucket:            bucketName,
		cfg:               cfg,
		diskCache:         disk,
		prefix:            prefix,
		verbose:           verbose,
		uploadStatsChan:   make(chan Stats),
		downloadStatsChan: make(chan Stats, 100),
		done:              make(chan bool),
	}
	cache.StartStatsGathering()
	return cache
}

func (s *S3Cache) client() (*s3.Client, error) {
	if s.s3Client != nil {
		return s.s3Client, nil
	}
	s.s3Client = s3.NewFromConfig(*s.cfg)
	return s.s3Client, nil
}

func isNotFoundError(err error) bool {
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			code := ae.ErrorCode()
			return code == "AccessDenied" || code == "NoSuchKey"
		}
	}
	return false
}

func (s *S3Cache) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	outputID, diskPath, err = s.diskCache.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}
	client, err := s.client()
	if err != nil {
		if s.verbose {
			log.Printf("error getting S3 client: %v", err)
		}
		return "", "", err
	}
	actionKey := s.actionKey(actionID)

	outputResult, getOutputErr := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.Bucket,
		Key:    &actionKey,
	})
	if isNotFoundError(getOutputErr) {
		// handle object not found
		return "", "", nil
	} else if getOutputErr != nil {
		if s.verbose {
			log.Printf("error S3 get for %s:  %v", actionKey, getOutputErr)
		}
		return "", "", fmt.Errorf("unexpected S3 get for %s:  %v", actionKey, getOutputErr)
	}
	contentSize := outputResult.ContentLength
	outputID, ok := outputResult.Metadata[OutputIdKey]
	if !ok || outputID == "" {
		return "", "", fmt.Errorf("outputId not found in metadata")
	}
	content := outputResult.Body
	downloadFunc := func() error {
		defer outputResult.Body.Close()
		diskPath, err = s.diskCache.Put(ctx, actionID, outputID, contentSize, content)
		if err != nil {
			return err
		}
		return nil
	}
	if s.verbose {
		speed, err := DoAndMeasureSpeed(contentSize, downloadFunc)
		if err == nil {
			s.downloadStatsChan <- Stats{
				Bytes: contentSize,
				Speed: speed,
			}
		} else {
			log.Printf("error downloading %s: %v", actionKey, err)
		}
	} else {
		err = downloadFunc()
	}

	return outputID, diskPath, err
}

func (s *S3Cache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/%s", s.prefix, actionID)
}

func (s *S3Cache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	// Write to disk locally as we write it remotely, as we need to guarantee
	// it's on disk locally for the caller.
	var bytesReaderForDisk io.Reader
	var bytesBufferForS3 bytes.Buffer

	if size == 0 {
		bytesReaderForDisk = bytes.NewReader(nil)
		bytesBufferForS3 = bytes.Buffer{}
	} else {
		bytesReaderForDisk = io.TeeReader(body, &bytesBufferForS3)
	}

	diskPath, err := s.diskCache.Put(ctx, actionID, outputID, size, bytesReaderForDisk)
	if err != nil {
		return "", err
	}
	client, err := s.client()
	if err != nil {
		return "", err
	}
	s.uploadOutput(ctx, actionID, outputID, client, bytesBufferForS3, size)
	return
}

func (s *S3Cache) uploadOutput(ctx context.Context, actionId, outputID string, client *s3.Client, readerForS3 bytes.Buffer, size int64) {
	outputKey := s.actionKey(actionId)
	putObjectFunc := func() error {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        &s.Bucket,
			Key:           &outputKey,
			Body:          &readerForS3,
			ContentLength: size,
			Metadata: map[string]string{
				OutputIdKey: outputID,
			},
		})
		return err
	}
	if s.verbose {
		speed, err := DoAndMeasureSpeed(size, putObjectFunc)
		if err == nil {
			s.uploadStatsChan <- Stats{
				Bytes: size,
				Speed: speed,
			}
		}
	} else {
		_ = putObjectFunc()
	}
}

func (s *S3Cache) BytesDownloaded() int64 {
	return s.bytesDownloaded
}

func (s *S3Cache) BytesUploaded() int64 {
	return s.bytesUploaded
}

func (s *S3Cache) AvgBytesDownloadSpeed() float64 {
	return s.avgBytesDownloadSpeed
}

func (s *S3Cache) AvgBytesUploadSpeed() float64 {
	return s.avgBytesUploadSpeed
}

func newAverage(oldAverage float64, count int64, newValue float64) float64 {
	return (oldAverage*float64(count) + newValue) / float64(count+1)
}

func DoAndMeasureSpeed(dataSize int64, functionOnData func() error) (float64, error) {
	start := time.Now()
	err := functionOnData()
	elapsed := time.Since(start)
	speed := float64(dataSize) / elapsed.Seconds()
	return speed, err
}

func (s *S3Cache) StartStatsGathering() {
	go func() {
		for stats := range s.uploadStatsChan {
			s.bytesUploaded += stats.Bytes
			s.avgBytesUploadSpeed = newAverage(s.avgBytesUploadSpeed, s.uploadCount, stats.Speed)
			s.uploadCount++
		}
		s.done <- true
	}()
	go func() {
		for stats := range s.downloadStatsChan {
			s.bytesDownloaded += stats.Bytes
			s.avgBytesDownloadSpeed = newAverage(s.avgBytesDownloadSpeed, s.downloadCount, stats.Speed)
			s.downloadCount++
		}
		s.done <- true
	}()
}

func (s *S3Cache) Close() {
	close(s.downloadStatsChan)
	close(s.uploadStatsChan)
	<-s.done
	<-s.done
}
