package cachers

import (
	"bytes"
	"context"
	"encoding/json"
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

func (c *S3Cache) client() (*s3.Client, error) {
	if c.s3Client != nil {
		return c.s3Client, nil
	}
	c.s3Client = s3.NewFromConfig(*c.cfg)
	return c.s3Client, nil
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

func (c *S3Cache) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	outputID, diskPath, err = c.diskCache.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}
	client, err := c.client()
	if err != nil {
		if c.verbose {
			log.Printf("error getting S3 client: %v", err)
		}
		return "", "", err
	}
	actionKey := c.actionKey(actionID)
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.Bucket,
		Key:    &actionKey,
	})
	// handle object not found
	if isNotFoundError(err) {
		return "", "", nil
	} else if err != nil {
		if c.verbose {
			log.Printf("error S3 get for %s:  %v", actionKey, err)
		}
		return "", "", fmt.Errorf("unexpected S3 get for %s:  %v", actionKey, err)
	}
	defer result.Body.Close()
	var av ActionValue
	body, err := io.ReadAll(result.Body)
	if err != nil {
		return "", "", err
	}
	if err := json.Unmarshal(body, &av); err != nil {
		if c.verbose {
			log.Printf("error unmarshalling JSON for %s:  %v", actionKey, err)
		}
		return "", "", err
	}

	outputID = av.OutputID

	var putBody io.Reader
	if av.Size == 0 {
		putBody = bytes.NewReader(nil)
		diskPath, err = c.diskCache.Put(ctx, actionID, outputID, av.Size, putBody)
	} else {
		outputKey := c.outputKey(outputID)
		outputResult, getOutputErr := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &c.Bucket,
			Key:    &outputKey,
		})
		if isNotFoundError(getOutputErr) {
			// handle object not found
			return "", "", nil
		} else if getOutputErr != nil {
			if c.verbose {
				log.Printf("error S3 get for %s:  %v", outputKey, getOutputErr)
			}
			return "", "", fmt.Errorf("unexpected S3 get for %s:  %v", outputKey, getOutputErr)
		}
		downloadFunc := func() error {
			defer outputResult.Body.Close()
			putBody = outputResult.Body
			diskPath, err = c.diskCache.Put(ctx, actionID, outputID, av.Size, putBody)
			if err != nil {
				return err
			}
			return nil
		}
		if c.verbose {
			speed, err := DoAndMeasureSpeed(av.Size, downloadFunc)
			if err == nil {
				c.downloadStatsChan <- Stats{
					Bytes: outputResult.ContentLength,
					Speed: speed,
				}
			} else {
				log.Printf("error downloading %s: %v", outputKey, err)
			}
		} else {
			err = downloadFunc()
		}
	}
	return outputID, diskPath, err
}

func (c *S3Cache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/actions/%s", c.prefix, actionID)
}

func (c *S3Cache) outputKey(outputID string) string {
	return fmt.Sprintf("%s/output/%s", c.prefix, outputID)
}

func (c *S3Cache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	// Write to disk locally as we write it remotely, as we need to guarantee
	// it's on disk locally for the caller.
	var readerForDisk io.Reader
	var readerForS3 bytes.Buffer

	if size == 0 {
		// Special case the empty file so NewRequest sets "Content-Length: 0",
		// as opposed to thinking we didn't set it and not being able to sniff its size
		// from the type.
		readerForDisk = bytes.NewReader(nil)
	} else {
		readerForDisk = io.TeeReader(body, &readerForS3)
	}

	diskPath, err := c.diskCache.Put(ctx, actionID, outputID, size, readerForDisk)
	if err != nil {
		return "", err
	}

	client, err := c.client()
	if err != nil {
		return "", err
	}
	av := ActionValue{
		OutputID: outputID,
		Size:     size,
	}
	avj, err := json.Marshal(av)
	if err == nil {
		actionKey := c.actionKey(actionID)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &c.Bucket,
			Key:    &actionKey,
			Body:   bytes.NewReader(avj),
		})
	}
	if size > 0 && err == nil {
		c.uploadOutput(ctx, outputID, client, readerForS3, size)
	}
	return
}

func (c *S3Cache) uploadOutput(ctx context.Context, outputID string, client *s3.Client, readerForS3 bytes.Buffer, size int64) {
	outputKey := c.outputKey(outputID)
	putObjectFunc := func() error {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        &c.Bucket,
			Key:           &outputKey,
			Body:          &readerForS3,
			ContentLength: size,
		})
		return err
	}
	if c.verbose {
		speed, err := DoAndMeasureSpeed(size, putObjectFunc)
		if err == nil {
			c.uploadStatsChan <- Stats{
				Bytes: size,
				Speed: speed,
			}
		}
	} else {
		_ = putObjectFunc()
	}
}

func (c *S3Cache) BytesDownloaded() int64 {
	return c.bytesDownloaded
}

func (c *S3Cache) BytesUploaded() int64 {
	return c.bytesUploaded
}

func (c *S3Cache) AvgBytesDownloadSpeed() float64 {
	return c.avgBytesDownloadSpeed
}

func (c *S3Cache) AvgBytesUploadSpeed() float64 {
	return c.avgBytesUploadSpeed
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

func (c *S3Cache) StartStatsGathering() {
	go func() {
		for s := range c.uploadStatsChan {
			c.bytesUploaded += s.Bytes
			c.avgBytesUploadSpeed = newAverage(c.avgBytesUploadSpeed, c.uploadCount, s.Speed)
			c.uploadCount++
		}
		c.done <- true
	}()
	go func() {
		for s := range c.downloadStatsChan {
			c.bytesDownloaded += s.Bytes
			c.avgBytesDownloadSpeed = newAverage(c.avgBytesDownloadSpeed, c.downloadCount, s.Speed)
			c.downloadCount++
		}
		c.done <- true
	}()
}

func (c *S3Cache) Close() {
	close(c.downloadStatsChan)
	close(c.uploadStatsChan)
	<-c.done
	<-c.done
}
