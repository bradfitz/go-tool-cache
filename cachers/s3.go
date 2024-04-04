package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"

	"github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	outputIDMetadataKey = "outputid"
)

// s3Client represents the functions we need from the S3 client
type s3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3Cache is a remote cache that is backed by S3 bucket
type S3Cache struct {
	bucket           string
	prefix           string
	log              *slog.Logger
	s3Client         s3Client
	SkipZeroBytePuts bool
}

var _ RemoteCache = &S3Cache{}

func (s *S3Cache) Start(context.Context) error {
	s.log.Info("start", "bucket", s.bucket, "prefix", s.prefix)
	return nil
}

func (s *S3Cache) Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error) {
	s.log.Debug("get", "actionID", actionID)
	actionKey := s.actionKey(actionID)
	outputResult, getOutputErr := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &actionKey,
	})
	if isNotFoundError(getOutputErr) {
		// handle object not found
		return "", 0, nil, nil
	} else if getOutputErr != nil {
		return "", 0, nil, fmt.Errorf("unexpected S3 get for %s:  %v", actionKey, getOutputErr)
	}
	contentSize := outputResult.ContentLength
	outputID, ok := outputResult.Metadata[outputIDMetadataKey]
	if !ok || outputID == "" {
		return "", 0, nil, fmt.Errorf("outputId not found in metadata")
	}
	return outputID, *contentSize, outputResult.Body, nil
}

func (s *S3Cache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (err error) {
	if size == 0 {
		if s.SkipZeroBytePuts {
			s.log.Debug("put (skip)", "actionID", actionID, "outputID", outputID, "size", size)
			return nil
		}
		body = bytes.NewReader(nil)
	}
	s.log.Debug("put", "actionID", actionID, "outputID", outputID, "size", size)
	// The AWS SDK seems to fail to ReadAll the whole Reader sometimes, which leads to SignatueDoesNotMatch errors (and presumably truncated data??), so we read the whole thing into memory. Normally this is memory-inefficient, but since the SDK needs the whole body to compute the signature (namely length), it should be fine. (And hopefully it's not copyying the whole thing again.)
	buf, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("error reading body: %w", err)
	}
	if int64(len(buf)) != size {
		return fmt.Errorf("size mismatch: expected %d, got %d: %q", size, len(buf), buf)
	}
	body = bytes.NewReader(buf)
	actionKey := s.actionKey(actionID)
	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           &actionKey,
		Body:          body,
		ContentLength: &size,
		Metadata: map[string]string{
			outputIDMetadataKey: outputID,
		},
	})
	if err != nil {
		return err
	}
	return
}

func (s *S3Cache) Close() error {
	s.log.Info("close")
	return nil
}

func NewS3Cache(client s3Client, bucketName string, cacheKey string) *S3Cache {
	// get target architecture
	goarch := os.Getenv("GOARCH")
	if goarch == "" {
		goarch = runtime.GOARCH
	}
	// get target operating system
	goos := os.Getenv("GOOS")
	if goos == "" {
		goos = runtime.GOOS
	}
	// TOOD: I don't love how "cache" is in here
	prefix := fmt.Sprintf("cache/%s/%s/%s", cacheKey, goarch, goos)
	cache := &S3Cache{
		s3Client: client,
		bucket:   bucketName,
		prefix:   prefix,
		log:      slog.Default().WithGroup("s3"),
	}
	return cache
}

func isNotFoundError(err error) bool {
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

func (s *S3Cache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/%s", s.prefix, actionID)
}
