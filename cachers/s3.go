package cachers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/bradfitz/go-tool-cache/internal/sbytes"
	"github.com/klauspost/compress/s2"
)

const (
	outputIDMetadataKey   = "outputid"
	compressedMetadataKey = "compressed"
	decompSizeMetadataKey = "decomp-size"
)

// s3Client represents the functions we need from the S3 client
type s3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3Cache is a remote cache that is backed by S3 bucket
type S3Cache struct {
	bucket string
	prefix string
	tags   string
	// verbose optionally specifies whether to log verbose messages.
	verbose  bool
	s3Client s3Client
}

var _ RemoteCache = &S3Cache{}

func (s *S3Cache) Kind() string {
	return "s3"
}

func (s *S3Cache) Start(context.Context) error {
	log.Printf("[%s]\tconfigured to s3://%s/%s", s.Kind(), s.bucket, s.prefix)
	return nil
}

func (s *S3Cache) Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error) {
	actionKey := s.actionKey(actionID)
	outputResult, getOutputErr := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &actionKey,
	})
	if s.verbose {
		log.Printf("[%s]\t GetObject: s3://%s/%s ok:%v", s.Kind(), s.bucket, actionKey, getOutputErr == nil)
	}
	if isNotFoundError(getOutputErr) {
		// handle object not found
		return "", 0, nil, nil
	} else if getOutputErr != nil {
		if s.verbose {
			log.Printf("error S3 get for %s:  %v", actionKey, getOutputErr)
		}
		return "", 0, nil, fmt.Errorf("unexpected S3 get for %s:  %v", actionKey, getOutputErr)
	}
	contentSize := outputResult.ContentLength
	outputID, ok := outputResult.Metadata[outputIDMetadataKey]
	if !ok || outputID == "" || contentSize == nil {
		return "", 0, nil, fmt.Errorf("outputId or contentSize not found in metadata")
	}
	if outputResult.Metadata[compressedMetadataKey] == "s2" {
		sz, err := strconv.Atoi(outputResult.Metadata[decompSizeMetadataKey])
		if err != nil {
			return "", 0, nil, err
		}
		*contentSize = int64(sz)
		outputResult.Body = struct {
			io.Reader
			io.Closer
		}{Reader: s2.NewReader(outputResult.Body), Closer: outputResult.Body}
	}
	return outputID, *contentSize, outputResult.Body, nil
}

var s2Encoders = sync.Pool{
	New: func() interface{} { return s2.NewWriter(nil, s2.WriterBlockSize(1<<20), s2.WriterBetterCompression()) },
}

func (s *S3Cache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (err error) {
	if size == 0 {
		body = sbytes.NewBuffer(nil)
	}

	actionKey := s.actionKey(actionID)
	if s.verbose {
		log.Printf("[%s]\t PutObject: s3://%s/%s", s.Kind(), s.bucket, actionKey)
	}
	metadata := map[string]string{
		outputIDMetadataKey: outputID,
	}

	if bb, ok := body.(*sbytes.Buffer); size > 8<<10 && ok {
		dst := sbytes.NewBuffer(make([]byte, 0, size/2))
		enc := s2Encoders.Get().(*s2.Writer)
		enc.Reset(dst)
		enc.EncodeBuffer(bb.Bytes())
		enc.Close()
		metadata[compressedMetadataKey] = "s2"
		metadata[decompSizeMetadataKey] = strconv.Itoa(int(size))
		enc.Reset(nil)
		s2Encoders.Put(enc)
		body = dst
		size = int64(dst.Len())
	}

	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           &actionKey,
		Body:          body,
		ContentLength: &size,
		Metadata:      metadata,
		Tagging:       &s.tags,
	}, func(options *s3.Options) {
		options.RetryMaxAttempts = 1 // We cannot perform seek in Body
	})
	if err != nil && s.verbose {
		log.Printf("error S3 put for %s:  %v", actionKey, err)
	}
	return
}

func (s *S3Cache) Close() error {
	return nil
}

func NewS3Cache(client s3Client, bucketName, prefix string, verbose bool) *S3Cache {
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
	tags := make(url.Values, 2)
	tags.Add("GOARCH", goarch)
	tags.Add("GOOS", goos)

	cache := &S3Cache{
		s3Client: client,
		bucket:   bucketName,
		prefix:   strings.Trim(prefix, "/.\\"),
		verbose:  verbose,
		tags:     tags.Encode(),
	}
	return cache
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

func (s *S3Cache) actionKey(actionID string) string {
	objPre := ""
	if len(actionID) > 3 {
		// 4096 prefixes.
		objPre = actionID[:3]
		actionID = actionID[3:]
	}
	return path.Join(s.prefix, objPre, actionID)
}
