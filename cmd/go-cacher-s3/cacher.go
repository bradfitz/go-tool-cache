// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

const defaultCacheKey = "v1"

var userCacheDir, _ = os.UserCacheDir()
var defaultLocalCacheDir = filepath.Join(userCacheDir, "go-cacher")

var (
	flagVerbose       = flag.Int("v", 0, "logging verbosity; 0=error, 1=warn, 2=info, 3=debug, 4=trace")
	flagCacheKey      = flag.String("cache-key", defaultCacheKey, "cache key")
	flagLocalCacheDir = flag.String("local-cache-dir", defaultLocalCacheDir, "local cache directory")
	bucket            string
	flagQueueLen      = flag.Int("queue-len", 0, "length of the queue for async s3 cache (0=synchronous)")
	flagWorkers       = flag.Int("workers", 1, "number of workers for async s3 cache (1=synchronous)")
)

// logHandler implements slog.Handler to print logs nicely
// mostly this was an exercise to use slog, probably not the best choice here TBH
type logHandler struct {
	Out    io.Writer
	Level  slog.Level
	attrs  []slog.Attr
	groups []string
}

func (h *logHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.Level
}

func (h *logHandler) Handle(_ context.Context, r slog.Record) error {
	s := r.Level.String()[:1]
	if len(h.groups) > 0 {
		s += " " + strings.Join(h.groups, ".") + ":"
	}
	s += " " + r.Message
	attrs := h.attrs
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	for i, a := range attrs {
		if i == 0 {
			s += " {"
		}
		s += fmt.Sprintf("%s=%q", a.Key, a.Value)
		if i < len(attrs)-1 {
			s += " "
		} else {
			s += "}"
		}
	}
	s += "\n"
	_, err := h.Out.Write([]byte(s))
	return err
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{
		Out:    h.Out,
		Level:  h.Level,
		attrs:  append(h.attrs, attrs...),
		groups: h.groups,
	}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if h.groups == nil {
		h.groups = []string{}
	}
	return &logHandler{
		Out:    h.Out,
		Level:  h.Level,
		attrs:  h.attrs,
		groups: append(h.groups, name),
	}
}

// Logf allows us to also implement AWS's logging.Logger
func (h *logHandler) Logf(cls logging.Classification, format string, args ...interface{}) {
	var l slog.Level
	switch cls {
	case logging.Debug:
		l = slog.LevelDebug
	case logging.Warn:
		l = slog.LevelWarn
	default:
		l = slog.LevelDebug
	}

	h.Handle(context.Background(), slog.Record{
		Level:   l,
		Message: fmt.Sprintf(format, args...),
	})
}

var levelTrace = slog.Level(slog.LevelDebug - 4)

func main() {
	flag.Parse()
	if len(flag.Args()) != 1 {
		log.Fatalf("usage: %s <bucket>", os.Args[0])
	}
	bucket = flag.Args()[0]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logLevel := slog.Level(*flagVerbose*-4 + 8)
	h := &logHandler{
		Level: logLevel,
		Out:   os.Stderr,
	}

	slog.SetDefault(slog.New(h))

	slog.Info(fmt.Sprintf("Log level: %s", logLevel))
	slog.Info("starting cache")
	var clientLogMode aws.ClientLogMode
	if logLevel <= levelTrace {
		clientLogMode = aws.LogRetries | aws.LogRequest
	}
	awsConfig, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(clientLogMode), config.WithLogger(h))
	if err != nil {
		log.Fatal("S3 cache disabled; failed to load AWS config: ", err)
	}
	// TODO: maybe an option to use the async s3 cache vs the sync one?
	proc := cacheproc.NewCacheProc(
		cachers.NewDiskAsyncS3Cache(
			cachers.NewSimpleDiskCache(*flagLocalCacheDir),
			s3.NewFromConfig(awsConfig),
			bucket,
			*flagCacheKey,
			*flagQueueLen,
			*flagWorkers,
		),
	)
	if err := proc.Run(ctx); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
