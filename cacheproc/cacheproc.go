// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cacheproc implements the mechanics of talking to cmd/go's GOCACHE protocol
// so you can write a caching child process at a higher level.
package cacheproc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/bradfitz/go-tool-cache/cachers"
	"golang.org/x/sync/errgroup"

	"github.com/bradfitz/go-tool-cache/wire"
)

type cacherCtxKey string

var (
	ErrUnknownCommand = errors.New("unknown command")
	ErrNoOutputID     = errors.New("no outputID")
	requestIDKey      = cacherCtxKey("requestID")
)

// Process implements the cmd/go JSON protocol over stdin & stdout via three
// funcs that callers can optionally implement.
type Process struct {
	cache    cachers.LocalCache
	closer   sync.Once
	errClose error
}

func NewCacheProc(cache cachers.LocalCache) *Process {
	return &Process{
		cache: cache,
	}
}

func (p *Process) Run(ctx context.Context) error {
	br := bufio.NewReader(os.Stdin)
	jd := json.NewDecoder(br)

	bw := bufio.NewWriter(os.Stdout)
	je := json.NewEncoder(bw)
	caps := []wire.Cmd{"get", "put", "close"}
	if err := je.Encode(&wire.Response{KnownCommands: caps}); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}

	var wmu sync.Mutex // guards writing responses

	wg, ctx := errgroup.WithContext(ctx)
	if err := p.cache.Start(ctx); err != nil {
		return err
	}
	defer func() {
		_ = p.close()
		_ = wg.Wait()
	}()
	for {
		var req wire.Request
		if err := jd.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if req.Command == wire.CmdPut && req.BodySize > 0 {
			// TODO(bradfitz): stream this and pass a checksum-validating
			// io.Reader that validates on EOF.
			var bodyb []byte
			if err := jd.Decode(&bodyb); err != nil {
				log.Fatal(err)
			}
			if int64(len(bodyb)) != req.BodySize {
				log.Fatalf("only got %d bytes of declared %d", len(bodyb), req.BodySize)
			}
			req.Body = bytes.NewReader(bodyb)
		}
		wg.Go(func() error {
			res := &wire.Response{ID: req.ID}
			ctx := context.WithValue(ctx, requestIDKey, &req)
			if err := p.handleRequest(ctx, &req, res); err != nil {
				res.Err = err.Error()
			}
			wmu.Lock()
			defer wmu.Unlock()
			_ = je.Encode(res)
			_ = bw.Flush()
			return nil
		})
	}
}

func (p *Process) handleRequest(ctx context.Context, req *wire.Request, res *wire.Response) error {
	switch req.Command {
	default:
		return ErrUnknownCommand
	case "close":
		return p.close()
	case "get":
		return p.handleGet(ctx, req, res)
	case "put":
		return p.handlePut(ctx, req, res)
	}
}

func (p *Process) handleGet(ctx context.Context, req *wire.Request, res *wire.Response) (retErr error) {
	outputID, diskPath, err := p.cache.Get(ctx, fmt.Sprintf("%x", req.ActionID))
	if err != nil {
		return err
	}
	if outputID == "" && diskPath == "" {
		res.Miss = true
		return nil
	}
	if outputID == "" {
		return ErrNoOutputID
	}
	res.OutputID, err = hex.DecodeString(outputID)
	if err != nil {
		return fmt.Errorf("invalid OutputID: %w", err)
	}
	fi, err := os.Stat(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			res.Miss = true
			return nil
		}
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}
	res.Size = fi.Size()
	res.TimeNanos = fi.ModTime().UnixNano()
	res.DiskPath = diskPath
	return nil
}

func (p *Process) handlePut(ctx context.Context, req *wire.Request, res *wire.Response) (retErr error) {
	actionID, objectID := fmt.Sprintf("%x", req.ActionID), fmt.Sprintf("%x", req.ObjectID)
	defer func() {
		if retErr != nil {
			log.Printf("put(action %s, obj %s, %v bytes): %v", actionID, objectID, req.BodySize, retErr)
		}
	}()
	var body = req.Body
	if body == nil {
		body = bytes.NewReader(nil)
	}
	diskPath, err := p.cache.Put(ctx, actionID, objectID, req.BodySize, body)
	if err != nil {
		return err
	}
	fi, err := os.Stat(diskPath)
	if err != nil {
		return fmt.Errorf("stat after successful Put: %w", err)
	}
	if fi.Size() != req.BodySize {
		return fmt.Errorf("failed to write file to disk with right size: disk=%v; wanted=%v", fi.Size(), req.BodySize)
	}
	res.DiskPath = diskPath
	return nil
}

func (p *Process) close() error {
	p.closer.Do(func() {
		p.errClose = p.cache.Close()
		if p.errClose != nil {
			log.Printf("cache stop failed: %v", p.errClose)
		}
	})
	return p.errClose
}
