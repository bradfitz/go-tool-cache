package cachers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
)

// ActionValue is the JSON value returned by the cacher server for an GET /action request.
type ActionValue struct {
	OutputID string `json:"outputID"`
	Size     int64  `json:"size"`
}

type HTTPClient struct {
	// BaseURL is the base URL of the cacher server, like "http://localhost:31364".
	BaseURL string

	// Disk is where to write the output files to local disk, as required by the
	// cache protocol.
	Disk *DiskCache

	// HTTPClient optionally specifies the http.Client to use.
	// If nil, http.DefaultClient is used.
	HTTPClient *http.Client

	// Verbose optionally specifies whether to log verbose messages.
	Verbose bool

	// AccessToken optionally specifies a Bearer access token to include
	// in requests to the server.
	AccessToken string

	// BestEffortHTTP, when true, makes all HTTP errors non-fatal.
	// Get returns a cache miss and Put returns the local disk result,
	// silently ignoring any HTTP failures (connection errors, server errors, etc.).
	// It also makes the remote HTTP PUTs run asynchronously: Put returns as soon
	// as the blob is on local disk, and the upload happens in a background goroutine.
	BestEffortHTTP bool

	// AsyncPutTimeout, if non-zero, bounds how long a background PUT may run
	// before its context is cancelled. It only applies when BestEffortHTTP is set.
	AsyncPutTimeout time.Duration

	// AsyncPutMaxConcurrent, if positive, caps the number of background PUTs
	// running at once. New PUTs beyond the cap are queued until an existing PUT
	// finishes. If Close is called while some background PUTs are in-progress, it
	// will wait for up to AsyncPutTimeout before forcing shutdown. Disk writes
	// are not affected, and it only applies when BestEffortHTTP is set.
	AsyncPutMaxConcurrent int

	asyncOnce   sync.Once
	asyncPutSem chan struct{}
	inFlightWG  sync.WaitGroup
	baseCtx     context.Context
	baseCancel  context.CancelFunc
}

func (c *HTTPClient) ensureAsyncSetup() {
	c.asyncOnce.Do(func() {
		c.baseCtx, c.baseCancel = context.WithCancel(context.Background())
		if c.AsyncPutMaxConcurrent > 0 {
			c.asyncPutSem = make(chan struct{}, c.AsyncPutMaxConcurrent)
		}
	})
}

// asyncPutSemaphore returns the buffered channel bounding concurrent background
// PUTs, or nil if AsyncPutMaxConcurrent is unset (unbounded).
func (c *HTTPClient) asyncPutSemaphore() chan struct{} {
	c.ensureAsyncSetup()
	return c.asyncPutSem
}

// asyncPutContext returns the context shared by all background PUTs. Shutdown
// cancels it when the drain deadline elapses.
func (c *HTTPClient) asyncPutContext() context.Context {
	c.ensureAsyncSetup()
	return c.baseCtx
}

// Shutdown blocks until all background PUTs have finished or AsyncPutTimeout
// elapses, whichever comes first. If AsyncPutTimeout is zero (unbounded
// PUTs), Shutdown waits indefinitely. It reports whether all PUTs drained in
// time.
func (c *HTTPClient) Shutdown() (drained bool) {
	done := make(chan struct{})
	go func() {
		c.inFlightWG.Wait()
		close(done)
	}()
	if c.AsyncPutTimeout == 0 {
		<-done
		return true
	}
	t := time.NewTimer(c.AsyncPutTimeout)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		c.ensureAsyncSetup()
		c.baseCancel()
		return false
	}
}

func (c *HTTPClient) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// tryDrainResponse reads and throws away a small bounded amount of data from
// res.Body. This is a best-effort attempt to allow connection reuse. (Go's
// HTTP/1 Transport won't reuse a TCP connection unless you fully consume HTTP
// responses)
func tryDrainResponse(res *http.Response) {
	io.CopyN(io.Discard, res.Body, 4<<10)
}

func tryReadErrorMessage(res *http.Response) []byte {
	msg, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
	return msg
}

// responseBody returns the response body and the uncompressed content length.
// If the response has Content-Encoding: lz4, the body is wrapped with an lz4
// decompressor and the uncompressed length is read from X-Uncompressed-Length.
// For uncompressed responses, Content-Length is used directly.
func responseBody(res *http.Response) (body io.Reader, uncompressedLength int64, err error) {
	if res.Header.Get("Content-Encoding") == "lz4" {
		sizeStr := res.Header.Get("X-Uncompressed-Length")
		if sizeStr == "" {
			return nil, 0, fmt.Errorf("lz4-compressed response missing X-Uncompressed-Length header")
		}
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid X-Uncompressed-Length %q: %v", sizeStr, err)
		}
		return lz4.NewReader(res.Body), size, nil
	}
	if res.ContentLength == -1 {
		return nil, 0, fmt.Errorf("no Content-Length from server")
	}
	return res.Body, res.ContentLength, nil
}

func (c *HTTPClient) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	outputID, diskPath, err = c.Disk.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}

	req, _ := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/action/"+actionID, nil)
	if c.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AccessToken)
	}

	req.Header.Set("Accept-Encoding", "lz4")

	// Set a header to indicate we want the object and metadata in one response.
	// Prior to 2025-08-09, the protocol was two separate requests. Rather than
	// change this repo's protocol and potentially break existing clients,
	// we just add a header to indicate we want the object and then we support
	// both the old and new response types.
	req.Header.Set("Want-Object", "1") // opt in to new single roundtrip protocol

	res, err := c.httpClient().Do(req)
	if err != nil {
		if c.BestEffortHTTP {
			return "", "", nil
		}
		return "", "", err
	}
	defer res.Body.Close()
	defer tryDrainResponse(res)
	if res.StatusCode == http.StatusNotFound {
		return "", "", nil
	}
	if res.StatusCode != http.StatusOK {
		msg := tryReadErrorMessage(res)
		if c.BestEffortHTTP && res.StatusCode == http.StatusUnauthorized {
			// Known error code that will repeatedly happen, so avoid
			// filling the logs with these errors.
			//
			// 401: can happen when gocached restarts during a session, as it
			// doesn't persist access tokens.
			// TODO(tomhjp): make the client retry auth in the background.
		} else {
			log.Printf("error GET /action/%s: %v, %s", actionID, res.Status, msg)
		}
		if c.BestEffortHTTP {
			return "", "", nil
		}
		return "", "", fmt.Errorf("unexpected GET /action/%s status %v", actionID, res.Status)
	}

	switch res.Header.Get("Content-Type") {
	default:
		return "", "", fmt.Errorf("unexpected Content-Type %q from server", res.Header.Get("Content-Type"))

	case "application/octet-stream": // new single roundtrip protocol
		outputID = res.Header.Get("Go-Output-Id")
		if outputID == "" {
			return "", "", fmt.Errorf("missing Go-Output-Id header in response")
		}
		var body io.Reader
		var size int64
		body, size, err = responseBody(res)
		if err != nil {
			return "", "", err
		}
		diskPath, err = c.Disk.Put(ctx, actionID, outputID, size, body)

	case "application/json": // old two-hop protocol
		var av ActionValue
		if err := json.NewDecoder(res.Body).Decode(&av); err != nil {
			return "", "", err
		}
		outputID = av.OutputID

		// If not on disk, download it to disk.
		var putBody io.Reader
		if av.Size == 0 {
			putBody = bytes.NewReader(nil)
		} else {
			req, _ = http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/output/"+outputID, nil)
			req.Header.Set("Accept-Encoding", "lz4")
			res, err = c.httpClient().Do(req)
			if err != nil {
				if c.BestEffortHTTP {
					return "", "", nil
				}
				return "", "", err
			}
			defer res.Body.Close()
			defer tryDrainResponse(res)
			if res.StatusCode == http.StatusNotFound {
				return "", "", nil
			}
			if res.StatusCode != http.StatusOK {
				msg := tryReadErrorMessage(res)
				log.Printf("error GET /output/%s: %v, %s", outputID, res.Status, msg)
				if c.BestEffortHTTP {
					return "", "", nil
				}
				return "", "", fmt.Errorf("unexpected GET /output/%s status %v", outputID, res.Status)
			}
			putBody, _, err = responseBody(res)
			if err != nil {
				return "", "", err
			}
		}
		diskPath, err = c.Disk.Put(ctx, actionID, outputID, av.Size, putBody)
	}

	return outputID, diskPath, err
}

func (c *HTTPClient) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	// Write to disk first.
	diskPath, err := c.Disk.Put(ctx, actionID, outputID, size, body)
	if err != nil {
		log.Printf("HTTPClient.Put local disk write error: %v", err)
		return "", err
	}

	if c.BestEffortHTTP {
		sem := c.asyncPutSemaphore()
		// Background PUTs use a client-owned context, not the per-request ctx,
		// so they survive the RPC returning.
		putCtx := c.asyncPutContext()
		c.inFlightWG.Go(func() {
			// Acquire a concurrency slot before opening the file, so a queued
			// backlog doesn't hold an open file descriptor per pending upload.
			if sem != nil {
				select {
				case sem <- struct{}{}:
				case <-putCtx.Done():
					return
				}
				defer func() {
					<-sem
				}()
			}
			f, err := os.Open(diskPath)
			if err != nil {
				log.Printf("HTTPClient.Put local disk open after write error: %v", err)
				return
			}
			c.putRemote(putCtx, actionID, outputID, size, f)
		})
		return diskPath, nil
	}

	f, err := os.Open(diskPath)
	if err != nil {
		log.Printf("HTTPClient.Put local disk open after write error: %v", err)
		return "", err
	}
	if err := c.putRemote(ctx, actionID, outputID, size, f); err != nil {
		return diskPath, err
	}

	return diskPath, nil
}

func (c *HTTPClient) putRemote(ctx context.Context, actionID, outputID string, size int64, body io.ReadCloser) error {
	defer body.Close()
	if c.BestEffortHTTP && c.AsyncPutTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.AsyncPutTimeout)
		defer cancel()
	}
	// For a zero-length body, hand net/http NoBody so it sends an explicit
	// Content-Length: 0 rather than switching to chunked transfer encoding,
	// which the server rejects.
	var reqBody io.Reader = body
	if size == 0 {
		reqBody = http.NoBody
	}
	req, _ := http.NewRequestWithContext(ctx, "PUT", c.BaseURL+"/"+actionID+"/"+outputID, reqBody)
	req.ContentLength = size
	if c.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AccessToken)
	}

	res, err := c.httpClient().Do(req)
	if err != nil {
		log.Printf("error PUT /%s/%s: %v", actionID, outputID, err)
		return err
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		errMsg := fmt.Sprintf("error PUT /%s/%s: %s, %s", actionID, outputID, res.Status, tryReadErrorMessage(res))
		if c.BestEffortHTTP && (res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden) {
			// Known error codes that will repeatedly happen, so avoid
			// filling the logs with these errors.
			//
			// 401: can happen when gocached restarts during a session, as it
			// doesn't persist access tokens.
			// TODO(tomhjp): make the client retry auth in the background.
			//
			// 403: can happen when authed with a JWT that didn't grant global
			// write permissions.
			// TODO(tomhjp): support namespaces so all sessions can safely write.
		} else {
			log.Print(errMsg)
		}
		return errors.New(errMsg)
	}

	return nil
}
