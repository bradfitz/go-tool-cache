package cachers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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

func (c *HTTPClient) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	outputID, diskPath, err = c.Disk.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}

	req, _ := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/action/"+actionID, nil)

	// Set a header to indicate we want the object and metadata in one response.
	// Prior to 2025-08-09, the protocol was two separate requests. Rather than
	// change this repo's protocol and potentially break existing clients,
	// we just add a header to indicate we want the object and then we support
	// both the old and new response types.
	req.Header.Set("Want-Object", "1") // opt in to new single roundtrip protocol

	res, err := c.httpClient().Do(req)
	if err != nil {
		return "", "", err
	}
	defer res.Body.Close()
	defer tryDrainResponse(res)
	if res.StatusCode == http.StatusNotFound {
		return "", "", nil
	}
	if res.StatusCode != http.StatusOK {
		msg := tryReadErrorMessage(res)
		log.Printf("error GET /action/%s: %v, %s", actionID, res.Status, msg)
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
		if res.ContentLength == -1 {
			return "", "", fmt.Errorf("no Content-Length from server")
		}
		diskPath, err = c.Disk.Put(ctx, actionID, outputID, res.ContentLength, res.Body)

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
			res, err = c.httpClient().Do(req)
			if err != nil {
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
				return "", "", fmt.Errorf("unexpected GET /output/%s status %v", outputID, res.Status)
			}
			if res.ContentLength == -1 {
				return "", "", fmt.Errorf("no Content-Length from server")
			}
			putBody = res.Body
		}
		diskPath, err = c.Disk.Put(ctx, actionID, outputID, av.Size, putBody)
	}

	return outputID, diskPath, err
}

func (c *HTTPClient) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	// Write to disk locally as we write it remotely, as we need to guarantee
	// it's on disk locally for the caller.
	pr, pw := io.Pipe()
	diskPutCh := make(chan any, 1)
	go func() {
		var putBody io.Reader = pr
		if size == 0 {
			putBody = bytes.NewReader(nil)
		}
		diskPath, err := c.Disk.Put(ctx, actionID, outputID, size, putBody)
		if err != nil {
			diskPutCh <- err
		} else {
			diskPutCh <- diskPath
		}
	}()

	var putBody io.Reader
	if size == 0 {
		// Special case the empty file so NewRequest sets "Content-Length: 0",
		// as opposed to thinking we didn't set it and not being able to sniff its size
		// from the type.
		putBody = bytes.NewReader(nil)
	} else {
		putBody = io.TeeReader(body, pw)
	}
	req, _ := http.NewRequestWithContext(ctx, "PUT", c.BaseURL+"/"+actionID+"/"+outputID, putBody)
	req.ContentLength = size
	res, err := c.httpClient().Do(req)
	pw.Close()
	if err != nil {
		log.Printf("error PUT /%s/%s: %v", actionID, outputID, err)
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		msg := tryReadErrorMessage(res)
		log.Printf("error PUT /%s/%s: %v, %s", actionID, outputID, res.Status, msg)
		return "", fmt.Errorf("unexpected PUT /%s/%s status %v", actionID, outputID, res.Status)
	}
	v := <-diskPutCh
	if err, ok := v.(error); ok {
		log.Printf("HTTPClient.Put local disk error: %v", err)
		return "", err
	}
	return v.(string), nil
}
