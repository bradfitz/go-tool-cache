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

func (c *HTTPClient) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	outputID, diskPath, err = c.Disk.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}

	req, _ := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/action/"+actionID, nil)

	res, err := c.httpClient().Do(req)
	if err != nil {
		return "", "", err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return "", "", nil
	}
	if res.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("unexpected GET /action/%s status %v", actionID, res.Status)
	}
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
		if res.StatusCode == http.StatusNotFound {
			return "", "", nil
		}
		if res.StatusCode != http.StatusOK {
			return "", "", fmt.Errorf("unexpected GET /output/%s status %v", outputID, res.Status)
		}
		if res.ContentLength == -1 {
			return "", "", fmt.Errorf("no Content-Length from server")
		}
		putBody = res.Body
	}
	diskPath, err = c.Disk.Put(ctx, actionID, outputID, av.Size, putBody)
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
		all, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
		return "", fmt.Errorf("unexpected PUT /%s/%s status %v: %s", actionID, outputID, res.Status, all)
	}
	v := <-diskPutCh
	if err, ok := v.(error); ok {
		log.Printf("HTTPClient.Put local disk error: %v", err)
		return "", err
	}
	return v.(string), nil
}
