package abs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/bradfitz/go-tool-cache/cachers"
)

func actionBlobName(actionID string) string {
	return "action/" + actionID
}

func outputBlobName(outputID string) string {
	return "output/" + outputID
}

type CacheUpstream struct {
	AccountName string
	AccountKey  string
	UseAZCLI    bool
	Endpoint    string
	Container   string

	mu           sync.Mutex
	containerURL *azblob.ContainerURL
}

var _ cachers.Upstream = (*CacheUpstream)(nil)

func (c *CacheUpstream) Init(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.containerURL != nil {
		return nil
	}

	accountKey := c.AccountKey
	if accountKey == "" {
		return errors.New("abs: missing account key")
	}

	credential, err := azblob.NewSharedKeyCredential(c.AccountName, accountKey)
	if err != nil {
		return err
	}

	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", c.AccountName)
	}
	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("cannot parse azure endpoint: %w", err)
	}

	// Build pipeline and reference to container.
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: 24 * time.Hour,
		},
	})
	containerURL := azblob.NewServiceURL(*endpointURL, pipeline).
		NewContainerURL(c.Container)
	c.containerURL = &containerURL

	return nil
}

func (c *CacheUpstream) GetAction(ctx context.Context, actionID string) (*cachers.ActionValue, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	blob := c.containerURL.NewBlobURL(actionBlobName(actionID))
	resp, err := blob.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err // TODO: convert 404 to ErrNotFound
	}
	body := resp.Body(azblob.RetryReaderOptions{})
	defer body.Close()

	var av cachers.ActionValue
	if err := json.NewDecoder(body).Decode(&av); err != nil {
		return nil, err
	}
	return &av, nil
}

func (c *CacheUpstream) GetOutput(ctx context.Context, outputID string) (body io.ReadCloser, err error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	blob := c.containerURL.NewBlobURL(outputBlobName(outputID))
	resp, err := blob.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err // TODO: convert 404 to ErrNotFound
	}
	return resp.Body(azblob.RetryReaderOptions{}), nil
}

func (c *CacheUpstream) Put(ctx context.Context, actionID string, outputID string, size int64, body io.Reader) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	if size > 0 {
		// TODO: lease blobs for writing?
		outputBlob := c.containerURL.NewBlockBlobURL(outputBlobName(outputID))
		_, err := azblob.UploadStreamToBlockBlob(ctx, body, outputBlob, azblob.UploadStreamToBlockBlobOptions{
			BlobHTTPHeaders: azblob.BlobHTTPHeaders{ContentType: "application/octet-stream"},
			BlobAccessTier:  azblob.DefaultAccessTier,
		})
		if err != nil {
			return err
		}
	}

	ac := &cachers.ActionValue{
		OutputID: outputID,
		Size:     size,
	}
	acBody := bytes.NewBuffer(nil)
	if err := json.NewEncoder(acBody).Encode(ac); err != nil {
		return err
	}
	actionBlob := c.containerURL.NewBlockBlobURL(actionBlobName(actionID))
	_, err := azblob.UploadStreamToBlockBlob(ctx, acBody, actionBlob, azblob.UploadStreamToBlockBlobOptions{
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{ContentType: "application/json"},
		BlobAccessTier:  azblob.DefaultAccessTier,
	})
	if err != nil {
		return err
	}

	return nil
}
