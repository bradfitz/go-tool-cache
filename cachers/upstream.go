package cachers

import (
	"bytes"
	"context"
	"io"
)

type WithUpstream struct {
	Upstream Upstream
	Local    Cache // usually a disk cache
}

var _ Cache = (*WithUpstream)(nil)

func (wu *WithUpstream) Get(
	ctx context.Context,
	actionID string,
) (outputID string, diskPath string, err error) {
	outputID, diskPath, err = wu.Local.Get(ctx, actionID)
	if err == nil && outputID != "" { // found in local disk
		return outputID, diskPath, nil
	}

	// If not on disk, download it to disk.
	av, err := wu.Upstream.GetAction(ctx, actionID)
	if err != nil {
		return "", "", IgnoreNotFound(err)
	}
	outputID = av.OutputID

	var outputBody io.Reader
	if av.Size == 0 {
		outputBody = bytes.NewReader(nil)
	} else {
		b, err := wu.Upstream.GetOutput(ctx, outputID)
		if err != nil {
			return "", "", IgnoreNotFound(err)
		}
		defer b.Close()
		outputBody = b
	}

	diskPath, err = wu.Local.Put(ctx, actionID, outputID, av.Size, outputBody)
	return outputID, diskPath, err
}

func (wu *WithUpstream) Put(
	ctx context.Context,
	actionID string,
	outputID string,
	size int64,
	body io.Reader,
) (diskPath string, err error) {
	// Write to disk locally as we write it remotely, as we need to guarantee
	// it's on disk locally for the caller.
	pr, pw := io.Pipe()
	diskPutCh := make(chan any, 1)
	go func() {
		var putBody io.Reader = pr
		if size == 0 {
			putBody = bytes.NewReader(nil)
		}
		diskPath, err := wu.Local.Put(ctx, actionID, outputID, size, putBody)
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

	err = wu.Upstream.Put(ctx, actionID, outputID, size, putBody)
	pw.Close() // close write
	if err != nil {
		return "", err
	}

	// wait for disk to finish writing
	v := <-diskPutCh
	if err, ok := v.(error); ok {
		return "", err
	}
	diskPath = v.(string)
	return diskPath, nil
}
