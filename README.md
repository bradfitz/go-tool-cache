# go-tool-cache

Like Go's built-in build/test caching but wish it weren't purely stored on local disk in the `$GOCACHE` directory?

Want to share your cache over the network between your various machines, coworkers, and CI runs without all that GitHub actions/caches tarring and untarring?

Along with a [modification to Go's `cmd/go` tool](https://go-review.googlesource.com/c/go/+/486715) ([open proposal](https://github.com/golang/go/issues/59719)), this repo lets you write
custom `GOCACHE` implementations to handle the cache however you'd like.

## Status

Currently you need to build your own Go toolchain to use this. As of 2023-04-24 it's still an open proposal & work in progress.
