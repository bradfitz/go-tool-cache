# go-tool-cache

Do you like Go's built-in build & test caching but wish it weren't purely stored on local disk in the `$GOCACHE` directory?

Want to share your cache over the network between your various machines, coworkers, and CI runs without all that GitHub actions/caches tarring and untarring?

Go's [GOCACHEPROG](https://pkg.go.dev/cmd/go/internal/cacheprog) lets you do that!

This was a demonstration repro for when GOCACHEPROG was still a
[proposal](https://github.com/golang/go/issues/59719). Now it just contains some misc
examples.

## Status

GOCACHEPROG shipped as an experiment in Go 1.21. It became official in Go 1.24.

## Using

First, build your cache child process. For example,

```sh
$ go install github.com/bradfitz/go-tool-cache/cmd/go-cacher@latest
```

Then tell Go to use it:

```sh
$ GOCACHEPROG=$HOME/go/bin/go-cacher go install std
```

See some stats:

```sh
$ GOCACHEPROG="$HOME/go/bin/go-cacher --verbose" go install std
Defaulting to cache dir /home/bradfitz/.cache/go-cacher ...
cacher: closing; 548 gets (0 hits, 548 misses, 0 errors); 1090 puts (0 errors)
```

Run it again and watch the hit rate go up:

```sh
$ GOCACHEPROG="$HOME/go/bin/go-cacher --verbose" go install std
Defaulting to cache dir /home/bradfitz/.cache/go-cacher ...
cacher: closing; 808 gets (808 hits, 0 misses, 0 errors); 0 puts (0 errors)
```
