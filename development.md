# Development

## Go Toolchain

The `cacheprog` experiment flag is defaults to off. To enable this, we need to compile the Go toolchain via:

```
$ cd $gosrc
$ GOEXPERIMENT=cacheprog ./make.bash
```