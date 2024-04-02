FROM golang:1.22 as golang-cacheprog

# copy out of /usr/local/go; can't use GOROOT
# TODO: there's probably something we can do with GOTOOLCHAIN, but I don't understand it well enough
RUN cp -r /usr/local/go /gocacheprog
RUN cd /gocacheprog/src && GOEXPERIMENT=cacheprog ./make.bash
RUN cp -r /gocacheprog/* /usr/local/go/
RUN go version | grep cacheprog
RUN rm -rf /gocacheprog

FROM golang-cacheprog as go-cacher-s3
ADD . /workdir
# need go.work because we're a fork
RUN cd /workdir && go work init && go work use . && go install ./cmd/go-cacher-s3

# TODO: not sure this affects the EXEC
ENV GOCACHEPROG="go-cacher-s3 --verbose"