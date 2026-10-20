# Scanner image for https://github.com/PlakarKorp/kloset (main), built by Anthropic's OSS Scanner
# (https://github.com/anthropics/oss-scanner, enrolled as projects/kloset, whose project.yaml points here).
#
# kloset is a library: there is no binary to build. The audit runs with no network, so this build fetches
# every Go module, compiles every package and its tests (with and without the race detector), and runs
# the test suite once so the log shows the image works.
# threat_model.md, next to this file, says how to drive the engine from tests and the mocks in testing/.

FROM golang:1.26-bookworm
ENV DEBIAN_FRONTEND=noninteractive

# scanner contract: the checkout lives inside the image, at /src
COPY . /src
WORKDIR /src

# Every module the library and its tests depend on.
RUN go mod download

# Every package and its tests, as CI builds them. A step that fails fails the build, and primary_contact
# gets the log.
RUN go build ./... \
    && go test -run '^$' ./... \
    && go test -race -run '^$' ./...

# The test suite and vet, as `make test` runs them. A failure must not abort the build.
RUN (timeout 1200 go test -timeout 2m ./... || echo "WARNING: go test reported failures") \
    && (go vet ./... || echo "WARNING: go vet reported problems")

# Everything is fetched. From here on the go command must not try to reach a proxy or download a toolchain.
ENV GOPROXY=off \
    GOTOOLCHAIN=local
