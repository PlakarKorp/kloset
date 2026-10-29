package connectors

import (
	"io"

	"github.com/PlakarKorp/integration-sdk"
)

type LazyReader = integration.LazyReader

func NewLazyReader(open func() (io.ReadCloser, error)) *LazyReader {
	return integration.NewLazyReader(open)
}
