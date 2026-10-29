package connectors

import (
	"io"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/kloset/objects"
)

type (
	Options = integration.Options
	Record  = integration.Record
	Result  = integration.Result
)

func NewRecord(pathname, target string, fileinfo objects.FileInfo, xattr []string, read func() (io.ReadCloser, error)) *Record {
	return integration.NewRecord(pathname, target, fileinfo, xattr, read)
}

func NewXattr(pathname, xattr string, kind objects.Attribute, read func() (io.ReadCloser, error)) *Record {
	return integration.NewXattr(pathname, xattr, kind, read)
}

func NewError(pathname string, err error) *Record {
	return integration.NewError(pathname, err)
}
