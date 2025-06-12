package state

import (
	"os"

	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
)

func NewRepository(local, remote map[objects.MAC][]byte) (*repository.Repository, error) {
	logger := logging.NewLogger(os.Stdout, os.Stderr)

	ctx := kcontext.NewKContext()
	ctx.SetLogger(logger)

	return repository.Inexistent(ctx, map[string]string{
		"location": "fake+state://xyz",
	})
}
