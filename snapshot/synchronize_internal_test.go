package snapshot

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSyncImporterPingClose covers the interface-satisfying Ping and Close
// methods on syncImporter.  Synchronize itself does not call them, so they
// stay at 0% from the public-API tests.
func TestSyncImporterPingClose(t *testing.T) {
	imp := &syncImporter{}
	ctx := context.Background()
	require.NoError(t, imp.Ping(ctx))
	require.NoError(t, imp.Close(ctx))
}
