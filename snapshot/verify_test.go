package snapshot_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/encryption/keypair"
	"github.com/stretchr/testify/require"
)

// will be fixed when signed snapshots are back post beta
func _TestVerify(t *testing.T) {
	defaultKeyPair, err := keypair.Generate()
	require.NoError(t, err)
	require.NotNil(t, defaultKeyPair)

	_, snap := generateSnapshot(t) // TODO: sign with defaultKeyPair
	defer snap.Close()

	verified, err := snap.Verify()
	require.NoError(t, err)
	require.True(t, verified)
}
