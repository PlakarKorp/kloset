package repository_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

func TestLock(t *testing.T) {
	hostname := "test-host"

	t.Run("NewExclusiveLock", func(t *testing.T) {
		lock := repository.NewExclusiveLock(hostname)
		require.NotNil(t, lock)
		require.Equal(t, hostname, lock.Hostname)
		require.True(t, lock.Exclusive)
		require.WithinDuration(t, time.Now(), lock.Timestamp, time.Second)
		require.Equal(t, versioning.FromString(repository.LOCK_VERSION), lock.Version)
	})

	t.Run("NewSharedLock", func(t *testing.T) {
		lock := repository.NewSharedLock(hostname)
		require.NotNil(t, lock)
		require.Equal(t, hostname, lock.Hostname)
		require.False(t, lock.Exclusive)
		require.WithinDuration(t, time.Now(), lock.Timestamp, time.Second)
		require.Equal(t, versioning.FromString(repository.LOCK_VERSION), lock.Version)
	})

	t.Run("IsStale", func(t *testing.T) {
		lock := repository.NewExclusiveLock(hostname)
		require.False(t, lock.IsStale())

		// Make the lock stale by setting its timestamp to be older than LOCK_TTL
		lock.Timestamp = time.Now().Add(-repository.LOCK_TTL - time.Minute)
		require.True(t, lock.IsStale())
	})

	t.Run("SerializeAndDeserialize", func(t *testing.T) {
		// Create a lock and serialize it
		originalLock := repository.NewExclusiveLock(hostname)
		var buf bytes.Buffer
		err := originalLock.SerializeToStream(&buf)
		require.NoError(t, err)

		// Deserialize the lock
		deserializedLock, err := repository.NewLockFromStream(originalLock.Version, &buf)
		require.NoError(t, err)
		require.NotNil(t, deserializedLock)

		// Compare the original and deserialized locks
		require.Equal(t, originalLock.Version, deserializedLock.Version)
		require.Equal(t, originalLock.Hostname, deserializedLock.Hostname)
		require.Equal(t, originalLock.Exclusive, deserializedLock.Exclusive)
		require.WithinDuration(t, originalLock.Timestamp, deserializedLock.Timestamp, time.Second)
	})

	t.Run("DeserializeInvalidData", func(t *testing.T) {
		invalidData := []byte("invalid data")
		_, err := repository.NewLockFromStream(versioning.FromString(repository.LOCK_VERSION), bytes.NewReader(invalidData))
		require.Error(t, err)
	})

	t.Run("SerializeToInvalidWriter", func(t *testing.T) {
		lock := repository.NewExclusiveLock(hostname)
		invalidWriter := &invalidWriter{}
		err := lock.SerializeToStream(invalidWriter)
		require.Error(t, err)
	})
}

// invalidWriter is a writer that always returns an error
type invalidWriter struct{}

func (w *invalidWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrClosedPipe
}
