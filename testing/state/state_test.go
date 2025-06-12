package state

import (
	"bytes"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository/state"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestRebuildNoStates(t *testing.T) {
	repo, err := NewRepository(nil, nil)
	require.NoError(t, err)

	cache := newCache()

	err = repo.RebuildState(cache)
	require.NoError(t, err)

	require.Equal(t, 0, len(cache.states))
}

func TestRebuildWithRemoteStates(t *testing.T) {
	repo, err := NewRepository(nil, nil)
	require.NoError(t, err)

	cache := newCache()

	for i := range 5 {
		meta := state.Metadata{
			Version:   0,
			Timestamp: time.Unix(int64(i), 0),
			Serial:    uuid.UUID([]byte("a1da730f9c58f9af0e248ca3684ad43e")),
		}
		b, err := meta.ToBytes()
		require.NoError(t, err)

		data := []byte{byte(state.ET_METADATA)}
		data = append(data, b...)

		err = repo.PutState(objects.MAC{byte(i)}, bytes.NewReader(data))
		require.NoError(t, err)
	}

	err = repo.RebuildState(cache)
	require.NoError(t, err)

	require.Equal(t, 5, len(cache.states))
}

func TestRebuildWithLocalState(t *testing.T) {
	repo, err := NewRepository(nil, nil)
	require.NoError(t, err)

	cache := newCache()

	for i := range 3 {
		cache.states[objects.MAC{byte(i)}] = nil
	}

	for i := range 5 {
		meta := state.Metadata{
			Version:   0,
			Timestamp: time.Unix(int64(i), 0),
			Serial:    uuid.UUID([]byte("a1da730f9c58f9af0e248ca3684ad43e")),
		}
		b, err := meta.ToBytes()
		require.NoError(t, err)

		data := []byte{byte(state.ET_METADATA)}
		data = append(data, b...)

		err = repo.PutState(objects.MAC{byte(i)}, bytes.NewReader(data))
		require.NoError(t, err)
	}

	err = repo.RebuildState(cache)
	require.NoError(t, err)

	require.Equal(t, 5, len(cache.states))
}

func TestRebuildNewLayout(t *testing.T) {
	type Metadata2 struct {
		Foo       int                `msgpack:"foo"`
		Version   versioning.Version `msgpack:"version"`
		Timestamp time.Time          `msgpack:"timestamp"`
		Serial    uuid.UUID          `msgpack:"serial"`
	}

	repo, err := NewRepository(nil, nil)
	require.NoError(t, err)

	cache := newCache()

	for i := range 3 {
		cache.states[objects.MAC{byte(i)}] = nil
	}

	for i := range 5 {
		meta := Metadata2{
			Version:   0,
			Timestamp: time.Unix(int64(i), 0),
			Serial:    uuid.UUID([]byte("a1da730f9c58f9af0e248ca3684ad43e")),
		}
		b, err := msgpack.Marshal(&meta)
		require.NoError(t, err)

		oldmeta := state.Metadata{}
		err = msgpack.Unmarshal(b, &oldmeta)
		require.NoError(t, err)
		require.Equal(t, meta.Timestamp, oldmeta.Timestamp)

		data := []byte{byte(state.ET_METADATA)}
		data = append(data, b...)

		err = repo.PutState(objects.MAC{byte(i)}, bytes.NewReader(data))
		require.NoError(t, err)
	}

	err = repo.RebuildState(cache)
	require.NoError(t, err)

	require.Equal(t, 5, len(cache.states))
}
