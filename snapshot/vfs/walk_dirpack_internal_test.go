package vfs

import (
	"errors"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

type walkCall struct {
	path string
	err  error
}

func collectPayload(t *testing.T, d *dirpackDir) []walkCall {
	t.Helper()

	var calls []walkCall
	fsc := &Filesystem{}
	err := walkDirpackPayload(d.path, fsc.dirpackDirEntries(d), map[string]struct{}{}, func(p string, e *Entry, err error) error {
		if err == nil {
			require.NotNil(t, e)
		}
		calls = append(calls, walkCall{path: p, err: err})
		return nil
	})
	require.NoError(t, err)
	return calls
}

// A directory whose object could not be fetched is reported to fn as is.
func TestWalkDirpackMissingObject(t *testing.T) {
	errMissing := errors.New("missing object")

	calls := collectPayload(t, &dirpackDir{path: "/data", err: errMissing})

	require.Len(t, calls, 1)
	require.Equal(t, "/data", calls[0].path)
	require.ErrorIs(t, calls[0].err, errMissing)
}

// A missing chunk keeps the entries decoded before it, like a streamed read,
// then reports the chunk error to fn.
func TestWalkDirpackMissingChunk(t *testing.T) {
	entry := func(name string) []byte {
		return encodeDirpackEntry(t, &Entry{
			ParentPath: "/data",
			FileInfo:   objects.FileInfo{Lname: name, Lmode: 0644},
		})
	}
	a, b, c := entry("a"), entry("b"), entry("c")

	tests := []struct {
		name   string
		first  []byte
		second []byte
		want   []string
	}{
		{
			name:   "chunk boundary",
			first:  append(append([]byte{}, a...), b...),
			second: c,
			want:   []string{"/data/a", "/data/b"},
		},
		{
			// b is cut in half: the chunk error wins over the decode one
			name:   "mid record",
			first:  append(append([]byte{}, a...), b[:len(b)/2]...),
			second: append(append([]byte{}, b[len(b)/2:]...), c...),
			want:   []string{"/data/a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errMissing := errors.New("missing chunk")
			first, second := objects.MAC{1}, objects.MAC{2}

			d := &dirpackDir{
				path: "/data",
				obj: &objects.Object{Chunks: []objects.Chunk{
					{ContentMAC: first, Length: uint32(len(tc.first))},
					{ContentMAC: second, Length: uint32(len(tc.second))},
				}},
				size: int64(len(tc.first) + len(tc.second)),
			}
			d.decode(map[objects.MAC]blobResult{
				first:  {data: tc.first},
				second: {err: errMissing},
			})

			calls := collectPayload(t, d)

			require.Len(t, calls, len(tc.want)+1)
			for i, p := range tc.want {
				require.Equal(t, p, calls[i].path)
				require.NoError(t, calls[i].err)
			}
			last := calls[len(calls)-1]
			require.Equal(t, "/data", last.path)
			require.ErrorIs(t, last.err, errMissing)
		})
	}
}
