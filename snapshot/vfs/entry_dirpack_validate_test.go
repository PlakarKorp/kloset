package vfs

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// encodeDirpackEntry frames an entry the way a dirpack object stores it:
// a type byte, a size, the msgpack body, then the entry MAC.
func encodeDirpackEntry(t *testing.T, e *Entry) []byte {
	t.Helper()

	body, err := msgpack.Marshal(e)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint8(0)))
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(len(body)+len(e.MAC))))
	buf.Write(body)
	buf.Write(e.MAC[:])
	return buf.Bytes()
}

// decodeDirpackStream mirrors the decode loop in getdentsDirpack, including its
// validation, so entry rejection can be exercised without standing up a whole
// repository.  It must stay in sync with getdentsDirpack.
func decodeDirpackStream(t *testing.T, prefix string, raw []byte) ([]*Entry, error) {
	t.Helper()

	rd := bytes.NewReader(raw)
	var out []*Entry
	for rd.Len() > 0 {
		_, siz, err := readDirPackHdr(rd)
		if err != nil {
			return out, err
		}

		var entry Entry
		lrd := &lentil{rd: rd, n: int64(siz - uint32(len(entry.MAC)))}
		if err := msgpack.NewDecoder(lrd).Decode(&entry); err != nil {
			return out, err
		}
		if _, err := rd.Read(entry.MAC[:]); err != nil {
			return out, err
		}

		if err := entry.validate(); err != nil {
			return out, err
		}
		if entry.ParentPath != prefix {
			return out, ErrMalformedEntry
		}
		out = append(out, &entry)
	}
	return out, nil
}

type lentil struct {
	rd *bytes.Reader
	n  int64
}

func (l *lentil) Read(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, nil
	}
	if int64(len(p)) > l.n {
		p = p[:l.n]
	}
	n, err := l.rd.Read(p)
	l.n -= int64(n)
	return n, err
}

// A dirpack for the root directory that lists a symlink and then a file whose
// parent is that symlink: the second entry must be rejected, since it is not a
// direct child of the directory whose listing it appears in.
func TestDirpackRejectsMismatchedParent(t *testing.T) {
	symlink := &Entry{
		ParentPath: "/",
		FileInfo:   objects.FileInfo{Lname: "link", Lmode: 0777},
	}
	forged := &Entry{
		ParentPath: "/link",
		FileInfo:   objects.FileInfo{Lname: "child", Lmode: 0644},
	}

	raw := append(encodeDirpackEntry(t, symlink), encodeDirpackEntry(t, forged)...)

	entries, err := decodeDirpackStream(t, "/", raw)
	require.ErrorIs(t, err, ErrMalformedEntry,
		"an entry claiming a parent other than the dirpack's directory must be rejected")
	require.Len(t, entries, 1, "the legitimate symlink entry is still returned")
	require.Equal(t, "/link", entries[0].Path())
}

// A name carrying a separator is rejected before it can be joined into a path.
func TestDirpackRejectsSeparatorInName(t *testing.T) {
	forged := &Entry{
		ParentPath: "/",
		FileInfo:   objects.FileInfo{Lname: "link/child", Lmode: 0644},
	}

	_, err := decodeDirpackStream(t, "/", encodeDirpackEntry(t, forged))
	require.ErrorIs(t, err, ErrMalformedEntry)
}

// A well-formed listing decodes cleanly.
func TestDirpackAcceptsWellFormedListing(t *testing.T) {
	var raw []byte
	for _, name := range []string{"a.txt", "b.txt", "subdir"} {
		raw = append(raw, encodeDirpackEntry(t, &Entry{
			ParentPath: "/data",
			FileInfo:   objects.FileInfo{Lname: name, Lmode: 0644},
		})...)
	}

	entries, err := decodeDirpackStream(t, "/data", raw)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, "/data/a.txt", entries[0].Path())
	require.Equal(t, "/data/subdir", entries[2].Path())
}
