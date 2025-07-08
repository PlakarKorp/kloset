package reading

import (
	"io"
	"testing"
)

type TestReader struct {
	Size   int
	Closed bool
}

func (rd *TestReader) Read(buf []byte) (int, error) {
	if rd.Closed {
		return 0, io.ErrClosedPipe
	}

	n := min(rd.Size, int(len(buf)))
	rd.Size -= n
	for i := range n {
		buf[i] = 'x'
	}

	if rd.Size == 0 {
		return n, io.EOF
	}

	return n, nil
}

func (rd *TestReader) Close() error {
	if rd.Closed {
		return io.ErrClosedPipe
	}
	rd.Closed = true
	return nil
}

func TestClosingReaderReadAll(t *testing.T) {
	N := 100
	r := &TestReader{Size: N}
	rd := ClosingReader(r)
	data, err := io.ReadAll(rd)
	if err != nil {
		t.Errorf("ReadAll failed: %v", err)
	}
	if len(data) != N {
		t.Errorf("wrong data len: %v / %v", len(data), N)
	}
	if !r.Closed {
		t.Errorf("reader not closed")
	}
}

func TestClosingLimitedReaderReadAll(t *testing.T) {
	N := 100
	r := &TestReader{Size: N}
	// The reader has less data than the limit
	rd := ClosingLimitedReader(r, 200)

	data, err := io.ReadAll(rd)
	if err != nil {
		t.Errorf("ReadAll failed: %v", err)
	}
	if len(data) != N {
		t.Errorf("wrong data len: %v / %v", len(data), N)
	}
	if !r.Closed {
		t.Errorf("reader not closed")
	}
}

func TestClosingLimitedReaderReadAll2(t *testing.T) {
	N := 100
	EXP := 30
	r := &TestReader{Size: N}
	// The reader has more data than the limit
	rd := ClosingLimitedReader(r, int64(EXP))

	data, err := io.ReadAll(rd)
	if err != nil {
		t.Errorf("ReadAll failed: %v", err)
	}
	if len(data) != EXP {
		t.Errorf("wrong data len: %v / %v", len(data), EXP)
	}
	if r.Size != N-EXP {
		t.Errorf("wrong remaining data len: %v / %v", r.Size, N-EXP)
	}
	if !r.Closed {
		t.Errorf("reader not closed")
	}
}

func TestClosingReaderReadFull(t *testing.T) {
	N := 100
	r := &TestReader{Size: N}
	rd := ClosingReader(r)

	// Buffer has the same size as the reader
	buf := make([]byte, N)
	n, err := io.ReadFull(rd, buf)
	if err != nil {
		t.Errorf("ReadFull failed: %v", err)
	}
	if n != N {
		t.Errorf("wrong data len: %v / %v", n, N)
	}
	if !r.Closed {
		t.Errorf("reader not closed")
	}
}

func TestClosingLimitedReaderReadFull(t *testing.T) {
	N := 100
	r := &TestReader{Size: N}
	rd := ClosingLimitedReader(r, 200)

	// Buffer has the same size as the reader
	buf := make([]byte, N)
	n, err := io.ReadFull(rd, buf)
	if err != nil {
		t.Errorf("ReadFull failed: %v", err)
	}
	if n != N {
		t.Errorf("wrong data len: %v / %v", n, N)
	}
	if !r.Closed {
		t.Errorf("reader not closed")
	}
}

func TestClosingLimitedReaderReadFull2(t *testing.T) {
	N := 100
	EXP := 30
	r := &TestReader{Size: N}
	rd := ClosingLimitedReader(r, int64(EXP))

	// Limit is smaller than the target buffer
	buf := make([]byte, N)
	n, err := io.ReadFull(rd, buf)
	if err != nil {
		if err != io.ErrUnexpectedEOF {
			t.Errorf("ReadFull failed: %v", err)
		}
	}
	if n != EXP {
		t.Errorf("wrong data len: %v / %v", n, EXP)
	}
	if !r.Closed {
		t.Errorf("reader not closed")
	}
	if r.Size != N-EXP {
		t.Errorf("wrong remaining data len: %v / %v", r.Size, N-EXP)
	}
	if !r.Closed {
		t.Errorf("reader not closed")
	}
}

func TestClosingLimitedReaderReadFull3(t *testing.T) {
	N := 100
	EXP := 30
	r := &TestReader{Size: N}
	rd := ClosingLimitedReader(r, int64(EXP))

	// Limit is larger than the target buffer
	buf := make([]byte, 20)
	n, err := io.ReadFull(rd, buf)
	if err != nil {
		if err != io.ErrUnexpectedEOF {
			t.Errorf("ReadFull failed: %v", err)
		}
	}
	if n != 20 {
		t.Errorf("wrong data len: %v / %v", n, 20)
	}
	if r.Size != N-n {
		t.Errorf("wrong remaining data len: %v / %v", r.Size, N-n)
	}
	if r.Closed {
		t.Errorf("reader closed")
	}
}
