package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

// Size of uint64 is 8 bytes
// lenWidth is the number of bytes used to store length of each appended record
const lenWidth = 8

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Appends slice of bytes to the store
// Returns:
// n - total number of bytes written
// pos - starting position from where append started (used to create index)
// err - error
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	// Write the length of p in buffer
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// w - total number of bytes written
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// Adding 8 bytes to w because we've written length of p i.e. uint64 to the buffer
	w += lenWidth
	// Increment the total size of the store
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Whenever we append, we write the length of content we're appending
// Then use it to read the content
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)

	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// Read data into p from the offset off till the size of p
// Returns number of bytes read
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
