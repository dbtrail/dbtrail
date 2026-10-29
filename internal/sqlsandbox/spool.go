package sqlsandbox

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// spoolFile is the name of a spooled result inside its directory.
const spoolFile = "rows.ndjson"

// Spool is a result too large to hold in memory (Limits.SpoolMaxBytes): its
// rows in a file, one JSON array per line, read as the client takes them.
// The parent opens the file before handing the Spool over, so the directory
// being removed meanwhile (a sweep of stale results, a cleanup) does not cut
// a read under way: an open file stays readable once unlinked. Close closes
// it and removes the directory; it is the holder's to call, once it has read
// what it needs or given up.
type Spool struct {
	mu      sync.Mutex
	f       *os.File
	dir     string
	rows    int64
	closed  bool
	release func()
}

// openSpool opens the result the worker wrote in dir, rows rows long;
// release gives back its share of the disk (Runner.reserveSpool) on Close.
func openSpool(dir string, rows int64, release func()) (*Spool, error) {
	f, err := os.Open(filepath.Join(dir, spoolFile))
	if err != nil {
		return nil, fmt.Errorf("open the result file: %w", err)
	}
	if release == nil {
		release = func() {}
	}
	return &Spool{f: f, dir: dir, rows: rows, release: release}, nil
}

// Rows is how many rows the spool holds.
func (s *Spool) Rows() int64 { return s.rows }

// errSpoolClosed is Each's error on a Spool already closed.
var errSpoolClosed = errors.New("the result file was already closed")

// Each hands fn every row, in order, each cell as Result.Rows would hold it
// (numbers as json.Number). An error from fn stops it and is returned. It
// reads from the start each time it is called, and holds no lock while fn
// runs: fn may Close the Spool (a client that left), after which the read
// stops with an error. A file that holds fewer rows than the worker said is
// an error, never a short answer taken as whole.
func (s *Spool) Each(fn func(row []any) error) error {
	s.mu.Lock()
	closed, f := s.closed, s.f
	s.mu.Unlock()
	if closed {
		return errSpoolClosed
	}
	// ReadAt, not the file's own offset: two reads never share a position.
	dec := json.NewDecoder(bufio.NewReaderSize(io.NewSectionReader(f, 0, 1<<62), 1<<20))
	dec.UseNumber()
	var n int64
	for {
		var row []any
		err := dec.Decode(&row)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read the result file at row %d: %w", n+1, err)
		}
		n++
		if err := fn(row); err != nil {
			return err
		}
	}
	if n != s.rows {
		return fmt.Errorf("the result file holds %d rows, the worker wrote %d", n, s.rows)
	}
	return nil
}

// Close closes the file and removes its directory. Safe to call more than
// once.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	err := s.f.Close()
	if rerr := os.RemoveAll(s.dir); rerr != nil && err == nil {
		err = rerr
	}
	s.release()
	return err
}

// spoolWriter is the worker's side: the rows past MaxResultBytes, written
// as they come, up to max bytes.
type spoolWriter struct {
	f     *os.File
	w     *bufio.Writer
	bytes int64
	max   int64
	rows  int64
}

func createSpool(dir string, max int64) (*spoolWriter, error) {
	f, err := os.OpenFile(filepath.Join(dir, spoolFile), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &spoolWriter{f: f, w: bufio.NewWriterSize(f, 1<<20), max: max}, nil
}

// write appends one row, already encoded; false when the file would pass
// its cap.
func (s *spoolWriter) write(enc []byte) (bool, error) {
	s.bytes += int64(len(enc)) + 1
	if s.bytes > s.max {
		return false, nil
	}
	if _, err := s.w.Write(enc); err != nil {
		return false, err
	}
	if err := s.w.WriteByte('\n'); err != nil {
		return false, err
	}
	s.rows++
	return true, nil
}

// finish flushes and closes the file, which the parent then opens.
func (s *spoolWriter) finish() error {
	err := s.w.Flush()
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// abandon closes the file; the parent removes its directory.
func (s *spoolWriter) abandon() { _ = s.f.Close() }
