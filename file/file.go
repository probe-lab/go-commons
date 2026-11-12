// Package file writes files so that a reader sees the old file or the
// complete new one, never a partial file.
package file

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// PartSuffix ends the name of the temporary file that holds the content
// until it is complete.
const PartSuffix = ".part"

// Atomic is a file that appears at its path only when Commit succeeds. Until
// then the content goes to the path with PartSuffix added.
type Atomic struct {
	path   string
	part   *os.File
	closed bool
}

// CreateAtomic creates the temporary file for path, and the directories
// above it.
func CreateAtomic(path string) (*Atomic, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	part, err := os.Create(path + PartSuffix)
	if err != nil {
		return nil, err
	}

	return &Atomic{path: path, part: part}, nil
}

// Write writes to the temporary file.
func (a *Atomic) Write(p []byte) (int, error) {
	return a.part.Write(p)
}

// Commit syncs the temporary file to disk, closes it and renames it to the
// path, which replaces any file there. When Commit fails, it removes the
// temporary file and leaves the path as it was.
func (a *Atomic) Commit() error {
	if a.closed {
		return fmt.Errorf("commit %s: file already closed", a.path)
	}
	a.closed = true

	err := a.part.Sync()
	if cerr := a.part.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(a.part.Name(), a.path)
	}
	if err != nil {
		return errors.Join(fmt.Errorf("commit %s: %w", a.path, err), removePart(a.part.Name()))
	}

	return nil
}

// Abort closes and removes the temporary file and leaves the path as it was.
// After Commit or an earlier Abort it does nothing.
func (a *Atomic) Abort() error {
	if a.closed {
		return nil
	}
	a.closed = true

	return errors.Join(a.part.Close(), removePart(a.part.Name()))
}

// WriteAtomic writes a file through write, buffered, and moves it into place
// only when write succeeds; see Atomic.
func WriteAtomic(path string, write func(w io.Writer) error) error {
	a, err := CreateAtomic(path)
	if err != nil {
		return err
	}

	bw := bufio.NewWriterSize(a, 1<<20)
	if err := write(bw); err != nil {
		return errors.Join(err, a.Abort())
	}
	if err := bw.Flush(); err != nil {
		return errors.Join(err, a.Abort())
	}

	return a.Commit()
}

func removePart(name string) error {
	if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
