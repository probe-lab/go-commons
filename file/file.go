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

const partSuffix = ".part"

// Atomic is a file that appears at its path only when Commit succeeds. Until
// then the content goes, buffered, to path + ".part". An Atomic is not safe
// for concurrent use.
//
// Abort does nothing after Commit, so it can be deferred:
//
//	out, err := file.CreateAtomic(path)
//	if err != nil {
//		return err
//	}
//	defer out.Abort()
//
//	if _, err := out.Write(data); err != nil {
//		return err
//	}
//
//	return out.Commit()
type Atomic struct {
	path   string
	part   *os.File
	buf    *bufio.Writer
	closed bool
}

var _ io.Writer = (*Atomic)(nil)

// CreateAtomic creates the part file for path, and the directories above
// path.
func CreateAtomic(path string) (*Atomic, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	part, err := os.Create(path + partSuffix)
	if err != nil {
		return nil, err
	}

	return &Atomic{path: path, part: part, buf: bufio.NewWriterSize(part, 1<<20)}, nil
}

// Write writes to the part file.
func (a *Atomic) Write(p []byte) (int, error) {
	return a.buf.Write(p)
}

// Commit syncs the part file to disk, closes it and renames it to the path;
// the rename replaces any file there. When a step fails, Commit removes the
// part file and leaves the file at the path as it was.
func (a *Atomic) Commit() error {
	if a.closed {
		return fmt.Errorf("write %s: file already closed", a.path)
	}
	a.closed = true

	err := a.buf.Flush()
	if err == nil {
		err = a.part.Sync()
	}

	if cerr := a.part.Close(); err == nil {
		err = cerr
	}

	if err == nil {
		err = os.Rename(a.part.Name(), a.path)
	}

	if err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", a.path, err), removePart(a.part.Name()))
	}

	return nil
}

// Abort closes and removes the part file and leaves the file at the path as
// it was. After Commit or an earlier Abort it does nothing.
func (a *Atomic) Abort() error {
	if a.closed {
		return nil
	}
	a.closed = true

	return errors.Join(a.part.Close(), removePart(a.part.Name()))
}

// WriteAtomic writes a file through write and moves it into place only when
// write succeeds; see Atomic. When a step fails, it removes the part file
// and leaves the file at path as it was.
func WriteAtomic(path string, write func(w io.Writer) error) error {
	a, err := CreateAtomic(path)
	if err != nil {
		return err
	}

	if err := write(a); err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", path, err), a.Abort())
	}

	return a.Commit()
}

// WriteFileAtomic writes data to the file at path and moves it into place
// only when the write succeeds; see Atomic.
func WriteFileAtomic(path string, data []byte) error {
	a, err := CreateAtomic(path)
	if err != nil {
		return err
	}

	if _, err := a.Write(data); err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", path, err), a.Abort())
	}

	return a.Commit()
}

func removePart(name string) error {
	if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}
