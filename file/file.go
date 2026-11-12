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

// WriteAtomic writes a file through write and moves it into place only when
// write succeeds. The content goes, buffered, to path + ".part", which is
// synced to disk and renamed to path; the rename replaces any file there.
// WriteAtomic creates the directories above path. When a step fails, it
// removes the part file and leaves the file at path as it was.
func WriteAtomic(path string, write func(w io.Writer) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	part := path + ".part"
	fh, err := os.Create(part)
	if err != nil {
		return err
	}

	err = writeAndSync(fh, write)
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(part, path)
	}
	if err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", path, err), removePart(part))
	}

	return nil
}

func writeAndSync(fh *os.File, write func(w io.Writer) error) error {
	bw := bufio.NewWriterSize(fh, 1<<20)
	if err := write(bw); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return fh.Sync()
}

func removePart(name string) error {
	if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
