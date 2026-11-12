package file

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

var _ io.Writer = (*Atomic)(nil)

func TestWriteAtomic(t *testing.T) {
	errWrite := errors.New("write failed")

	tests := []struct {
		name     string
		existing string // content of the file before the write; empty means none
		content  string
		writeErr error
		want     string // content after the write; empty means no file
	}{
		{name: "new file", content: "new", want: "new"},
		{name: "replaces a file", existing: "old", content: "new", want: "new"},
		{name: "failed write keeps the old file", existing: "old", content: "partial", writeErr: errWrite, want: "old"},
		{name: "failed write leaves no file", content: "partial", writeErr: errWrite, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "a", "b", "file.json")
			if tt.existing != "" {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(tt.existing), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			err := WriteAtomic(path, func(w io.Writer) error {
				if _, err := io.WriteString(w, tt.content); err != nil {
					return err
				}
				return tt.writeErr
			})
			if !errors.Is(err, tt.writeErr) {
				t.Fatalf("WriteAtomic error = %v, want %v", err, tt.writeErr)
			}

			got, err := os.ReadFile(path)
			switch {
			case tt.want == "" && !errors.Is(err, os.ErrNotExist):
				t.Errorf("file exists (read error %v), want none", err)
			case tt.want != "" && string(got) != tt.want:
				t.Errorf("file = %q (read error %v), want %q", got, err, tt.want)
			}

			if _, err := os.Stat(path + PartSuffix); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("part file is left over (stat error %v)", err)
			}
		})
	}
}

func TestAtomicCommitAndAbort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")

	a, err := CreateAtomic(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file is visible before Commit (stat error %v)", err)
	}

	if err := a.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "data" {
		t.Fatalf("file = %q (read error %v), want data", got, err)
	}

	if err := a.Commit(); err == nil {
		t.Error("second Commit succeeded, want an error")
	}
	if err := a.Abort(); err != nil {
		t.Errorf("Abort after Commit: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("Abort after Commit removed the file: %v", err)
	}

	b, err := CreateAtomic(filepath.Join(t.TempDir(), "aborted"))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, err := os.Stat(b.part.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("part file is left over after Abort (stat error %v)", err)
	}
}
