package file

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

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

			if _, err := os.Stat(path + ".part"); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("part file is left over (stat error %v)", err)
			}
		})
	}
}
