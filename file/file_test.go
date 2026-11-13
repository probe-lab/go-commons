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

func TestWriteFileAtomic(t *testing.T) {
	tests := []struct {
		name     string
		existing string // content of the file before the write; empty means none
		content  string
	}{
		{name: "new file", content: "new"},
		{name: "replaces a file", existing: "old", content: "new"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "a", "b", "file.json")
			if tt.existing != "" {
				writeTestFile(t, path, tt.existing)
			}

			if err := WriteFileAtomic(path, []byte(tt.content)); err != nil {
				t.Fatalf("WriteFileAtomic: %v", err)
			}

			checkTestFile(t, path, tt.content)
		})
	}
}

func TestAtomic(t *testing.T) {
	tests := []struct {
		name      string
		existing  string // content of the file before the write; empty means none
		steps     []string
		commitErr bool   // the last commit step must fail
		want      string // content after the steps; empty means no file
	}{
		{name: "commit", steps: []string{"commit"}, want: "new"},
		{name: "commit replaces a file", existing: "old", steps: []string{"commit"}, want: "new"},
		{name: "abort leaves no file", steps: []string{"abort"}, want: ""},
		{name: "abort keeps the old file", existing: "old", steps: []string{"abort"}, want: "old"},
		{name: "abort after commit does nothing", steps: []string{"commit", "abort"}, want: "new"},
		{name: "abort twice", steps: []string{"abort", "abort"}, want: ""},
		{name: "commit after abort fails", steps: []string{"abort", "commit"}, commitErr: true, want: ""},
		{name: "commit twice fails", steps: []string{"commit", "commit"}, commitErr: true, want: "new"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "a", "b", "file.json")
			if tt.existing != "" {
				writeTestFile(t, path, tt.existing)
			}

			a, err := CreateAtomic(path)
			if err != nil {
				t.Fatalf("CreateAtomic: %v", err)
			}

			if _, err := io.WriteString(a, "new"); err != nil {
				t.Fatalf("Write: %v", err)
			}

			for i, step := range tt.steps {
				last := i == len(tt.steps)-1

				switch step {
				case "commit":
					err := a.Commit()
					if wantErr := last && tt.commitErr; (err != nil) != wantErr {
						t.Fatalf("Commit error = %v, want error %t", err, wantErr)
					}
				case "abort":
					if err := a.Abort(); err != nil {
						t.Fatalf("Abort: %v", err)
					}
				}
			}

			checkTestFile(t, path, tt.want)
		})
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// checkTestFile checks the content of the file at path, where an empty want
// means no file, and that no part file is left over.
func checkTestFile(t *testing.T, path, want string) {
	t.Helper()

	got, err := os.ReadFile(path)
	switch {
	case want == "" && !errors.Is(err, os.ErrNotExist):
		t.Errorf("file exists (read error %v), want none", err)
	case want != "" && string(got) != want:
		t.Errorf("file = %q (read error %v), want %q", got, err, want)
	}

	if _, err := os.Stat(path + ".part"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("part file is left over (stat error %v)", err)
	}
}
