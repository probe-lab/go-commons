//go:build !linux

package limit

// osReader reads no signals outside Linux.
type osReader struct{}

func newOSReader(float64) *osReader {
	return &osReader{}
}

func (o *osReader) read(r *Reading) {
	r.Missing = append(r.Missing, SignalFiles, SignalMemory, SignalCPU)
}
