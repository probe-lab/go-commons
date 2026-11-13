package http

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// Encode writes v as a JSON Response with the given status. An error or an
// *ErrResponse goes into the error field, any other value into the data field.
func Encode[T any](rw http.ResponseWriter, status int, v T) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)

	var resp *Response[T]
	switch val := any(v).(type) {
	case *ErrResponse:
		resp = &Response[T]{
			Error: val,
		}
	case error:
		resp = &Response[T]{
			Error: &ErrResponse{Message: val.Error()},
		}
	default:
		resp = &Response[T]{
			Data: v,
		}
	}

	if err := json.NewEncoder(rw).Encode(resp); err != nil {
		slog.Error("Failed to encode response", "err", err)
	}
}

// EncodeErr writes errMsg as a JSON error Response with the given status.
func EncodeErr(rw http.ResponseWriter, status int, errMsg string) {
	errResp := &ErrResponse{
		Message: errMsg,
	}
	Encode(rw, status, errResp)
}

// Decode reads one JSON value of type T from r.
func Decode[T any](r io.Reader) (T, error) {
	var v T
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		return v, fmt.Errorf("decode json: %w", err)
	}
	return v, nil
}

// DecodeAndClose decodes one JSON value of type T from r and closes r.
func DecodeAndClose[T any](r io.ReadCloser) (T, error) {
	data, err := Decode[T](r)
	if err != nil {
		return data, err
	}
	if err := r.Close(); err != nil {
		return data, fmt.Errorf("close reader: %w", err)
	}
	return data, nil
}
