package http

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

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
		slog.Error("failed to encode response", "err", err, "value", v)
	}
}

func EncodeErr(rw http.ResponseWriter, status int, errMsg string) {
	errResp := &ErrResponse{
		Message: errMsg,
	}
	Encode(rw, status, errResp)
}

func Decode[T any](rw io.Reader) (T, error) {
	var v T
	if err := json.NewDecoder(rw).Decode(&v); err != nil {
		return v, fmt.Errorf("decode json: %w", err)
	}
	return v, nil
}

func DecodeAndClose[T any](rw io.ReadCloser) (T, error) {
	data, err := Decode[T](rw)
	if err != nil {
		return data, err
	}
	if err := rw.Close(); err != nil {
		return data, fmt.Errorf("close reader: %w", err)
	}
	return data, nil
}
