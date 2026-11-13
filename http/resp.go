package http

// Response is the JSON envelope of every server response.
type Response[T any] struct {
	Data  T            `json:"data,omitempty"`
	Error *ErrResponse `json:"error,omitempty"`
}

// ErrResponse is the error part of a Response.
type ErrResponse struct {
	Message string `json:"message"`
}

func (e *ErrResponse) Error() string {
	return e.Message
}
