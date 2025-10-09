package http

type Response[T any] struct {
	Data  T            `json:"data,omitempty"`
	Error *ErrResponse `json:"error,omitempty"`
}

type ErrResponse struct {
	Message string `json:"message"`
}

func (e *ErrResponse) Error() string {
	return e.Message
}
