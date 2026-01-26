package ptr

func From[T comparable](t T) *T {
	if t == *new(T) {
		return nil
	}
	return &t
}
