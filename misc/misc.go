package misc

import (
	"fmt"
)

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func EnsureError(err interface{}) error {
	switch err := err.(type) {
	case error:
		return err
	default:
		return fmt.Errorf("%v", err)
	}
}

func MapValues[K comparable, V any](m map[K]V) []V {
	values := make([]V, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

type GetIDInterface interface {
	GetID() string
}
