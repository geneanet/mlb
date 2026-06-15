package misc

import (
	"fmt"
)

func EnsureError(err interface{}) error {
	switch err := err.(type) {
	case error:
		return err
	default:
		return fmt.Errorf("%v", err)
	}
}

type GetIDInterface interface {
	GetID() string
}
