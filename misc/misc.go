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

type GetIDInterface interface {
	GetID() string
}
