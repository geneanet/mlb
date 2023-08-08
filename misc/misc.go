package misc

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

type GetIDInterface interface {
	GetID() string
}
