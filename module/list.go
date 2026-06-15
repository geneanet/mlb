package module

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

type Module interface {
	GetID() string
	Bind(modules ModulesList)
}

type ModulesList map[string]Module

func NewModulesList() ModulesList {
	return ModulesList{}
}

func (ml ModulesList) AddModule(m Module) {
	ml[m.GetID()] = m
}

// TODO: Rewrite Get and Filter as methods of ModulesList when Go 1.27 (supporting generic methods) is released.

func Get[T any](ml ModulesList, id string) T {
	module, ok := ml[id]
	if !ok {
		log.Panic().Str("module", id).Msg("Module does not exist")
	}

	target, ok := module.(T)
	if !ok {
		log.Panic().
			Str("module", id).
			Str("expected", fmt.Sprintf("%T", *new(T))).
			Str("actual", fmt.Sprintf("%T", module)).
			Msg("Module is not of the expected type")
	}

	return target
}

func Filter[T any](ml ModulesList) ModulesList {
	result := NewModulesList()

	for _, m := range ml {
		if _, ok := m.(T); ok {
			result.AddModule(m)
		}
	}

	return result
}
