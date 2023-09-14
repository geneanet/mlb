package module

import (
	"mlb/backend"

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

func (ml ModulesList) GetBackendUpdateProvider(id string) backend.BackendUpdateProvider {
	module, ok := ml[id]

	if !ok {
		log.Panic().Str("module", id).Msg("Module does not exist")
	}

	bup, ok := module.(backend.BackendUpdateProvider)

	if !ok {
		log.Panic().Str("module", id).Msg("Module is not a BackendUpdateProvider")
	}

	return bup
}

func (ml ModulesList) GetBackendProvider(id string) backend.BackendProvider {
	module, ok := ml[id]

	if !ok {
		log.Panic().Str("module", id).Msg("Module does not exist")
	}

	bp, ok := module.(backend.BackendProvider)

	if !ok {
		log.Panic().Str("module", id).Msg("Module is not a BackendProvider")
	}

	return bp
}

func (ml ModulesList) GetBackendListProvider(id string) backend.BackendListProvider {
	module, ok := ml[id]

	if !ok {
		log.Panic().Str("module", id).Msg("Module does not exist")
	}

	blp, ok := module.(backend.BackendListProvider)

	if !ok {
		log.Panic().Str("module", id).Msg("Module is not a BackendListProvider")
	}

	return blp
}

func (ml ModulesList) GetBackendListProviders() ModulesList {
	bpls := NewModulesList()

	for _, module := range ml {
		_, ok := module.(backend.BackendListProvider)
		if ok {
			bpls.AddModule(module)
		}
	}

	return bpls
}
