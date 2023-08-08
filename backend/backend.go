package backend

import "golang.org/x/exp/slices"

// Backend
type Backend struct {
	Address string
	Status  string
	Tags    []string
	Weight  int
	Meta    MetaMap
}

func (b *Backend) Copy() *Backend {
	new := &Backend{
		Address: b.Address,
		Status:  b.Status,
		Weight:  b.Weight,
		Tags:    make([]string, len(b.Tags)),
		Meta:    b.Meta.Copy(),
	}
	copy(new.Tags, b.Tags)
	return new
}

func (b *Backend) Equal(other *Backend) bool {
	if b.Address != other.Address || b.Status != other.Status || b.Weight != other.Weight {
		return false
	}

	slices.Sort(b.Tags)
	slices.Sort(other.Tags)

	return slices.Equal(b.Tags, other.Tags) && b.Meta.Equal(other.Meta)
}

func (b *Backend) UpdateTags(new_tags []string) {
	b.Tags = make([]string, len(new_tags))
	copy(b.Tags, new_tags)
}

func (b *Backend) UpdateMeta(new_meta MetaMap, except ...string) {
	new := new_meta.Copy()
	for _, k := range except {
		if v, ok := b.Meta[k]; ok {
			new[k] = v
		}
	}
	b.Meta = new
}

// Messages
type BackendMessage struct {
	Kind    BackendMessageKind
	Address string
	Backend *Backend
}

type BackendMessageKind int

const (
	MsgBackendAdded BackendMessageKind = iota
	MsgBackendModified
	MsgBackendRemoved
)

// Interfaces
type Subscribable interface {
	Subscribe() chan BackendMessage
}

type BackendProvider interface {
	GetBackend() *Backend
}
