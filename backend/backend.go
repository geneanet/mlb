package backend

import (
	"reflect"
)

// Backend
type Backend struct {
	Address string
	Status  string
	Tags    TagList
	Weight  int
	Meta    MetaMap
}

func (b *Backend) Clone() *Backend {
	new := &Backend{
		Address: b.Address,
		Status:  b.Status,
		Weight:  b.Weight,
		Tags:    b.Tags.Clone(),
		Meta:    b.Meta.Clone(),
	}
	return new
}

func (b *Backend) Equal(other *Backend) bool {
	if b.Address != other.Address || b.Status != other.Status || b.Weight != other.Weight {
		return false
	}

	return b.Tags.Equal(other.Tags) && b.Meta.Equal(other.Meta)
}

func (b *Backend) UpdateTags(new_tags TagList) {
	b.Tags = new_tags.Clone()
}

func (b *Backend) UpdateMeta(new_meta MetaMap, except ...string) {
	new := new_meta.Clone()
	for _, k := range except {
		if v, ok := b.Meta[k]; ok {
			new[k] = v
		}
	}
	b.Meta = new
}

// TagList
type TagList map[string]interface{}

func (tl TagList) Add(t string) {
	tl[t] = nil
}

func (tl TagList) Remove(t string) {
	delete(tl, t)
}

func (tl TagList) Has(t string) bool {
	_, ok := tl[t]
	return ok
}

func (tl1 TagList) Equal(tl2 TagList) bool {
	return reflect.DeepEqual(tl1, tl2)
}

func (tl TagList) Clone() TagList {
	newtl := make(TagList, len(tl))
	for k := range tl {
		newtl[k] = nil
	}
	return newtl
}

func (tl TagList) List() []string {
	list := make([]string, len(tl))
	i := 0
	for k := range tl {
		list[i] = k
		i++
	}
	return list
}

func NewTagList(list []string) TagList {
	tl := make(TagList, len(list))
	for _, t := range list {
		tl[t] = nil
	}
	return tl
}

// Map
type BackendsMap map[string]*Backend

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
