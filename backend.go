package main

type Backend struct {
	address string
	status  string
	tags    []string
	weight  int
}

func (b *Backend) Copy() *Backend {
	new := &Backend{
		address: b.address,
		status:  b.status,
		weight:  b.weight,
		tags:    make([]string, len(b.tags)),
	}
	copy(new.tags, b.tags)
	return new
}

func (b *Backend) UpdateTags(new_tags []string) {
	b.tags = make([]string, len(new_tags))
	copy(b.tags, new_tags)
}

type BackendMessage struct {
	kind    BackendMessageKind
	address string
	backend *Backend
}

type BackendMessageKind int

const (
	MsgBackendAdded BackendMessageKind = iota
	MsgBackendModified
	MsgBackendRemoved
)
