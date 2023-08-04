package backend

type Backend struct {
	Address string
	Status  string
	Tags    []string
	Weight  int
}

func (b *Backend) Copy() *Backend {
	new := &Backend{
		Address: b.Address,
		Status:  b.Status,
		Weight:  b.Weight,
		Tags:    make([]string, len(b.Tags)),
	}
	copy(new.Tags, b.Tags)
	return new
}

func (b *Backend) UpdateTags(new_tags []string) {
	b.Tags = make([]string, len(new_tags))
	copy(b.Tags, new_tags)
}

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

type Subscribable interface {
	Subscribe() chan BackendMessage
}

type BackendProvider interface {
	GetBackend() *Backend
}
