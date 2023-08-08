package backend

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
	return b.Address == other.Address && b.Status == other.Status && b.Weight == other.Weight && b.Tags.Equal(other.Tags) && b.Meta.Equal(other.Meta)
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
