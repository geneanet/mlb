package backend

import (
	"encoding/json"
	"reflect"

	"golang.org/x/exp/slices"
)

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
	slices.Sort(list)
	return list
}

func (tl TagList) MarshalJSON() ([]byte, error) {
	return json.Marshal(tl.List())
}

func NewTagList(list []string) TagList {
	tl := make(TagList, len(list))
	for _, t := range list {
		tl[t] = nil
	}
	return tl
}
