package main

import "strings"

type proxyFlag struct {
	id      string
	address string
	tag     string
	status  string
}

type proxyFlags []proxyFlag

func (i *proxyFlags) String() string {
	ret := "["
	for _, f := range *i {
		ret += f.address + "," + f.tag + "," + f.status
	}
	ret += "]"
	return ret
}

func (i *proxyFlags) Set(value string) error {
	parts := strings.Split(value, ",")
	*i = append(*i, proxyFlag{
		id:      parts[0],
		address: parts[1],
		tag:     parts[2],
		status:  parts[3],
	})
	return nil
}
