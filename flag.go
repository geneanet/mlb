package main

import "strings"

type proxyFlag struct {
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
		address: parts[0],
		tag:     parts[1],
		status:  parts[2],
	})
	return nil
}
