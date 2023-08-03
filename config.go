package main

type Config struct {
	ConsulInventoryList []ConsulInventoryConfig `hcl:"consul_inventory,block"`
	MySQLCheckerList    []MySQLCheckerConfig    `hcl:"mysql_checker,block"`
	SimpleFilterList    []SimpleFilterConfig    `hcl:"simple_filter,block"`
	WRRBalancerList     []WRRBalancerConfig     `hcl:"wrr_balancer,block"`
	TCPProxyList        []TCPProxyConfig        `hcl:"tcp_proxy,block"`
	HTTPAddress         string                  `hcl:"http_address"`
	RLimitNOFile        uint64                  `hcl:"rlimit_nofile,optional"`
}

type ConsulInventoryConfig struct {
	ID            string  `hcl:"id,label"`
	URL           string  `hcl:"url"`
	Service       string  `hcl:"service"`
	Period        string  `hcl:"period"`
	MaxPeriod     string  `hcl:"max_period"`
	BackoffFactor float64 `hcl:"backoff_factor"`
}

type MySQLCheckerConfig struct {
	ID            string  `hcl:"id,label"`
	Source        string  `hcl:"source"`
	User          string  `hcl:"user"`
	Password      string  `hcl:"password"`
	Period        string  `hcl:"period"`
	MaxPeriod     string  `hcl:"max_period"`
	BackoffFactor float64 `hcl:"backoff_factor"`
}

type SimpleFilterConfig struct {
	ID     string `hcl:"id,label"`
	Source string `hcl:"source"`
	Tag    string `hcl:"tag"`
	Status string `hcl:"status"`
}

type WRRBalancerConfig struct {
	ID     string `hcl:"id,label"`
	Source string `hcl:"source"`
}

type TCPProxyConfig struct {
	ID           string `hcl:"id,label"`
	Source       string `hcl:"source"`
	Address      string `hcl:"address"`
	CloseTimeout string `hcl:"close_timeout"`
}
