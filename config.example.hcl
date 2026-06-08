metrics {
  address = ":2112"
}

system {
  rlimit {
    nofile = 65536
  }
}

backends_inventory "consul" "mysql" {
  url = "http://localhost:8500"
  service = "mysql"
  // period = "1s"
  // max_period = "5s"
  // backoff_factor = 1.5
}

backends_inventory "static" "mysql_static" {
  hosts = ["127.0.0.1:3306", "127.0.0.1:3307"]
}

backends_processor "consul_kv" "sqlweight" {
  source = backends_inventory.consul.mysql
  url = "http://localhost:8500"
  // period = "500ms"
  // max_period = "2s"
  // backoff_factor = 1.5
  value "weight" {
    consul_key = "server_weights/${backend.meta.consul.node}"
    default = "0"
  }
}

backends_processor "mysql" "mysql" {
  source = backends_inventory.consul.mysql
  user = "mlb"
  password = "mlb_password"
  // period = "1s"
  // max_period = "5s"
  // backoff_factor = 1.5
	// connect_timeout = "0s"
	// read_timeout = "0s"
	// write_timeout = "0s"
	// conn_max_lifetime = "5m"
  // check_replica = false
}

backends_processor "simple_filter" "mysql_main_ro" {
  source = backends_processor.mysql.mysql
  condition = (
    backend.meta.mysql.status == "ok"
    && backend.meta.mysql.readonly == true
    && contains(backend.meta.consul.tags, "main")
    && !contains(backend.meta.consul.tags, "backup")
  )
}

balancer "wrr" "mysql_main_ro" {
  source = backends_processor.simple_filter.mysql_main_ro
  weight = backend.meta.consul.weight
  // timeout = "0s"
}

proxy "tcp" "mysql_main_ro" {
  source = balancer.wrr.mysql_main_ro
  // backup_source = balancer.wrr.other_balancer
  addresses = [":3306"]
  // close_timeout = "0s"
 	// connect_timeout = "0s"
	// client_timeout = "0s"
	// server_timeout = "0s"
  // buffer_size = 16384
  // nodelay = false
}

proxy "redis" "redis" {
  source = backends_inventory.consul.mysql
  addresses = [":6379"]
  // connect_timeout = "0s"
  // close_timeout = "0s"
  // backend_wait_timeout = "0s"
  // buffer_size = 16384
  // client_queue_size = 64
  // backend_inflight_queue_size = 512
  // backend_connection_pool_size = 1
  // retry_period = "100ms"
  // retry_max_period = "1s"
  // retry_backoff_factor = 1.5
}
