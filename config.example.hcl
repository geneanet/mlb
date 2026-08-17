metrics {
  address = ":2112"
}

system {
  rlimit {
    nofile = 65536
  }
  // gomaxprocs = 4
}

backends_inventory "consul" "mysql" {
  url = "http://localhost:8500"
  service = "mysql"
  // period = "1s"
  // max_period = "5s"
  // backoff_factor = 1.5
  // log_backend_updates = true
}

backends_inventory "consul" "redis" {
  url = "http://localhost:8500"
  service = "redis"
  // period = "1s"
  // max_period = "5s"
  // backoff_factor = 1.5
  // log_backend_updates = true
}

backends_inventory "static" "memcache_static" {
  hosts = ["127.0.0.1:11211", "127.0.0.1:11212"]
  // log_backend_updates = true
}

backends_inventory "static" "mysql_static" {
  hosts = ["127.0.0.1:3306", "127.0.0.1:3307"]
  // log_backend_updates = true
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
  // log_backend_updates = true
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
  // retry_period = "100ms"
  // retry_max_period = "1s"
  // retry_backoff_factor = 1.5
  // retry_max_attempts = 3
  // conn_max_lifetime = "5m"
  // check_replica = false
  // log_backend_updates = true
}

backends_processor "redis" "redis" {
  source = backends_inventory.consul.redis
  // password = "redis_password"
  // period = "1s"
  // max_period = "5s"
  // backoff_factor = 1.5
  // connect_timeout = "1s"
  // read_timeout = "1s"
  // write_timeout = "1s"
  // retry_period = "100ms"
  // retry_max_period = "1s"
  // retry_backoff_factor = 1.5
  // retry_max_attempts = 3
  // log_backend_updates = true
}

backends_processor "simple_filter" "mysql_main_ro" {
  source = backends_processor.mysql.mysql
  condition = (
    backend.meta.mysql.status == "ok"
    && backend.meta.mysql.readonly == true
    && contains(backend.meta.consul.tags, "main")
    && !contains(backend.meta.consul.tags, "backup")
  )
  // Optional sorting and limiting
  // sort_by = backend.meta.mysql.replica_latency
  // sort_order = "asc" // "asc" (default) or "desc"
  // limit = 10
  // log_backend_updates = true
}

backends_processor "simple_filter" "redis_master" {
  source = backends_processor.redis.redis
  condition = (
    backend.meta.redis.status == "ok"
    && backend.meta.redis.role == "master"
  )
  // log_backend_updates = true
}

balancer "wrr" "mysql_main_ro" {
  source = backends_processor.simple_filter.mysql_main_ro
  weight = backend.meta.consul.weight
  // timeout = "0s"
  // log_backend_updates = true
}

balancer "wrr" "redis_master" {
  source = backends_processor.simple_filter.redis_master
  // timeout = "0s"
  // log_backend_updates = true
}

proxy "tcp" "mysql_main_ro" {
  sources = [
    balancer.wrr.mysql_main_ro
    // balancer.wrr.other_balancer
    ]
  addresses = [":3306"]
  // addresses = ["unix:/tmp/mysql_proxy.sock"]
  // close_timeout = "0s"
  // connect_timeout = "0s"
  // client_timeout = "0s"
  // server_timeout = "0s"
  // timeout_margin = "1s"
  // backend_tcp_keepalive = "5s"
  // buffer_size = "16kb"
  // close_on_backend_removal = false
}

proxy "redis" "redis" {
  source = balancer.wrr.redis_master
  addresses = [":6379"]
  // connect_timeout = "5s"
  // close_timeout = "30s"
  // backend_wait_timeout = "0s"
  // backend_tcp_keepalive = "5s"
  // buffer_size = "16kb"
  // max_reused_buffer_size = "64kb"
  // preconnect = 0
  // idle_timeout = "5m"
  // idle_cleanup_period = "10s"
  // healthcheck = false
  // healthcheck_timeout = "1s"
  // reset_timeout = "2s"
  // log_backend_updates = true
}

proxy "memcache" "memcache" {
  source = backends_inventory.static.memcache_static
  addresses = [":11211"]
  // connect_timeout = "0s"
  // close_timeout = "0s"
  // buffer_size = "16kb"
  // client_queue_size = 64
  // backend_input_queue_size = 1024
  // backend_inflight_queue_size = 512
  // backend_min_connections = 1
  // backend_max_connections = 1
  // backend_tcp_keepalive = "5s"
  // max_fields_per_command = 16
  // flush_backend_when_added = false
  // log_backend_updates = true
}
