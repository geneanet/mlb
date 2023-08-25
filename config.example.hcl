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

backends_processor "mysql" "mysql" {
  source = backends_inventory.consul.mysql
  user = "mlb"
  password = "mlb_password"
  // period = "500ms"
  // max_period = "2s"
  // backoff_factor = 1.5
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
}

proxy "tcp" "mysql_main_ro" {
  source = balancer.wrr.mysql_main_ro
  address = ":3306"
  // close_timeout = "10s"
}