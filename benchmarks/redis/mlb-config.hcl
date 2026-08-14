backends_inventory "static" "redis" {
  hosts = ["valkey1:6379"]
}

balancer "wrr" "bench_balancer" {
  source = backends_inventory.static.redis
  weight = 1
}

proxy "redis" "bench" {
  source = backends_inventory.static.redis
  addresses = ["0.0.0.0:6380"]
  backend_wait_timeout = "5s"
}

proxy "tcp" "bench_tcp" {
  sources = [balancer.wrr.bench_balancer]
  addresses = ["0.0.0.0:6382"]
}
