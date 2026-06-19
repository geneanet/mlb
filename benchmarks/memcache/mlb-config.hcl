backends_inventory "static" "memcache" {
  hosts = ["memcached1:11211", "memcached2:11211"]
}

proxy "memcache" "bench" {
  source = backends_inventory.static.memcache
  addresses = ["0.0.0.0:11212"]
  backend_min_connections = 10
  backend_max_connections = 10
}
