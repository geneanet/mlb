# Configuration Examples

This page provides various configuration examples to help you build your MLB stack.

## Basic TCP Load Balancer

This example shows a simple setup that distributes TCP traffic between two static backends using round-robin.

```hcl
backends_inventory "static" "my_servers" {
  hosts = ["127.0.0.1:3306", "127.0.0.1:3307"]
}

balancer "wrr" "my_balancer" {
  source = backends_inventory.static.my_servers
  weight = 1 // Equal weights for simple round-robin
}

proxy "tcp" "my_proxy" {
  source = balancer.wrr.my_balancer
  addresses = [":3300"]
}
```

## Advanced MySQL Filtering

This example demonstrates how to use the `mysql` processor and `simple_filter` to route traffic only to healthy, read-only slave nodes, sorted by their replication latency.

```hcl
backends_inventory "consul" "mysql_cluster" {
  url = "http://localhost:8500"
  service = "mysql"
}

backends_processor "mysql" "checker" {
  source = backends_inventory.consul.mysql_cluster
  user = "mlb"
  password = "password"
  check_replica = true
}

backends_processor "simple_filter" "healthy_slaves" {
  source = backends_processor.mysql.checker
  condition = (
    backend.meta.mysql.status == "ok"
    && backend.meta.mysql.readonly == true
  )
  sort_by = backend.meta.mysql.replica_latency
  sort_order = "asc"
  limit = 5
}

balancer "wrr" "slave_balancer" {
  source = backends_processor.simple_filter.healthy_slaves
  weight = 1
}

proxy "tcp" "mysql_read_proxy" {
  source = balancer.wrr.slave_balancer
  addresses = [":3307"]
}
```

## Redis Proxy with Role Detection

Route traffic to the master node of a Redis cluster.

```hcl
backends_inventory "static" "redis_nodes" {
  hosts = ["10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379"]
}

backends_processor "redis" "checker" {
  source = backends_inventory.static.redis_nodes
}

backends_processor "simple_filter" "master_only" {
  source = backends_processor.redis.checker
  condition = backend.meta.redis.role == "master"
}

balancer "wrr" "master_balancer" {
  source = backends_processor.simple_filter.master_only
  weight = 1
}

proxy "redis" "redis_frontend" {
  source = balancer.wrr.master_balancer
  addresses = [":6379"]
}
```
