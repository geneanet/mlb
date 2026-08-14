# Balancer

Balancers are responsible for selecting a backend for a given request or connection. They sit between processors and proxies.

## WRR (Weighted Round-Robin) Balancer

The `wrr` balancer distributes traffic among backends according to a weight determined by an HCL expression. It uses a **Smooth Weighted Round-Robin** algorithm (similar to NGINX) and is implemented with a **lock-free** design for maximum performance.

```hcl
balancer "wrr" "my_balancer" {
  source = backends_processor.simple_filter.healthy_nodes
  weight = int(backend.meta.consul_kv.weight)
  timeout = "5s"
}
```

- `source` (string, required): The ID of the backend provider (inventory or processor).
- `weight` (HCL expression, required): An expression that evaluates to an integer representing the weight of the backend. Higher weights receive more traffic.
- `timeout` (duration string, optional): If the balancer has no available backends, it will wait up to this duration for a backend to become available before failing. Default: `0s`.
- `log_backend_updates` (boolean, optional): If true, logs an INFO message when a backend is added or removed.

## WLC (Weighted Least Connections) Balancer

The `wlc` balancer selects the backend with the lowest number of active connections relative to its weight. It is ideal for protocols with long-lived connections like TCP.

```hcl
balancer "wlc" "my_balancer" {
  source = backends_processor.simple_filter.healthy_nodes
  weight = int(backend.meta.consul_kv.weight)
  timeout = "5s"
}
```

- `source` (string, required): The ID of the backend provider.
- `weight` (HCL expression, required): An expression that evaluates to an integer. The balancer selects the backend minimizing `active_connections / weight`.
- `timeout` (duration string, optional): Duration to wait for a backend if none are available. Default: `0s`.
- `log_backend_updates` (boolean, optional): If true, logs an INFO message when a backend is added or removed.

