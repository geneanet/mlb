# Balancer

Balancers are responsible for selecting a backend for a given request or connection. They sit between processors and proxies.

## WRR (Weighted Round-Robin) Balancer

The `wrr` balancer distributes traffic among backends according to a weight determined by an HCL expression.

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
