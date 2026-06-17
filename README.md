# MLB - Modular Load Balancer

MLB (Modular Load Balancer) is a high-performance, flexible, and modular L4/L7 load-balancer written in Go.
It is designed to be easily extensible through its modular architecture and provides advanced features like zero-downtime restarts and deep integration with service discovery tools like Consul.

## Key Features

- **Modular Architecture:** Build your load-balancing stack by composing Inventories, Processors, Balancers, and Proxies.
- **Service Discovery:** Native support for Consul (health checks and KV) and static configurations.
- **Deep Health Probing:** Built-in MySQL and Redis health checkers with replication awareness and role detection.
- **Zero-Downtime Restarts:** Integrated process manager allows for configuration reloads without dropping connections.
- **Redis Protocol Support:** Specialized Redis proxy with command filtering and backend connection pooling.
- **Observability:** Prometheus metrics and structured logging (zerolog).
- **HCL Configuration:** Human-friendly configuration using HashiCorp Configuration Language.

## Architecture

MLB uses a pipeline architecture where backends flow through several stages:

1.  **Backends Inventory:** Sources of backend addresses (e.g., `consul`, `static`).
2.  **Backends Processor:** Enhances or filters backends based on metadata or health checks.
    - `mysql`, `redis`: Deep health probing with replication awareness and role detection.
    - `consul_kv`: Dynamic metadata enrichment from Consul KV.
    - `simple_filter`: Powerful filtering, sorting, and limiting using HCL expressions.
3.  **Balancer:** Selects a backend from a processed list using a specific algorithm (e.g., `wrr` - Weighted Round Robin).
4.  **Proxy:** Accepts incoming connections and forwards traffic to the backend selected by the balancer (e.g., `tcp`, `redis`).

## Getting Started

### Installation

To build MLB from source, you need Go 1.22+:

```bash
go build -o mlb .
```

Alternatively, you can use the provided Dockerfile:

```bash
docker build -t mlb .
```

### Quickstart

1.  Create a configuration file `config.hcl`. You can start from the `config.example.hcl` provided in the repository.
2.  Run MLB:

```bash
./mlb -config config.hcl
```

### Zero-Downtime Restart

To enable zero-downtime restarts, run MLB in process-manager mode:

```bash
./mlb -config config.hcl -process-manager
```

When you want to reload the configuration:
1.  Send a `SIGHUP` to the process manager.
2.  The process manager will start a new worker with the new configuration.
3.  Once the new worker is ready, the old one will be gracefully shut down.

## Configuration

MLB uses HCL for its configuration. A comprehensive example with all available options can be found in `config.example.hcl`.

### Basic Example

```hcl
backends_inventory "static" "my_servers" {
  hosts = ["127.0.0.1:3306", "127.0.0.1:3307"]
}

balancer "wrr" "my_balancer" {
  source = backends_inventory.static.my_servers
}

proxy "tcp" "my_proxy" {
  source = balancer.wrr.my_balancer
  addresses = [":3300"]
}
```

### Advanced Filtering and Sorting

The `simple_filter` processor allows for complex logic to select the best backends:

```hcl
backends_processor "simple_filter" "healthy_slaves" {
  source = backends_processor.mysql.my_db
  condition = (
    backend.meta.mysql.status == "ok"
    && backend.meta.mysql.readonly == true
  )
  sort_by = backend.meta.mysql.replica_latency
  sort_order = "asc"
  limit = 5
}
```

## Observability

MLB exposes Prometheus metrics on the port configured in the `metrics` block.

```hcl
metrics {
  address = ":2112"
}
```

## License

MLB is released under the Mozilla Public License Version 2.0. See `LICENSE` for details.
