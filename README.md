# MLB - Modular Load Balancer

MLB (Modular Load Balancer) is a high-performance, flexible, and modular L4/L7 load-balancer written in Go.
It is designed to be easily extensible through its modular architecture and provides advanced features like zero-downtime restarts and deep integration with service discovery tools like Consul.

## Key Features

- **Multi-protocol Support:** TCP, Redis, and Memcache.
- **Dynamic Backend Discovery:** Support for Consul and static host lists.
- **Advanced Health Checking:** Native health checks for MySQL and Redis.
- **Flexible Filtering:** Filter and sort backends based on rich metadata using HCL expressions.
- **Load Balancing:** Smooth Weighted Round-Robin (SWRR) and Weighted Least Connections (WLC) with dynamic weight resolution.
- **Observability:** Prometheus metrics and a built-in topology dashboard.
- **Performance:** Designed for high throughput with connection pooling and efficient protocol handling.

## Documentation

Comprehensive documentation for MLB is available in the **[docs](docs/README.md)** folder:

- **[Architecture](docs/architecture.md)**: Core concepts and the pipeline model.
- **[Operations Guide](docs/operations.md)**: Installation, CLI flags, and zero-downtime restarts.
- **[Configuration Guide](docs/configuration.md)**: Global settings and HCL expressions.
- **[Examples](docs/examples.md)**: Practical configuration snippets.
- **[Monitoring](docs/dashboard.md)**: Dashboard and Prometheus metrics.

Detailed module documentation:
- [Backends Inventory](docs/backends_inventory.md)
- [Backends Processor](docs/backends_processor.md)
- [Balancer](docs/balancer.md)
- [Proxy](docs/proxy.md)

## Quickstart

1.  **Build** MLB: `go build -o mlb .`
2.  **Configure**: Create a `config.hcl` (see `config.example.hcl` for a template).
3.  **Run**: `./mlb -config config.hcl`

For production use with zero-downtime reloads, see the [Operations Guide](docs/operations.md).

## License

MLB is released under the Mozilla Public License Version 2.0. See `LICENSE` for details.
