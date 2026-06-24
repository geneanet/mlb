# MLB - Modular Load Balancer

MLB (Modular Load Balancer) is a high-performance, flexible, and modular L4/L7 load-balancer written in Go.
It is designed to be easily extensible through its modular architecture and provides advanced features like zero-downtime restarts and deep integration with service discovery tools like Consul.

## Key Features

- **Multi-protocol Support:** TCP, Redis, and Memcache.
- **Dynamic Backend Discovery:** Support for Consul and static host lists.
- **Advanced Health Checking:** Native health checks for MySQL and Redis.
- **Flexible Filtering:** Filter and sort backends based on rich metadata using HCL expressions.
- **Load Balancing:** Weighted Round-Robin (WRR) with dynamic weight resolution.
- **Observability:** Prometheus metrics and a built-in topology dashboard.
- **Performance:** Designed for high throughput with connection pooling and efficient protocol handling.

## Documentation Index

- [Architecture](architecture.md): Core concepts and the pipeline model.
- [General Configuration](configuration.md): Global settings like metrics and system limits.
- [Backends Inventory](backends_inventory.md): Discovering backends from various sources.
- [Backends Processor](backends_processor.md): Health checking, metadata enrichment, and filtering.
- [Balancer](balancer.md): Distributing traffic among backends.
- [Proxy](proxy.md): Frontend protocols and proxying logic.
- [Configuration Examples](examples.md): Practical snippets for common use cases.
- [Dashboard & Metrics](dashboard.md): Monitoring and visualizing your MLB setup.
- [Operations Guide](operations.md): Running MLB, CLI flags, and zero-downtime restarts.

## Getting Started

To start MLB, you need a configuration file (usually `config.hcl`). You can use the `config.example.hcl` provided in the repository as a template.

```bash
./mlb -config my_config.hcl
```

For debugging, you can enable debug logs:

```bash
./mlb -config my_config.hcl -debug
```
