# Architecture

MLB uses a pipeline architecture where backends flow through several stages of enrichment and selection before being used by a proxy.

## The Pipeline Model

The load-balancing stack is composed of four main types of modules:

1.  **Backends Inventory:** Sources of backend addresses. They are responsible for discovering where the service instances are located (e.g., querying Consul or reading a static list).
2.  **Backends Processor:** Enhances or filters backends. Processors can perform health checks (MySQL, Redis), fetch additional metadata (Consul KV), or filter the list based on complex conditions.
3.  **Balancer:** Selects a backend from a processed list using a specific algorithm. MLB supports Smooth Weighted Round-Robin (`wrr`) and Weighted Least Connections (`wlc`).
4.  **Proxy:** Accepts incoming connections from clients and forwards traffic to the backend selected by the balancer. MLB provides specialized proxies for `tcp`, `redis`, and `memcache`.

## Metadata Flow

One of MLB's most powerful features is how metadata flows through the pipeline. Each stage can add "buckets" of metadata to a backend:

- **Consul Inventory** adds `node`, `weight`, and `tags`.
- **MySQL Processor** adds `status`, `readonly`, and `replica_latency`.
- **Consul KV Processor** adds custom keys from the KV store.

All subsequent modules (like `simple_filter` or `wrr` balancer) can then use this metadata in HCL expressions to make routing decisions.

## Zero-Downtime Design

MLB is designed for high availability from the ground up. By using `SO_REUSEPORT` and a dedicated process manager, it allows for seamless configuration reloads and binary upgrades without losing a single client connection. See the [Operations Guide](operations.md) for more details.
