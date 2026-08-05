# Backends Processor

Backends processors take a source of backends (from an inventory or another processor) and perform operations such as health checking, metadata enrichment, or filtering.

## MySQL Processor

The `mysql` processor performs health checks on MySQL backends. It checks for connectivity, read-only status, and replication lag.

```hcl
backends_processor "mysql" "my_db" {
  source = backends_inventory.consul.mysql
  user = "mlb"
  password = "password"
  check_replica = true
  period = "1s"
  // ... timeouts
}
```

- `source` (string, required): The ID of the backend provider to monitor.
- `user` (string, optional): MySQL username for the health check connection.
- `password` (string, optional): MySQL password.
- `check_replica` (boolean, optional): If `true`, the processor will also monitor replication lag and status using `SHOW REPLICA STATUS`. Default: `false`.
- `period` (duration string, optional): How often to perform the health check. Default: `1s`.
- `max_period` (duration string, optional): The maximum duration between checks when an exponential backoff is applied due to repeated failures. Default: `5s`.
- `backoff_factor` (number, optional): The multiplier used to increase the check interval after a failure. Default: `1.5`.
- `connect_timeout` (duration string, optional): Maximum duration to wait for a connection to the MySQL server to be established. Default: `0s` (OS default).
- `read_timeout` (duration string, optional): Maximum duration to wait for a response from the MySQL server during a check. Default: `0s`.
- `write_timeout` (duration string, optional): Maximum duration to wait for a command to be sent to the MySQL server. Default: `0s`.
- `conn_max_lifetime` (duration string, optional): The maximum amount of time a health check connection may be reused before being recreated. Default: `5m`.

### Metadata Provided (`mysql` bucket)

- `backend.meta.mysql.status`: `ok` if the check passed, `err` otherwise.
- `backend.meta.mysql.readonly`: `true` if `@@read_only` is set.
- `backend.meta.mysql.replica_latency`: Replication lag in seconds (if `check_replica` is enabled).
- `backend.meta.mysql.replica_running`: `true` if replication is active (if `check_replica` is enabled).

---

## Redis Processor

The `redis` processor performs health checks on Redis backends and identifies their role (master/slave).

```hcl
backends_processor "redis" "my_redis" {
  source = backends_inventory.static.redis_nodes
  password = "secret_password"
}
```

- `source` (string, required): The ID of the backend provider to monitor.
- `password` (string, optional): Redis password for the health check connection.
- `period` (duration string, optional): How often to perform the health check. Default: `1s`.
- `max_period` (duration string, optional): The maximum duration between checks when backoff is applied. Default: `5s`.
- `backoff_factor` (number, optional): The factor by which the check interval increases on failure. Default: `1.5`.
- `connect_timeout` (duration string, optional): Maximum duration to wait for a TCP connection to the Redis server. Default: `1s`.
- `read_timeout` (duration string, optional): Maximum duration to wait for a response from the Redis server (used for the `ROLE` or `INFO` commands). Default: `1s`.
- `write_timeout` (duration string, optional): Maximum duration to wait for a command to be sent to the Redis server. Default: `1s`.

### Metadata Provided (`redis` bucket)

- `backend.meta.redis.status`: `ok` or `err`.
- `backend.meta.redis.role`: `master`, `slave`, or `unknown`.
- `backend.meta.redis.readonly`: `true` if the node is a slave.
- `backend.meta.redis.connected_slaves`: Number of connected slaves (only for masters).
- `backend.meta.redis.master_link_status`: `up` or `down` (only for slaves).
- `backend.meta.redis.master_sync_in_progress`: `true` if the slave is currently syncing with the master.

---

## Consul KV Processor

The `consul_kv` processor allows fetching additional metadata for backends from Consul's Key-Value store.

```hcl
backends_processor "consul_kv" "weights" {
  source = backends_inventory.consul.mysql
  url = "http://localhost:8500"
  value "weight" {
    consul_key = "server_weights/${backend.meta.consul.node}"
    default = "10"
  }
}
```

- `source` (string, required): The ID of the backend provider.
- `url` (string, required): Consul URL.
- `value` (block, required): Defines a KV watch.
    - `id` (label): The name of the metadata key.
    - `consul_key` (HCL expression): The key to fetch from Consul. You can use `${backend.meta...}` to dynamically build the key.
    - `default` (string): Default value if the key is missing.

### Metadata Provided (bucket: `consul_kv`)

The processor populates the `consul_kv` bucket with the IDs defined in the `value` blocks. Access it via `backend.meta.consul_kv.<id>`.

---

## Simple Filter Processor

The `simple_filter` processor filters, sorts, and limits a list of backends based on their metadata using HCL expressions.

```hcl
backends_processor "simple_filter" "mysql_slaves" {
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

- `source` (string, required): The ID of the backend provider.
- `condition` (HCL expression, required): A boolean expression that must evaluate to `true` for a backend to be included.
- `sort_by` (HCL expression, optional): An expression used to sort the backends.
- `sort_order` (string, optional): `asc` (default) or `desc`.
- `limit` (number, optional): Maximum number of backends to return after sorting.

## Metadata Summary

When using `simple_filter` or `wrr` weights, you can access the following metadata buckets:

| Bucket | Key | Provided By | Description |
|---|---|---|---|
| `consul` | `node` | `backends_inventory "consul"` | Consul node name |
| `consul` | `weight` | `backends_inventory "consul"` | Consul service weight |
| `consul` | `tags` | `backends_inventory "consul"` | Set of Consul service tags |
| `mysql` | `status` | `backends_processor "mysql"` | `ok` or `err` |
| `mysql` | `readonly` | `backends_processor "mysql"` | `true` if backend is read-only |
| `mysql` | `replica_latency`| `backends_processor "mysql"` | Replication lag in seconds |
| `mysql` | `replica_running`| `backends_processor "mysql"` | `true` if replication is active |
| `redis` | `status` | `backends_processor "redis"` | `ok` or `err` |
| `redis` | `role` | `backends_processor "redis"` | `master`, `slave`, or `unknown` |
| `redis` | `readonly` | `backends_processor "redis"` | `true` if backend is a slave |
| `redis` | `connected_slaves` | `backends_processor "redis"` | Number of connected slaves |
| `redis` | `master_link_status` | `backends_processor "redis"` | `up` or `down` (for slaves) |
| `redis` | `master_sync_in_progress` | `backends_processor "redis"` | `true` during replication sync |
| `consul_kv`| `<id>` | `backends_processor "consul_kv"`| Value fetched from Consul KV |
