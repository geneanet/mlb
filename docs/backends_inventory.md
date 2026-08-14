# Backends Inventory

Backends inventories are modules responsible for discovering available backend instances. They provide a raw list of addresses and basic metadata.

## Static Inventory

The `static` inventory allows you to manually specify a list of backend addresses.

```hcl
backends_inventory "static" "my_backends" {
  hosts = ["127.0.0.1:3306", "127.0.0.1:3307"]
}
```

- `hosts` (list of strings, required): A list of `host:port` addresses for the backends.
- `log_backend_updates` (boolean, optional): If true, logs an INFO message when a backend is added or removed.

## Consul Inventory

The `consul` inventory discovers backends by querying the Consul Service Discovery API.

```hcl
backends_inventory "consul" "mysql" {
  url = "http://localhost:8500"
  service = "mysql"
  period = "1s"
  max_period = "5s"
  backoff_factor = 1.5
}
```

- `url` (string, required): The base URL of the Consul agent or cluster.
- `service` (string, required): The name of the service to discover in Consul.
- `period` (duration string, optional): The polling period for Consul updates. Default: `1s`.
- `max_period` (duration string, optional): The maximum polling period when backoff is applied due to errors. Default: `5s`.
- `backoff_factor` (number, optional): The factor by which the polling period increases on error. Default: `1.5`.
- `log_backend_updates` (boolean, optional): If true, logs an INFO message when a backend is added or removed.

### Metadata Provided

The `consul` inventory adds the following metadata to each backend in the `consul` bucket:

- `backend.meta.consul.node`: The Consul node name.
- `backend.meta.consul.weight`: The service weight from Consul.
- `backend.meta.consul.tags`: A set of tags associated with the service in Consul.
