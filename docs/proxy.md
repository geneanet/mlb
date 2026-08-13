# Proxy

Proxies are the frontend of MLB. They listen on network addresses, speak specific protocols, and forward requests to backends.

## TCP Proxy

A low-level TCP proxy that forwards raw bytes. It supports multiple prioritized sources (balancers or backend providers).

```hcl
proxy "tcp" "my_tcp_proxy" {
  sources = [
    balancer.wrr.primary_balancer,
    balancer.wrr.backup_balancer,
    backends_inventory.static.maintenance_page
  ]
  addresses = [":3306"]
  connect_timeout = "2s"
  close_on_backend_removal = true
}
```

- `sources` (list of strings, optional): A prioritized list of backend providers. The proxy will try to get a backend from each source in the order defined.
- `source` (string, optional, **deprecated**): The ID of the primary balancer. Used for backward compatibility; if defined, it is added as the first element of the `sources` list. Use `sources` instead.
- `backup_source` (string, optional, **deprecated**): A secondary balancer used if the primary has no backends. Used for backward compatibility; if defined, it is added to the `sources` list after `source`. Use `sources` instead.
- `addresses` (list of strings, required): Listening addresses.
- `connect_timeout` (duration string, optional): Timeout for connecting to the backend. Default: `0s`.
- `client_timeout` (duration string, optional): Idle timeout for the client connection. Default: `0s`.
- `server_timeout` (duration string, optional): Idle timeout for the backend connection. Default: `0s`.
- `close_timeout` (duration string, optional): Grace period for closing connections during shutdown. Default: `0s`.
- `timeout_margin` (duration string, optional): A small margin added to deadlines to prevent race conditions between client and server timeouts. Default: `1s`.
- `buffer_size` (number, optional): Proxy buffer size in bytes. Default: `32768`.
- `close_on_backend_removal` (boolean, optional): If `true`, client connections are closed if the backend they are connected to is removed from the balancer. Default: `false`.
- `backend_tcp_keepalive` (duration string, optional): Timeout for sending TCP keepalive probes to backend connections. Set to `0s` to disable. Default: `5s`.

---

## Redis Proxy

A Redis-protocol aware proxy using connection-level multiplexing (connection pinning). Each frontend connection is exclusively paired with a backend connection for its entire duration. This architecture supports all Redis features, including transactions, blocking commands (`BLPOP`, etc.), and PubSub.

```hcl
proxy "redis" "my_redis_proxy" {
  source = balancer.wrr.redis_master
  addresses = [":6379"]
  preconnect = 5
  idle_timeout = "10m"
  healthcheck = true
}
```

- `source` (string, required): The ID of the backend provider (inventory or processor).
- `addresses` (list of strings, required): List of TCP addresses to listen on (e.g., `[":6379"]`).
- `connect_timeout` (duration string, optional): Timeout for establishing a new connection to a backend. Default: `0s` (OS default).
- `close_timeout` (duration string, optional): Grace period for existing connections to finish after a shutdown signal. Default: `0s`.
- `backend_wait_timeout` (duration string, optional): How long to wait for a backend to become available if the balancer is empty before returning an error to the client. Default: `0s`.
- `buffer_size` (number, optional): Size of the read/write buffers for network I/O. Default: `16384`.
- `preconnect` (number, optional): The number of connections to establish to backends at startup. Default: `0`.
- `idle_timeout` (duration string, optional): How long an unused connection remains in the pool before being closed. Default: `5m`.
- `healthcheck` (boolean, optional): If `true`, the proxy sends a `PING` to verify a connection's health before handing it to a client. Default: `false`.
- `backend_tcp_keepalive` (duration string, optional): Timeout for sending TCP keepalive probes to backend connections. Set to `0s` to disable. Default: `5s`.

*Note: When a client disconnects, the proxy automatically sends a `RESET` command to the backend to clear the connection state (e.g., clearing PubSub subscriptions or discarding open transactions) before returning the connection to the pool.*

---

## Memcache Proxy

A Memcache-protocol aware proxy supporting both ASCII and Meta protocols. It uses Ketama consistent hashing to route keys to backends.

```hcl
proxy "memcache" "my_memcache_proxy" {
  source = backends_inventory.static.memcache_nodes
  addresses = [":11211"]
  flush_backend_when_added = false
}
```

- `source` (string, required): The ID of the backend provider (inventory or processor).
- `addresses` (list of strings, required): List of TCP addresses to listen on (e.g., `[":11211"]`).
- `connect_timeout` (duration string, optional): Timeout for establishing a new connection to a backend. Default: `0s`.
- `close_timeout` (duration string, optional): Grace period for closing connections during shutdown. Default: `0s`.
- `buffer_size` (number, optional): Size of the read/write buffers for network I/O. Default: `16384`.
- `client_queue_size` (number, optional): The maximum number of pipelined requests allowed per client connection. Default: `64`.
- `backend_input_queue_size` (number, optional): Size of the internal buffer for requests waiting to be sent to a backend connection. Default: `1024`.
- `backend_inflight_queue_size` (number, optional): Size of the internal buffer for tracking requests that are currently being processed by a backend. Default: `512`.
- `backend_min_connections` (number, optional): The minimum number of persistent connections to maintain for each discovered backend. Default: `1`.
- `backend_max_connections` (number, optional): The maximum number of persistent connections allowed per backend. Default: `backend_min_connections`.
- `backend_tcp_keepalive` (duration string, optional): Timeout for sending TCP keepalive probes to backend connections. Set to `0s` to disable. Default: `5s`.
- `max_fields_per_command` (number, optional): The maximum number of space-separated fields allowed in a single Memcache command line. Default: `16`.
- `flush_backend_when_added` (boolean, optional): If `true`, MLB will send a `flush_all` command to a backend immediately after it is discovered. Useful for ensuring a clean state when adding new cache nodes. Default: `false`.
