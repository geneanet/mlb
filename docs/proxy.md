# Proxy

Proxies are the frontend of MLB. They listen on network addresses, speak specific protocols, and forward requests to backends.

## TCP Proxy

A low-level TCP proxy that forwards raw bytes.

```hcl
proxy "tcp" "my_tcp_proxy" {
  source = balancer.wrr.my_balancer
  backup_source = balancer.wrr.backup_balancer
  addresses = [":3306"]
  connect_timeout = "2s"
  close_on_backend_removal = true
}
```

- `source` (string, required): The primary balancer or backend provider.
- `backup_source` (string, optional): A secondary balancer used if the primary has no backends.
- `addresses` (list of strings, required): Listening addresses.
- `connect_timeout` (duration string, optional): Timeout for connecting to the backend. Default: `0s`.
- `client_timeout` (duration string, optional): Idle timeout for the client connection. Default: `0s`.
- `server_timeout` (duration string, optional): Idle timeout for the backend connection. Default: `0s`.
- `close_timeout` (duration string, optional): Grace period for closing connections during shutdown. Default: `0s`.
- `timeout_margin` (duration string, optional): A small margin added to deadlines to prevent race conditions between client and server timeouts. Default: `1s`.
- `buffer_size` (number, optional): Proxy buffer size in bytes. Default: `32768`.
- `close_on_backend_removal` (boolean, optional): If `true`, client connections are closed if the backend they are connected to is removed from the balancer. Default: `false`.

---

## Redis Proxy

A Redis-protocol aware proxy. It maintains a pool of connections to backends and handles commands.

```hcl
proxy "redis" "my_redis_proxy" {
  source = balancer.wrr.redis_master
  addresses = [":6379"]
  backend_min_connections = 5
  backend_max_connections = 20
}
```

- `source` (string, required): The balancer or backend provider to route traffic to.
- `addresses` (list of strings, required): List of TCP addresses to listen on (e.g., `[":6379"]`).
- `connect_timeout` (duration string, optional): Timeout for establishing a new connection to a backend. Default: `0s` (OS default).
- `close_timeout` (duration string, optional): Grace period for existing connections to finish after a shutdown signal. Default: `0s`.
- `backend_wait_timeout` (duration string, optional): How long to wait for a backend to become available if the balancer is empty before returning an error to the client. Default: `0s`.
- `buffer_size` (number, optional): Size of the read/write buffers for network I/O. Default: `16384`.
- `client_queue_size` (number, optional): The maximum number of pipelined requests allowed per client connection. Default: `64`.
- `backend_input_queue_size` (number, optional): Size of the internal buffer for requests waiting to be sent to a backend connection. Default: `1024`.
- `backend_inflight_queue_size` (number, optional): Size of the internal buffer for tracking requests that are currently being processed by a backend. Default: `512`.
- `backend_min_connections` (number, optional): The minimum number of persistent connections to maintain for each discovered backend. Default: `1`.
- `backend_max_connections` (number, optional): The maximum number of persistent connections allowed per backend. Default: `backend_min_connections`.
- `retry_period` (duration string, optional): Initial wait time before retrying a failed backend connection. Default: `100ms`.
- `retry_max_period` (duration string, optional): Maximum wait time between connection retries. Default: `1s`.
- `retry_backoff_factor` (number, optional): The factor by which the retry wait time increases after each failure. Default: `1.5`.

*Note: Some restricted commands like `SUBSCRIBE`, `CONFIG`, etc., are denied by the proxy.*

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

- `source` (string, required): The backend provider or inventory.
- `addresses` (list of strings, required): List of TCP addresses to listen on (e.g., `[":11211"]`).
- `connect_timeout` (duration string, optional): Timeout for establishing a new connection to a backend. Default: `0s`.
- `close_timeout` (duration string, optional): Grace period for closing connections during shutdown. Default: `0s`.
- `buffer_size` (number, optional): Size of the read/write buffers for network I/O. Default: `16384`.
- `client_queue_size` (number, optional): The maximum number of pipelined requests allowed per client connection. Default: `64`.
- `backend_input_queue_size` (number, optional): Size of the internal buffer for requests waiting to be sent to a backend connection. Default: `1024`.
- `backend_inflight_queue_size` (number, optional): Size of the internal buffer for tracking requests that are currently being processed by a backend. Default: `512`.
- `backend_min_connections` (number, optional): The minimum number of persistent connections to maintain for each discovered backend. Default: `1`.
- `backend_max_connections` (number, optional): The maximum number of persistent connections allowed per backend. Default: `backend_min_connections`.
- `max_fields_per_command` (number, optional): The maximum number of space-separated fields allowed in a single Memcache command line. Default: `16`.
- `flush_backend_when_added` (boolean, optional): If `true`, MLB will send a `flush_all` command to a backend immediately after it is discovered. Useful for ensuring a clean state when adding new cache nodes. Default: `false`.
