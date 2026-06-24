# Dashboard & Metrics

MLB provides built-in observability features to help you monitor and understand your traffic and configuration.

## Prometheus Metrics

MLB exports a variety of metrics in Prometheus format. If configured via the [`metrics` block](configuration.md#metrics-block), they are available at `/metrics`.

### Key Metrics

- `mlb_frontend_connections_processed`: Total number of client connections handled.
- `mlb_frontend_active_connections`: Number of currently active client connections.
- `mlb_frontend_requests_total`: Total number of requests processed (for Redis and Memcache).
- `mlb_frontend_bytes_in` / `mlb_frontend_bytes_out`: Network throughput at the frontend.
- `mlb_backend_connections_processed`: Total number of connections made to backends.
- `mlb_backend_active_connections`: Current active connections to backends.
- `mlb_connection_errors`: Counter for various connection and proxy errors.

All metrics are labeled with `proxy` (the ID of the proxy module) and `address` (frontend or backend address).

## Built-in Dashboard

MLB includes a web-based dashboard that provides:

1.  **Topology View:** A visual representation of your MLB configuration, showing how inventories, processors, balancers, and proxies are connected.
2.  **Backends Status:** A real-time view of all discovered backends, their health status, and metadata.
3.  **Live Metrics:** Basic traffic statistics for each configured proxy.

The dashboard is available at `/dashboard` (or simply `/` which redirects to the dashboard) on the address configured in the `metrics` block.

### API Endpoints

The dashboard server also provides several JSON API endpoints:

- `/backends`: Returns the list of all backends and their metadata.
- `/topology`: Returns the module graph and their HCL configuration.
- `/proxy_metrics`: Returns aggregated metrics for each proxy.
