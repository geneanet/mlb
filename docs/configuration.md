# General Configuration

The MLB configuration file uses the HCL (HashiCorp Configuration Language) format. This document describes the top-level configuration blocks.

## Metrics Block

The `metrics` block configures the internal HTTP server used for Prometheus metrics and the built-in dashboard.

```hcl
metrics {
  address = ":2112"
}
```

- `address` (string, required): The listening address for the metrics and dashboard server (e.g., `:2112` or `127.0.0.1:2112`).

Metrics are available at `/metrics` in Prometheus format. The dashboard is available at `/dashboard`.

## System Block

The `system` block configures system-level settings and resource limits.

```hcl
system {
  rlimit {
    nofile = 65536
  }
  gomaxprocs = 4
}
```

- `rlimit` (block, optional): Configures resource limits.
    - `nofile` (number, optional): Sets the maximum number of open file descriptors (`RLIMIT_NOFILE`).
- `gomaxprocs` (number, optional): Sets the `GOMAXPROCS` value, which limits the number of operating system threads that can execute user-level Go code simultaneously. Defaults to the number of logical CPUs.

## HCL Expressions & Functions

MLB leverages HCL's powerful expression language for filtering, weights, and dynamic configuration. You can use standard arithmetic, logic, and a set of built-in functions:

- `abs(number)`: Absolute value.
- `ceil(number)`: Smallest integer greater than or equal to number.
- `contains(list, value)`: Returns true if the list contains the value.
- `floor(number)`: Largest integer less than or equal to number.
- `int(value)`: Converts value to an integer.
- `join(separator, list)`: Joins a list of strings.
- `len(value)`: Returns the length of a list, map, or string.
- `max(numbers...)`: Returns the maximum value.
- `min(numbers...)`: Returns the minimum value.
- `parseint(string, base)`: Parses a string into an integer.
- `split(separator, string)`: Splits a string into a list.
- `strlen(string)`: Returns the length of a string.

You can also reference other modules using their IDs, e.g., `backends_inventory.static.my_servers`.
