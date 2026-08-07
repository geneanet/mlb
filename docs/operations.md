# Operations Guide

This document covers running MLB in production, including installation, CLI arguments, and zero-downtime restarts.

## Installation

### Building from Source

To build MLB from source, you need Go 1.26+:

```bash
# Basic build (version will be "dev")
go build -o mlb .

# Build with version information
go build -ldflags "-X main.Version=$(git describe --tags --always --dirty)" -o mlb .
```

### Docker

You can also use the provided Dockerfile to build an image:

```bash
docker build -t mlb .
```

## CLI Arguments

- `-config <path>`: Path to the HCL configuration file. Default: `config.hcl`.
- `-configtest`: Checks the configuration syntax and schema for errors without starting the load balancer. Returns exit code 0 if valid, 1 otherwise. Uses the path from `-config`.
- `-version`: Displays the application version and exit.
- `-debug`: Enables debug logging level.

## Zero-Downtime Restarts

MLB uses the `tableflip` library to handle zero-downtime restarts. This allows you to reload your configuration or upgrade the MLB binary without dropping active connections.

### How it works

1.  Start MLB normally.
2.  To reload the configuration or upgrade the binary, send a `SIGHUP` signal to the MLB process.
3.  MLB will spawn a new instance of itself.
4.  The new instance inherits the listening sockets from the old instance.
5.  Once the new instance is ready and has started all modules, it signals the old instance.
6.  The old instance stops accepting new connections and waits for existing ones to finish (honoring `close_timeout`) before exiting.

### Signals

- `SIGHUP`: Trigger a zero-downtime restart (reload config and/or upgrade binary).
- `SIGINT` / `SIGTERM`: Gracefully shut down MLB and all active connections.

## Configuration

You can use the `system` block to adjust resource limits (like the maximum number of open files) and specify a PID file to help coordinate restarts:

```hcl
system {
  pid_file = "/var/run/mlb.pid"
  rlimit {
    nofile = 100000
  }
}
```

## Troubleshooting & Profiling

### Debug Logs

Enable verbose logging with the `-debug` flag to see detailed module instantiation, backend discovery updates, and proxying decisions.

### Performance Profiling (pprof)

MLB includes Go's `pprof` tool for performance analysis. It is automatically enabled on the same address as the metrics and dashboard server.

You can access profiling data using the `go tool pprof` command:

```bash
# CPU Profile
go tool pprof http://localhost:2112/debug/pprof/profile?seconds=30

# Heap Profile
go tool pprof http://localhost:2112/debug/pprof/heap
```

Common endpoints:
- `/debug/pprof/`: Index page.
- `/debug/pprof/profile`: CPU profile.
- `/debug/pprof/heap`: Memory allocation profile.
- `/debug/pprof/goroutine`: Stack traces of all current goroutines.

### Common Issues

- **"no backend found"**: This usually means your filter condition is too restrictive or the upstream inventory (like Consul) is not returning any healthy instances. Check the dashboard to see which backends are being discovered and their current metadata.
