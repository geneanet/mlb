# Operations Guide

This document covers running MLB in production, including installation, CLI arguments, and zero-downtime restarts.

## Installation

### Building from Source

To build MLB from source, you need Go 1.26+:

```bash
go build -o mlb .
```

### Docker

You can also use the provided Dockerfile to build an image:

```bash
docker build -t mlb .
```

## CLI Arguments

- `-config <path>`: Path to the HCL configuration file. Default: `config.hcl`.
- `-configtest`: Checks the configuration syntax and schema for errors without starting the load balancer. Returns exit code 0 if valid, 1 otherwise. Uses the path from `-config`.
- `-debug`: Enables debug logging level.
- `-process-manager`: Enables the process manager mode for zero-downtime restarts.
- `-notify-parent`: Internal flag used by worker processes to signal the process manager they are ready.

## Zero-Downtime Restarts

MLB includes a built-in process manager that allows you to reload your configuration without dropping active connections.

### How it works

1.  Start MLB with the `-process-manager` flag.
2.  The process manager starts an initial worker process.
3.  When the worker is ready (all modules started), it notifies the parent.
4.  To reload, send a `SIGHUP` signal to the process manager.
5.  The process manager starts a *new* worker process with the updated configuration.
6.  The new worker opens its own listening ports using `SO_REUSEPORT`, allowing it to share the port with the old worker.
7.  Once the new worker is ready, it notifies the process manager.
8.  The process manager then sends a `SIGTERM` to the old worker.
9.  The old worker stops accepting new connections and waits for existing ones to finish (honoring `close_timeout`) before exiting.

### Signals

Sent to the **Process Manager**:
- `SIGHUP`: Trigger a zero-downtime restart (reload config).
- `SIGINT` / `SIGTERM`: Gracefully shut down the process manager and all workers.

Sent to a **Worker** (directly):
- `SIGINT` / `SIGTERM`: Gracefully shut down the worker.
- `SIGUSR1`: Used internally by the process manager (don't send this manually).

## Resource Limits

In high-traffic environments, you should ensure that MLB has enough file descriptors. Use the `system` block in your configuration to adjust `nofile`.

```hcl
system {
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
- **Port Conflict**: If you receive a "bind: address already in use" error when starting a new worker (during `SIGHUP`), ensure that `SO_REUSEPORT` is supported by your operating system and that no other application is using the same port without `SO_REUSEPORT`.
