# MLB Project Instructions

## Code Style & Clarity
- **Clarity First**: Ensure code is clear and well-documented. A developer new to the project must be able to understand it easily.
- **Commenting**: Use idiomatic Go comments for exported functions, types, and packages. Explain complex logic with inline comments.

## Testing
- **Standard Library Only**: Use the standard `testing` package with explicit `if` checks and `t.Errorf`/`t.Fatalf`.
- **Simplicity**: Keep tests simple, readable, and follow idiomatic Go patterns.

## Linting & Verification
- **Run Linters**: Use `golangci-lint` if available; otherwise, use `go vet`.

## Documentation & Configuration
- **Synchronize**: Update documentation (`docs/`) and example files (`config.example.hcl`) whenever a configuration parameter is added, removed, or modified.
