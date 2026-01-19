# ThunderDB Development Guidelines

## 1. Build & Test Commands

### Core Build
This project uses standard Go tooling.
- **Build**: `go build ./...`
- **Install Dependencies**: `go mod download`

### Testing
Tests are written using the standard `testing` package.
- **Run all tests**: `go test ./...`
- **Run tests with verbose output**: `go test -v ./...`
- **Run a specific test**: `go test -v -run TestName ./...`
  - Example: `go test -v -run TestQuery_Basic ./...`
- **Run tests in a specific package**: `go test ./package_name`

### Linting & Formatting
- **Format code**: `go fmt ./...`
- **Lint (if installed)**: `golangci-lint run` (standard recommendation, though not explicitly configured in repo)
- **Check for race conditions**: `go test -race ./...`

## 2. Code Style & Conventions

### General Philosophy
- **Idiomatic Go**: Follow standard Go conventions (Effective Go).
- **Simplicity**: Prefer simple, readable code over complex abstractions.
- **Performance**: This is a database project; be mindful of allocations and performance in hot paths (like query execution).

### Formatting & Imports
- Use `gofmt` (or `goimports`) for all file formatting.
- Group imports: standard library first, then third-party, then internal.
- Do not commit commented-out code unless it serves a specific documentation purpose (and is labeled as such).

### Naming Conventions
- **Files**: Snake case (e.g., `query_node.go`, `metadata.go`).
- **Types/Interfaces**: CamelCase. Exported types start with uppercase (e.g., `QueryNode`, `Metadata`).
- **Variables/Functions**: CamelCase. Private helpers start with lowercase.
- **Receivers**: Use short, 1-2 letter names matching the type (e.g., `n` for `Node`, `db` for `DB`).
- **Errors**: Prefix with `Err` (e.g., `ErrFieldNotFound`).

### Error Handling
- Return errors as the last return value.
- Use custom error types where specific handling is needed (e.g., `ThunderError`).
- Wrap errors with context when helpful, but avoid excessive wrapping in tight loops.
- **Check errors**: Always check returned errors. Do not ignore them (use `_` only if absolutely safe).

### Types & Data Structures
- **Generics**: Used selectively (e.g., `iter.Seq2`).
- **Iterators**: The project uses Go 1.23+ iterators (`iter.Seq2[*Row, error]`) for query results.
- **Maps**: heavily used for metadata and row values. Be careful with concurrent access (not safe by default).

### Project Structure (Inferred)
- **`thunderdb` package**: Core logic resides here.
- **Storage**: `store.go`, `bolt_store.go` (likely underlying storage).
- **Query Engine**: `query.go` (nodes, join logic, propagation).
- **Metadata**: `metadata.go`.
- **Transactions**: `transaction.go`.

### CI/CD (GitHub Actions)
- **Release**: `.github/workflows/release.yml` handles releases via GoReleaser on tag push.
- **Go Version**: 1.25.x (defined in `go.mod`).

## 3. Specific Logic Notes
- **Query Execution**: Uses a node-based architecture (`headQueryNode`, `joinedQueryNode`, `projectedQueryNode`).
- **Joins**: Implemented via nested loop or index lookups, propagating constraints up/down the tree.
- **Indexing**: `Metadata` struct tracks available indexes via a `map[uint64]bool`.
- **Values**: Values are often raw bytes or wrapped types; `ToKey` conversion is common.

## 4. Agent Operational Rules
- **Safety**: Always check for file existence before reading/writing.
- **Context**: Read related tests (`*_test.go`) before modifying core logic to understand usage.
- **Verification**: ALWAYS run related tests after modifying code (`go test -run RelatedTest`).
- **Cleanliness**: Run `go fmt` on modified files.
