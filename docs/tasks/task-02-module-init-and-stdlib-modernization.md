# Task 02: Module Init & Standard Library Modernization

## Goal

Bring the Go project structure up to standard Go module conventions.

## Steps

1. Run `go mod init github.com/alexsasharegan/mysqlbak` and `go mod tidy`.
2. Update `build.sh` to use `CGO_ENABLED=0 go build -ldflags="-s -w" -o mysqlbak .`.
3. Replace `io/ioutil` with `os` and `io` (`os.ReadDir`, `io.Discard`).
4. Replace `github.com/pkg/errors` with Go standard `%w` error wrapping.
