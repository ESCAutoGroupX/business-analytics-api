.PHONY: build run test test-verbose test-db clean

build:
	go build -o server ./cmd/server/

run:
	go run ./cmd/server/

# Run all tests. Tests that need a DB will skip unless TEST_DATABASE_URL is set.
# Example (local): TEST_DATABASE_URL=postgresql://autoaccount:pass@localhost:5432/autoaccount_test make test
test:
	go test ./... -v -count=1

# Alias for clarity
test-verbose: test

# Create + migrate the test database. Requires psql on PATH and a running
# Postgres with a superuser available. Override via PGUSER/PGHOST/PGPORT.
test-db:
	@echo "Creating autoaccount_test database (if not exists)..."
	@psql -v ON_ERROR_STOP=1 -c "CREATE DATABASE autoaccount_test" || true
	@echo "Done. Set TEST_DATABASE_URL and run 'make test'."

clean:
	rm -f server
