RUST_CRATE := rust/datafusion-c-abi
RUST_STATICLIB := rust/target/release/libdatafusion_c_abi.a

.PHONY: build
build: rust-build go-build

.PHONY: rust-build
rust-build:
	cd rust && cargo build --release -p datafusion-c-abi

.PHONY: go-build
go-build: $(RUST_STATICLIB)
	go build ./...

.PHONY: test
test: rust-test go-test

.PHONY: rust-test
rust-test:
	cd rust && cargo test --release -p datafusion-c-abi

.PHONY: go-test
go-test: $(RUST_STATICLIB)
	go test ./... -race

.PHONY: lint
lint: rust-lint go-lint

.PHONY: rust-lint
rust-lint:
	cd rust && cargo fmt --check && cargo clippy --all-targets -- -D warnings

.PHONY: go-lint
go-lint:
	go vet ./...
	@if [ -n "$$(gofmt -l .)" ]; then \
		echo "gofmt needs to be run on:"; \
		gofmt -l .; \
		exit 1; \
	fi

.PHONY: format
format: rust-format go-format

.PHONY: rust-format
rust-format:
	cd rust && cargo fmt

.PHONY: go-format
go-format:
	gofmt -w .

.PHONY: run-example
run-example: $(RUST_STATICLIB)
	go run ./examples/sql

$(RUST_STATICLIB):
	$(MAKE) rust-build

.PHONY: clean
clean:
	cd rust && cargo clean
	go clean ./...
