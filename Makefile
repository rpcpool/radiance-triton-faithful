DEFAULT:full

ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

install-deps:
	sudo apt install -y libsnappy-dev build-essential cmake zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
install_compatible_golang_version:
	go install golang.org/dl/go1.19.7@latest
	go1.19.7 download
build_rocksdb: install-deps
	mkdir -p facebook ; cd facebook ; \
	git clone https://github.com/facebook/rocksdb --branch v8.3.2 --depth 1 ; \
	cd ./rocksdb ; \
	mkdir -p build && cd build ; \
	cmake .. \
		-DCMAKE_BUILD_TYPE=Release \
		-DROCKSDB_BUILD_SHARED=OFF \
		-DWITH_GFLAGS=OFF \
		-DWITH_BZ2=ON \
		-DWITH_SNAPPY=OFF \
		-DWITH_ZLIB=ON \
		-DWITH_ZSTD=ON \
		-DWITH_ALL_TESTS=OFF \
		-DWITH_BENCHMARK_TOOLS=OFF \
		-DWITH_CORE_TOOLS=OFF \
		-DWITH_RUNTIME_DEBUG=OFF \
		-DWITH_TESTS=OFF \
		-DWITH_TOOLS=OFF \
		-DWITH_TRACE_TOOLS=OFF ; \
	make -j
full: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 build \
		-ldflags="-X main.GitCommit=$$(git rev-parse HEAD) -X main.GitTag=$$(git symbolic-ref -q --short HEAD || git describe --tags --exact-match)" \
		./cmd/radiance

	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 build \
		-ldflags="-X main.GitCommit=$$(git rev-parse HEAD) -X main.GitTag=$$(git symbolic-ref -q --short HEAD || git describe --tags --exact-match)" \
		./cmd/radiance/car/dump2
radiance: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 run ./cmd/radiance $(ARGS)
test-full: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 test ./... -cover -count=1
