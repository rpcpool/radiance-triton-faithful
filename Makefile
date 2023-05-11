DEFAULT:lite

ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

install_compatible_golang_version:
	go install golang.org/dl/go1.19.7@latest
build_rocksdb:
	mkdir -p facebook ; cd facebook ; \
	git clone https://github.com/facebook/rocksdb --branch v7.10.2 --depth 1 ; \
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
	# limit concurrency to number of CPU cores (not threads)
	# helps managing memory use during build
	make -j `nproc`
lite: install_compatible_golang_version
	go build -tags=lite ./cmd/radiance
full: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go build ./cmd/radiance
radiance: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 run ./cmd/radiance $(ARGS)
test-full: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 test ./... -cover -count=1
gen-proto:
	protoc \
		--experimental_allow_proto3_optional \
		--go_out=paths=source_relative:$$(pwd)/third_party/solana_proto/confirmed_block \
		-I=$$(pwd)/third_party/solana_proto/confirmed_block/ \
		$$(pwd)/third_party/solana_proto/confirmed_block/confirmed_block.proto
