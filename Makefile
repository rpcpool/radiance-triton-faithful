DEFAULT:lite

ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

install-deps:
	sudo apt install -y libsnappy-dev build-essential cmake zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
install_compatible_golang_version:
	go install golang.org/dl/go1.19.7@latest
	go1.19.7 download
build_rocksdb: install-deps
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
	make -j
lite: install_compatible_golang_version
	go1.19.7 build -tags=lite ./cmd/radiance
full: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 build ./cmd/radiance

	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 build ./cmd/radiance/car/dump2
radiance: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 run ./cmd/radiance $(ARGS)
test-full: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.19.7 test ./... -cover -count=1
install-protoc:
	@echo "Installing protoc..."
	@mkdir -p $$(pwd)/third_party/protoc
	@echo "Getting the latest release of protoc from github.com/protocolbuffers/protobuf..."
	@cd $$(pwd)/third_party/protoc && \
		wget https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-linux-x86_64.zip
	@echo "Unzipping protoc..."
	@cd $$(pwd)/third_party/protoc && \
		unzip protoc-23.1-linux-x86_64.zip
	@echo "Installing protoc..."
	@cd $$(pwd)/third_party/protoc && \
		sudo cp -r bin/* /usr/local/bin/ && \
		sudo cp -r include/* /usr/local/include/
	@echo "Installing protoc-gen-go..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
gen-proto: install-protoc
	@echo "Generating proto files..."
	protoc \
		--experimental_allow_proto3_optional \
		--go_out=paths=source_relative:$$(pwd)/third_party/solana_proto/confirmed_block \
		-I=$$(pwd)/third_party/solana_proto/confirmed_block/ \
		$$(pwd)/third_party/solana_proto/confirmed_block/confirmed_block.proto
