DEFAULT:full

ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

install-deps:
	sudo apt install -y libsnappy-dev build-essential cmake zlib1g-dev libbz2-dev liblz4-dev libzstd-dev libgflags-dev
build_rocksdb: install-deps
	mkdir -p facebook ; cd facebook ; \
	git clone https://github.com/facebook/rocksdb --branch v9.7.3 --depth 1 ; \
	cd ./rocksdb ; \
	mkdir -p build ; \
	make static_lib
full: build_rocksdb
	# replace default go tmp build dir from /tmp to ./tmp
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/ -lrocksdb -lstdc++ -lm -lbz2" \
	go build \
		-ldflags="-X main.GitCommit=$$(git rev-parse HEAD) -X main.GitTag=$$(git symbolic-ref -q --short HEAD || git describe --tags --exact-match)" \
		./cmd/radiance

radiance: build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/librocksdb.a -lbz2" \
	go run ./cmd/radiance $(ARGS)
test-full: build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go test ./... -cover -count=1
clear:
	rm -rf facebook/rocksdb
