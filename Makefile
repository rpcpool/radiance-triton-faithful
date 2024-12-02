DEFAULT:full

ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

install-deps:
	sudo apt install -y libsnappy-dev build-essential cmake zlib1g-dev libbz2-dev liblz4-dev libzstd-dev libgflags-dev
install_compatible_golang_version:
	go install golang.org/dl/go1.20.5@latest
	go1.20.5 download
build_rocksdb: install-deps
	mkdir -p facebook ; cd facebook ; \
	git clone https://github.com/facebook/rocksdb --branch v9.7.3 --depth 1 ; \
	cd ./rocksdb ; \
	mkdir -p build ; \
	make static_lib
full: install_compatible_golang_version build_rocksdb
	# replace default go tmp build dir from /tpm to ./tmp
	# TMPDIR=$$(pwd)/tmp \
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/ -l:librocksdb.a -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" \
	go1.20.5 build \
		-ldflags="-X main.GitCommit=$$(git rev-parse HEAD) -X main.GitTag=$$(git symbolic-ref -q --short HEAD || git describe --tags --exact-match)" \
		./cmd/radiance

radiance: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/librocksdb.a -lbz2" \
	go1.20.5 run ./cmd/radiance $(ARGS)
test-full: install_compatible_golang_version build_rocksdb
	CGO_CFLAGS="-I$$(pwd)/facebook/rocksdb/include" \
	CGO_LDFLAGS="-L$$(pwd)/facebook/rocksdb/build -lbz2" \
	go1.20.5 test ./... -cover -count=1
clear:
	rm -rf facebook/rocksdb
	rm -rf facebook/rocksdb/build
