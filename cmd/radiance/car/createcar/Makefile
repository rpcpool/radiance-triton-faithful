
IPLD_SCHEMA_PATH := ledger.ipldsch

bindcode: install-deps
	ipld schema codegen \
		--generator=go-bindnode \
		--package=ipldbindcode \
		--output=./ipld/ipldbindcode \
		$(IPLD_SCHEMA_PATH)
gengo: install-deps
	ipld schema codegen \
		--generator=go-gengo \
		--package=ipldsch \
		--output=./ipld/ipldsch \
		$(IPLD_SCHEMA_PATH)
install-deps:
	go install github.com/ipld/go-ipldtool/cmd/ipld@latest
