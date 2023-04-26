
IPLD_SCHEMA_PATH := ledger.ipldsch

bindcode:
	ipld schema codegen \
		--generator=go-bindnode \
		--package=ipldbindcode \
		--output=./ipld/ipldbindcode \
		$(IPLD_SCHEMA_PATH)
gengo:
	ipld schema codegen \
		--generator=go-gengo \
		--package=ipldsch \
		--output=./ipld/ipldsch \
		$(IPLD_SCHEMA_PATH)
