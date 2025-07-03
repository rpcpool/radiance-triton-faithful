# radiance ☀️

Assortment of Solana protocol modules written in Go.

⚠️ **No guarantees, no support.**
Check back later unless you're ready to read and understand the code. ⚠️

[![Go Reference](https://pkg.go.dev/badge/go.firedancer.io/radiance.svg)](https://pkg.go.dev/go.firedancer.io/radiance)

[Installation Guide](./INSTALL.md)

# Create a CAR file

To create a CAR file from a Solana block, use the `radiance car create` command:

```bash
radiance car create 311 \
        --db=/media/runner/bucket/rocksdb/133626720 \
        --db=/media/runner/bucket/rocksdb/133615822 \
        --db=/media/runner/bucket/rocksdb/133608471 \
        --db=/media/runner/bucket/rocksdb/133607318 \
        --db=/media/runner/bucket/rocksdb/133599872 \
        --out=/media/runner/bucket/cars/epoch-311.car
```

# Backfill missing transaction metadata from Bigtable

```bash
# To access Bigtable, you need to set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# to point to your Google Cloud service account credentials JSON file.
# Make sure you have the necessary permissions to access Bigtable.
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/bigtable-credentials.json
radiance car create 311 \
        --db=/media/runner/bucket/rocksdb/133626720 \
        --db=/media/runner/bucket/rocksdb/133615822 \
        --db=/media/runner/bucket/rocksdb/133608471 \
        --db=/media/runner/bucket/rocksdb/133607318 \
        --db=/media/runner/bucket/rocksdb/133599872 \
        --enable-backfill=true \
        --backfill-cache-dir=/media/runner/bucket/backfill-dbs/311 \
        --out=/media/runner/bucket/cars/epoch-311.car

# if you already know the blocks where there are missing tx metadata, you can specify them:
# --backfill-preload-blocks=134352008-134352010,134352050
```
