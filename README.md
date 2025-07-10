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
