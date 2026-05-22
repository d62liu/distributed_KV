# distributed_KV

A distributed key-value store built on Raft consensus. This is a learning project — the goal is to build two versions of the same store (CP and AP) and benchmark the consistency/availability tradeoff directly.

## Status

**CP store** — complete. **AP store** — in progress.

## CP Store

Single Raft group across 3 nodes. All writes go through the leader and require majority acknowledgment before committing. Reads use ReadIndex — the leader confirms it still holds quorum before serving — so reads are linearizable even across leader changes.

Key properties:
- Linearizable reads and writes
- Crash recovery via persisted term, vote, and append-only log
- Client request deduplication
- Unavailable when majority of nodes are down (correct CP behavior)

## Design

```
client → leader (Put/Get via gRPC)
leader → replicates to followers via AppendEntries
leader → commits on majority ack → applies to local KV store
leader → propagates commit_index to followers on next heartbeat
```

Reads go through ReadIndex rather than going through the log, avoiding a write per read while preserving linearizability.

## Building

```bash
cmake -B build && cmake --build build
```

Requires: gRPC, protobuf, spdlog (managed via vcpkg).

## Running

Start three nodes:

```bash
./build/dkv-server --id node1 --port 50051 --peers node2:50052,node3:50053
./build/dkv-server --id node2 --port 50052 --peers node1:50051,node3:50053
./build/dkv-server --id node3 --port 50053 --peers node1:50051,node2:50052
```

Interact via CLI:

```bash
./build/dkv-cli --addr localhost:50051 put foo bar
./build/dkv-cli --addr localhost:50051 get foo
```

Benchmark:

```bash
./build/dkv-bench --addr localhost:50051 --threads 4 --duration 10 --read_ratio 0.8
```

## WIP

Build an AP store (eventual consistency, leaderless writes) and run comparative benchmarks across latency, throughput, and consistency under partition.
