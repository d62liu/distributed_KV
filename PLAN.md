# Distributed Key-Value Store — Implementation Plan

## Overview

A distributed KV store in C++ targeting quant firm interviews. Nodes run as local processes on different ports, communicating via gRPC. You'll build up from a single-node store to a fully replicated cluster with failure injection.

**What you'll demonstrate:** consistent hashing, Raft consensus, idempotency, WAL crash recovery, fine-grained concurrency (sharded hash map + per-shard RWLocks), and latency benchmarking (P50/P99/P999).

**Stack:** C++, gRPC, protobuf, CMake, vcpkg

**Architecture:** Single Raft group — all nodes replicate all data. Consistent hashing sits on top as a client-side routing layer (prepares for multi-Raft later). No manager process. Nodes discover each other via a static YAML config file.

---

## Feature List

| Feature | Why |
|---------|-----|
| Consistent hashing (hash ring + 150 virtual nodes per node) | Even key distribution, minimal key movement on node change |
| Idempotency (request_id dedup table with TTL) | Safe client retries without double-applies |
| Raft consensus (leader election + log replication) | Linearizable reads/writes across replicas |
| WAL + crash recovery (binary log with CRC32) | Writes survive SIGKILL; corruption detection |
| Fine-grained concurrency (16-shard hash map, per-shard `std::shared_mutex`) | Concurrent reads during apply without global lock |
| Latency benchmarking (P50/P99/P999, throughput) | Quantify performance characteristics |
| Docker Compose (final phase) | Failure simulation, network partitions, latency injection |

---

## Directory Structure

```
dkv/
├── CMakeLists.txt
├── proto/
│   ├── kv_service.proto            # Client-facing: Put, Get, Delete
│   ├── raft_service.proto          # Internal: AppendEntries, RequestVote
│   └── admin_service.proto         # Health/status
├── src/
│   ├── main.cpp
│   ├── config/
│   │   ├── cluster_config.h/.cpp   # Reads YAML node list
│   │   └── config.yaml
│   ├── storage/
│   │   ├── shard_map.h/.cpp        # Sharded hash map with per-shard RWLocks
│   │   ├── wal.h/.cpp              # Write-ahead log (append, CRC, replay)
│   │   └── store.h/.cpp            # Wraps shard_map + WAL + idempotency
│   ├── raft/
│   │   ├── raft_node.h/.cpp        # Core Raft state machine
│   │   ├── raft_log.h/.cpp         # In-memory log + WAL persistence
│   │   ├── raft_service_impl.h/.cpp
│   │   └── raft_client.h/.cpp      # gRPC stubs to peers
│   ├── server/
│   │   ├── kv_service_impl.h/.cpp
│   │   └── admin_service_impl.h/.cpp
│   ├── routing/
│   │   ├── hash_ring.h/.cpp        # Consistent hashing with virtual nodes
│   │   └── router.h/.cpp
│   ├── idempotency/
│   │   └── dedup_table.h/.cpp      # request_id -> cached result, with TTL
│   └── util/
│       ├── timer.h/.cpp            # Resettable election/heartbeat timer
│       └── logging.h
├── client/
│   ├── dkv_client.h/.cpp           # Smart client with routing + retry
│   └── cli.cpp                     # dkv-cli put/get/delete
├── bench/
│   └── benchmark.cpp               # P50/P99/P999 latency, throughput
├── test/
│   ├── test_shard_map.cpp
│   ├── test_hash_ring.cpp
│   ├── test_wal.cpp
│   ├── test_raft.cpp
│   └── test_idempotency.cpp
└── docker/
    ├── Dockerfile
    └── docker-compose.yml
```

---

## Protobuf Service Definitions

### kv_service.proto

```protobuf
service KVService {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}

message PutRequest {
  string key = 1;
  string value = 2;
  string request_id = 3;  // Client-generated UUID for idempotency
}

message PutResponse {
  bool success = 1;
  string leader_hint = 2;  // Redirect if this node isn't leader
}

message GetRequest {
  string key = 1;
}

message GetResponse {
  bool found = 1;
  string value = 2;
  string leader_hint = 3;
}

message DeleteRequest {
  string key = 1;
  string request_id = 2;
}

message DeleteResponse {
  bool success = 1;
  string leader_hint = 2;
}
```

### raft_service.proto

```protobuf
service RaftService {
  rpc RequestVote(VoteRequest) returns (VoteResponse);
  rpc AppendEntries(AppendRequest) returns (AppendResponse);
}

message VoteRequest {
  uint64 term = 1;
  string candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
}

message VoteResponse {
  uint64 term = 1;
  bool vote_granted = 2;
}

message LogEntry {
  uint64 term = 1;
  uint64 index = 2;
  bytes command = 3;  // Serialized Command proto
}

message AppendRequest {
  uint64 term = 1;
  string leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntry entries = 5;
  uint64 leader_commit = 6;
}

message AppendResponse {
  uint64 term = 1;
  bool success = 2;
}
```

### Internal Command (WAL payload)

```protobuf
message Command {
  enum Type { PUT = 0; DELETE = 1; }
  Type type = 1;
  string key = 2;
  string value = 3;
  string request_id = 4;
}
```

---

## Phased Build Order

### Phase 1: Single-Node KV Store + gRPC (~2 days)

**Goal:** One node, in-memory, accessible via gRPC CLI.

**Build:**
- `CMakeLists.txt` with gRPC/protobuf codegen
- `proto/kv_service.proto` — Put, Get, Delete RPCs
- `src/storage/shard_map.h/.cpp` — 16 shards, each with `std::shared_mutex`. Shard selection: `std::hash(key) % 16`. Get takes read lock, Put/Delete takes write lock
- `src/server/kv_service_impl.h/.cpp` — receives gRPC call, delegates to shard_map
- `src/main.cpp` — starts gRPC server on port from argv
- `client/cli.cpp` — `dkv-cli --addr localhost:50051 put mykey myvalue`

**Test:**
- Run one node, Put/Get/Delete via CLI
- Unit test shard_map with concurrent reader/writer threads

**Libraries to install (vcpkg):** gRPC, protobuf, spdlog, yaml-cpp, gtest

---

### Phase 2: Write-Ahead Log + Crash Recovery (~3 days)

**Goal:** Writes survive SIGKILL.

**Build:**
- `src/storage/wal.h/.cpp` — append-only binary log
  - Record format: `[4B length][4B CRC32][payload]`
  - Operations: `Append` (with fsync), `ReadAll` (for replay), `TruncateFrom` (you'll need this for Raft later)
- `src/storage/store.h/.cpp` — wraps shard_map + WAL
  - Write path: append to WAL -> apply to shard_map
  - On startup: replay WAL into empty shard_map
- WAL entry payload = serialized protobuf `Command` (PUT|DELETE, key, value, request_id)

**Test:**
- Write 1000 keys, SIGKILL the process, restart, verify all keys present
- Write a corrupted record, verify CRC detection catches it

---

### Phase 3: Idempotency (~1 day)

**Goal:** Retried writes with the same `request_id` aren't applied twice.

**Build:**
- `src/idempotency/dedup_table.h/.cpp`
  - `unordered_map<string, CachedResult>` protected by mutex
  - Entries expire after 5 min TTL; background sweep every 60s
- Write path becomes: check dedup -> if hit, return cached result -> else apply (WAL + shard_map) -> store in dedup table
- Client generates a UUID per request, reuses it on retry
- `request_id` is stored in WAL entries, so the dedup table rebuilds on crash recovery

**Test:**
- Send the same Put twice with the same `request_id`, verify only applied once
- Restart, verify dedup table rebuilt from WAL

---

### Phase 4: Raft Consensus (~10-14 days)

The hardest phase. Break it into three sub-phases.

#### Phase 4a: Leader Election (~3 days)

- `src/config/cluster_config.h/.cpp` — reads YAML listing node IDs + addresses
- `src/raft/raft_node.h/.cpp` — Raft state machine: follower/candidate/leader
  - Election timeout: random in [150ms, 300ms]
  - **Single mutex protects all Raft state** — keeps concurrency bugs at bay
- `src/util/timer.h/.cpp` — resettable timer via `std::condition_variable`
- `src/raft/raft_service_impl.h/.cpp` — handles RequestVote + AppendEntries (heartbeat only for now)
- `src/raft/raft_client.h/.cpp` — gRPC stubs to send RPCs to peers
- **Must persist `current_term` and `voted_for` to disk (fsync)** — required for Raft correctness

**Test:**
- Start 3 nodes. One becomes leader within a few hundred ms
- Kill leader -> new election succeeds
- Old leader rejoins as follower

#### Phase 4b: Log Replication (~5 days)

- `src/raft/raft_log.h/.cpp` — in-memory log vector backed by WAL. Supports append, truncate, index lookup
- Leader maintains `next_index[]` and `match_index[]` per follower
- If node isn't leader, `kv_service_impl` returns `leader_hint` for client redirect

**Replicated write path:**
```
Client -> Put(key, value, request_id)
  -> leader checks dedup_table (fast path)
  -> raft_node.Propose(command)
    -> append to leader log
    -> AppendEntries to followers
    -> majority ack -> commit
  -> apply to store (shard_map)
  -> record in dedup_table
  -> return to client
```

**Test:**
- Put via leader, read from any node
- Kill a follower, write more keys, bring it back — verify it catches up
- Kill leader mid-write — verify new leader handles it

#### Phase 4c: Raft Crash Recovery (~2 days)

- On startup, replay WAL to reconstruct: Raft log, persistent state (term, voted_for), state machine (committed entries), dedup table
- Persist `commit_index` to avoid re-deriving it

**Test:**
- Write 1000 keys across 3 nodes. SIGKILL all 3. Restart all 3. Verify all data present and consistent

---

### Phase 5: Consistent Hashing + Smart Client (~3 days)

**Goal:** Hash ring for key routing. With a single Raft group this is a client-side routing layer — it prepares for multi-Raft later.

**Build:**
- `src/routing/hash_ring.h/.cpp`
  - `std::map<uint64_t, string>` mapping ring positions to node IDs
  - Each node gets 150 virtual nodes
  - Hash function: `SHA256(node_id + "#" + i)` truncated to uint64
  - `GetNode(key)` -> find first ring position >= hash(key)
- `client/dkv_client.h/.cpp` — smart client
  - Builds local hash ring from config
  - Routes Gets to hashed node, Puts to known leader
  - Follows `leader_hint` redirects
  - Retry with exponential backoff

**Test:**
- Verify even key distribution across nodes
- Remove a node from the ring, verify only ~1/N keys move

---

### Phase 6: Benchmark Tool (~2 days)

**Build:**
- `bench/benchmark.cpp`
  - Spawns W worker threads, each with its own gRPC channel
  - Each worker: random key from configurable key space, Put or Get based on read/write ratio, record latency via `high_resolution_clock`
  - Collect all latencies -> sort -> compute P50, P99, P999, mean, throughput (ops/sec)
- Usage: `dkv-bench --addr localhost:50051 --threads 8 --duration 30 --write-ratio 0.5 --key-space 100000`

**Test:**
- Run against 3-node cluster
- Compare single-node vs replicated latency numbers

---

### Phase 7: Docker Compose + Failure Simulation (~2 days)

**Build:**
- `docker/Dockerfile` — multi-stage build (compile in build image, copy binary to slim runtime image)
- `docker/docker-compose.yml` — 3 nodes on a bridge network
- Failure scripts:
  - `docker stop node1` — crash simulation
  - `docker network disconnect dkv-net node1` — network partition
  - `tc qdisc add dev eth0 root netem delay 200ms` — latency injection

**Test:**
- Run benchmark while injecting failures
- Observe recovery time and data consistency

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Single Raft group** (all nodes replicate all data) | Simpler than multi-Raft. Multi-Raft (one group per shard) is a stretch goal |
| **No manager process** (static YAML config) | Avoids SPOF and membership protocol complexity |
| **Single mutex for all Raft state** | gRPC handlers and timer thread share one lock — avoids subtle concurrency bugs. Fine-grained locking only in shard_map (where concurrent reads matter) |
| **Linearizable reads** | All reads go to leader, who confirms leadership via heartbeat round before responding |
| **Binary WAL with CRC32** (not raw protobuf) | Gives corruption detection on replay |
| **Idempotency in the Raft write path** | Dedup checked at leader proposal time AND during state machine apply |

---

## Verification Checklist (After Each Phase)

1. Run unit tests (`ctest` or `./test_*`)
2. Start a 3-node cluster locally, do manual Put/Get via CLI
3. Kill nodes with SIGKILL, restart, verify data integrity
4. Phase 6: run `dkv-bench` and record P50/P99/P999 numbers
5. Phase 7: run benchmark during failure injection, verify recovery

---

## References

- **Raft paper** (Ongaro & Ousterhout, 2014) — Figure 2 is your implementation spec
- **Dynamo paper** (DeCandia et al., 2007) — consistent hashing and virtual nodes
- **SWIM paper** (Das et al., 2002) — reference if you extend to dynamic membership later
