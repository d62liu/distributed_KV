#pragma once
#include "storage/shard_map.h"
#include "storage/wal.h"
#include <string>

// Store wraps ShardedTable + WAL.
// Write path: serialize 
// Command → append to WAL → apply to ShardedTable
// On startup: replay WAL to reconstruct ShardedTable from scratch

class Store {
    ShardedTable table;
    WAL wal;
public:
    Store(int num_shards, const std::string& wal_path);

    void put(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    void remove(const std::string& key);

    // Replay WAL into ShardedTable — called once on startup
    void recover();
};
