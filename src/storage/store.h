#pragma once
#include "storage/shard_map.h"
#include "storage/wal.h"
#include "idempotency/dedup.h"
#include <string>
#include <optional>

class Store {
    ShardedTable table;
    WAL wal;
    DedupTable dedup;
public:
    Store(int num_shards, const std::string& wal_path);

    void put(const std::string& key, const std::string& value, const std::string& request_id);
    std::optional<std::string> get(const std::string& key);
    void remove(const std::string& key, const std::string& request_id);

    void recover();
};
