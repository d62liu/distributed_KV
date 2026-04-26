#pragma once
#include "storage/shard_map.h"
#include "storage/wal.h"
#include <string>

class Store {
    ShardedTable table;
    WAL wal;
public:
    Store(int num_shards, const std::string& wal_path);
    void put(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    void remove(const std::string& key);
    void recover();
};
