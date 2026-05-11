#include "storage/store.h"
#include "kv_service.pb.h"
#include <spdlog/spdlog.h>

Store::Store(int num_shards, const std::string& wal_path)
    : table(num_shards), wal(wal_path) {}

void Store::put(const std::string& key, const std::string& value, const std::string& request_id) {
    if (dedup.lookup(request_id)) return;

    Command cmd;
    cmd.set_type(Command::PUT);
    cmd.set_key(key);
    cmd.set_value(value);
    cmd.set_request_id(request_id);

    std::string payload;
    cmd.SerializeToString(&payload);
    wal.append(payload);
    table.put(key, value);
    dedup.store(request_id, value, true);
}

std::optional<std::string> Store::get(const std::string& key) {
    return table.get(key);
}

void Store::remove(const std::string& key, const std::string& request_id) {
    if (dedup.lookup(request_id)) return;

    Command cmd;
    cmd.set_type(Command::DELETE);
    cmd.set_key(key);
    cmd.set_request_id(request_id);

    std::string payload;
    cmd.SerializeToString(&payload);
    wal.append(payload);
    table.remove(key);
    dedup.store(request_id, "", true);
}

void Store::recover() {
    std::vector<std::string> records = wal.readAll();
    for (const auto& payload : records) {
        Command cmd;
        if (!cmd.ParseFromString(payload)) {
            spdlog::warn("Store: failed to parse WAL record, skipping");
            continue;
        }
        switch (cmd.type()) {
            case Command::PUT:
                table.put(cmd.key(), cmd.value());
                dedup.store(cmd.request_id(), cmd.value(), true);
                break;
            case Command::DELETE:
                table.remove(cmd.key());
                dedup.store(cmd.request_id(), "", true);
                break;
            default:
                spdlog::warn("Store: unknown command type {}", static_cast<int>(cmd.type()));
        }
    }
    spdlog::info("Store: recovery complete, applied {} records", records.size());
}
