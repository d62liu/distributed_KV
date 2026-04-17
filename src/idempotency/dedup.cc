#include <unordered_map>
#include <shared_mutex>
#include <chrono>
#include <string>
#include <optional>

struct CachedResult {
    std::string cached_value;
    bool success;
    std::chrono::steady_clock::time_point inserted_at;
};

class DedupTable {
    std::shared_mutex mu;
    std::unordered_map<std::string, CachedResult> table;

    static constexpr auto TTL = std::chrono::minutes(5);

public:
    std::optional<CachedResult> lookup(const std::string& request_id) {
        std::shared_lock<std::shared_mutex> lock(mu);
        auto it = table.find(request_id);
        if (it != table.end()) return it->second;
        return std::nullopt;
    }

    void store(const std::string& request_id, const std::string& value, bool success) {
        std::unique_lock<std::shared_mutex> lock(mu);
        table[request_id] = CachedResult{value, success, std::chrono::steady_clock::now()};
    }

    void sweep() {
        auto now = std::chrono::steady_clock::now();
        std::unique_lock<std::shared_mutex> lock(mu);
        for (auto it = table.begin(); it != table.end();) {
            if (now - it->second.inserted_at > TTL) {
                it = table.erase(it);
            } else {
                ++it;
            }
        }
    }
};
