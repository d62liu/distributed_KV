#include "storage/wal.h"
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>

static uint32_t crc32_table[256];
static bool crc32_table_initialized = false;

static void init_crc32_table() {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
        }
        crc32_table[i] = crc;
    }
    crc32_table_initialized = true;
}

static uint32_t compute_crc32(const char* data, size_t len) {
    if (!crc32_table_initialized) init_crc32_table();
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32_table[(crc ^ data[i]) & 0xFF];
    }
    return crc ^ 0xFFFFFFFF;
}

WAL::WAL(const std::string& filepath) : filepath(filepath), fd(-1) {
    fd = open(filepath.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        spdlog::error("WAL: failed to open {}", filepath);
    }
}

WAL::~WAL() {
    if (fd >= 0) close(fd);
}

bool WAL::append(const std::string& payload) {
    uint32_t length = static_cast<uint32_t>(payload.size());
    uint32_t crc = compute_crc32(payload.data(), payload.size());

    if (::write(fd, &length, 4) != 4) return false;
    if (::write(fd, &crc, 4) != 4) return false;
    if (::write(fd, payload.data(), length) != static_cast<ssize_t>(length)) return false;

    fsync(fd);
    return true;
}

std::vector<std::string> WAL::readAll() {
    std::vector<std::string> records;
    int read_fd = open(filepath.c_str(), O_RDONLY);
    if (read_fd < 0) {
        spdlog::info("WAL: no log file at {}, starting fresh", filepath);
        return records;
    }

    while (true) {
        uint32_t length = 0;
        if (::read(read_fd, &length, 4) != 4) break;

        uint32_t stored_crc = 0;
        if (::read(read_fd, &stored_crc, 4) != 4) break;

        std::string payload(length, '\0');
        if (::read(read_fd, payload.data(), length) != static_cast<ssize_t>(length)) {
            spdlog::warn("WAL: truncated record at end of log, discarding");
            break;
        }

        uint32_t computed_crc = compute_crc32(payload.data(), payload.size());
        if (computed_crc != stored_crc) {
            spdlog::warn("WAL: CRC mismatch, stopping replay (corrupted tail)");
            break;
        }

        records.push_back(std::move(payload));
    }

    close(read_fd);
    spdlog::info("WAL: replayed {} records from {}", records.size(), filepath);
    return records;
}

void WAL::truncateFrom(size_t offset) {
    ftruncate(fd, offset);
    lseek(fd, 0, SEEK_END);
}
