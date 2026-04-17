#pragma once
#include <string>
#include <vector>
#include <cstdint>

// Record format on disk:
// [4B payload length][4B CRC32][N bytes payload]
//
// Length: how many bytes of payload follow
// CRC32: checksum over the payload, detects corruption from partial writes
// Payload: serialized Command proto

class WAL {
    std::string filepath;
    int fd;
public:
    WAL(const std::string& filepath);
    ~WAL();

    bool append(const std::string& payload);
    std::vector<std::string> readAll();
    void truncateFrom(size_t offset);
};
