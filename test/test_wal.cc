#include <gtest/gtest.h>
#include "storage/wal.h"
#include "storage/store.h"
#include "kv_service.pb.h"
#include <fstream>
#include <cstdio>
#include <string>

static const std::string WAL_PATH = "/tmp/test_wal.wal";

static void remove_wal() {
    std::remove(WAL_PATH.c_str());
}

static std::string make_put_payload(const std::string& key, const std::string& value) {
    Command cmd;
    cmd.set_type(Command::PUT);
    cmd.set_key(key);
    cmd.set_value(value);
    std::string payload;
    cmd.SerializeToString(&payload);
    return payload;
}

static std::string make_delete_payload(const std::string& key) {
    Command cmd;
    cmd.set_type(Command::DELETE);
    cmd.set_key(key);
    std::string payload;
    cmd.SerializeToString(&payload);
    return payload;
}


TEST(WAL, AppendAndReadBack) {
    remove_wal();
    {
        WAL wal(WAL_PATH);
        ASSERT_TRUE(wal.append(make_put_payload("hello", "world")));
        ASSERT_TRUE(wal.append(make_put_payload("foo", "bar")));
    }

    WAL wal(WAL_PATH);
    auto records = wal.readAll();
    ASSERT_EQ(records.size(), 2);

    Command c1, c2;
    ASSERT_TRUE(c1.ParseFromString(records[0]));
    ASSERT_TRUE(c2.ParseFromString(records[1]));

    EXPECT_EQ(c1.type(), Command::PUT);
    EXPECT_EQ(c1.key(), "hello");
    EXPECT_EQ(c1.value(), "world");

    EXPECT_EQ(c2.type(), Command::PUT);
    EXPECT_EQ(c2.key(), "foo");
    EXPECT_EQ(c2.value(), "bar");
}

// ─────────────────────────────────────────────
// 2. CRC corruption detection
// ─────────────────────────────────────────────

TEST(WAL, CorruptedCRCStopsReplay) {
    remove_wal();
    {
        WAL wal(WAL_PATH);
        wal.append(make_put_payload("key1", "val1"));
        wal.append(make_put_payload("key2", "val2"));
    }

    // Flip a byte in the CRC of the second record
    // First record: 4B length + 4B CRC + payload
    // We need to find offset of second record's CRC
    {
        // Read first record's length to find where second record starts
        std::ifstream f(WAL_PATH, std::ios::binary);
        uint32_t len1;
        f.read(reinterpret_cast<char*>(&len1), 4);
        // Second record starts at: 4 (len) + 4 (crc) + len1
        size_t second_record_offset = 4 + 4 + len1;
        // CRC of second record is at: second_record_offset + 4
        size_t second_crc_offset = second_record_offset + 4;
        f.close();

        // Corrupt the CRC
        std::fstream rw(WAL_PATH, std::ios::binary | std::ios::in | std::ios::out);
        rw.seekp(second_crc_offset);
        char bad = 0xFF;
        rw.write(&bad, 1);
    }

    WAL wal(WAL_PATH);
    auto records = wal.readAll();
    // Only the first record should be valid
    ASSERT_EQ(records.size(), 1);
    Command c;
    ASSERT_TRUE(c.ParseFromString(records[0]));
    EXPECT_EQ(c.key(), "key1");
}

// ─────────────────────────────────────────────
// 3. Truncated record
// ─────────────────────────────────────────────

TEST(WAL, TruncatedRecordDiscarded) {
    remove_wal();
    {
        WAL wal(WAL_PATH);
        wal.append(make_put_payload("good", "record"));
    }

    // Append a partial record manually — length header only, no payload
    {
        std::ofstream f(WAL_PATH, std::ios::binary | std::ios::app);
        uint32_t fake_length = 20;
        f.write(reinterpret_cast<const char*>(&fake_length), 4);
        // Write CRC but only 5 of the 20 payload bytes — truncated
        uint32_t fake_crc = 0xDEADBEEF;
        f.write(reinterpret_cast<const char*>(&fake_crc), 4);
        f.write("hello", 5);  // only 5 bytes of promised 20
    }

    WAL wal(WAL_PATH);
    auto records = wal.readAll();
    // Only the complete record should come back
    ASSERT_EQ(records.size(), 1);
    Command c;
    ASSERT_TRUE(c.ParseFromString(records[0]));
    EXPECT_EQ(c.key(), "good");
}

// ─────────────────────────────────────────────
// 4. Recovery through Store
// ─────────────────────────────────────────────

TEST(Store, RecoverAfterRestart) {
    remove_wal();
    {
        Store store(16, WAL_PATH);
        store.put("hello", "world");
        store.put("foo", "bar");
        store.put("key1", "value1");
        store.remove("foo");
        // store goes out of scope — simulates crash/shutdown
    }

    // New Store instance — simulates restart
    Store store(16, WAL_PATH);
    store.recover();

    EXPECT_EQ(store.get("hello"), "world");
    EXPECT_EQ(store.get("key1"), "value1");
    EXPECT_EQ(store.get("foo"), "");   // was deleted
}

TEST(Store, OverwriteRecoveredCorrectly) {
    remove_wal();
    {
        Store store(16, WAL_PATH);
        store.put("key", "original");
        store.put("key", "updated");
    }

    Store store(16, WAL_PATH);
    store.recover();

    // Should have the latest value
    EXPECT_EQ(store.get("key"), "updated");
}

// ─────────────────────────────────────────────
// 5. Multiple restarts
// ─────────────────────────────────────────────

TEST(Store, MultipleRestarts) {
    remove_wal();

    // First session
    {
        Store store(16, WAL_PATH);
        store.put("a", "1");
        store.put("b", "2");
    }

    // Second session — recover, add more
    {
        Store store(16, WAL_PATH);
        store.recover();
        EXPECT_EQ(store.get("a"), "1");
        store.put("c", "3");
        store.remove("a");
    }

    // Third session — recover again
    {
        Store store(16, WAL_PATH);
        store.recover();
        EXPECT_EQ(store.get("a"), "");  // deleted in session 2
        EXPECT_EQ(store.get("b"), "2"); // survived both restarts
        EXPECT_EQ(store.get("c"), "3"); // added in session 2
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
