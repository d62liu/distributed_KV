#include <grpcpp/grpcpp.h>
#include "kv_service.grpc.pb.h"
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

struct Stats {
    std::vector<int64_t> latencies_us;
    uint64_t errors = 0;
};

void worker(const std::string& addr, int thread_id, int duration_secs,
            double read_ratio, int num_keys, Stats& stats) {
    auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = KVService::NewStub(channel);

    std::mt19937 rng(thread_id);
    std::uniform_int_distribution<int> key_dist(0, num_keys - 1);
    std::uniform_real_distribution<double> op_dist(0.0, 1.0);

    uint64_t req_counter = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(duration_secs);

    while (std::chrono::steady_clock::now() < deadline) {
        std::string key = "key_" + std::to_string(key_dist(rng));
        bool is_read = op_dist(rng) < read_ratio;

        auto t0 = std::chrono::high_resolution_clock::now();
        grpc::Status status;

        if (is_read) {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            GetRequest req;
            GetResponse resp;
            req.set_key(key);
            status = stub->Get(&ctx, req, &resp);
        } else {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            PutRequest req;
            PutResponse resp;
            req.set_key(key);
            req.set_value("val_" + key);
            req.set_request_id("t" + std::to_string(thread_id) + "_" + std::to_string(req_counter++));
            status = stub->Put(&ctx, req, &resp);
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

        if (status.ok()) {
            stats.latencies_us.push_back(us);
        } else {
            stats.errors++;
        }
    }
}

int main(int argc, char* argv[]) {
    std::string addr = "localhost:50051";
    int threads = 8;
    int duration = 30;
    double read_ratio = 0.5;
    int num_keys = 10000;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--addr" && i + 1 < argc) addr = argv[++i];
        else if (arg == "--threads" && i + 1 < argc) threads = std::stoi(argv[++i]);
        else if (arg == "--duration" && i + 1 < argc) duration = std::stoi(argv[++i]);
        else if (arg == "--read-ratio" && i + 1 < argc) read_ratio = std::stod(argv[++i]);
        else if (arg == "--num-keys" && i + 1 < argc) num_keys = std::stoi(argv[++i]);
    }

    std::cout << "addr=" << addr
              << "  threads=" << threads
              << "  duration=" << duration << "s"
              << "  read_ratio=" << read_ratio
              << "  num_keys=" << num_keys << "\n";

    // Warmup: pre-populate all keys so reads aren't hitting empty slots
    {
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        auto stub = KVService::NewStub(channel);
        std::cout << "Warming up (" << num_keys << " keys)...\n";
        for (int i = 0; i < num_keys; ++i) {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            PutRequest req;
            PutResponse resp;
            req.set_key("key_" + std::to_string(i));
            req.set_value("init_" + std::to_string(i));
            req.set_request_id("warmup_" + std::to_string(i));
            stub->Put(&ctx, req, &resp);
        }
        std::cout << "Warmup done.\n\n";
    }

    std::vector<Stats> stats(threads);
    std::vector<std::thread> worker_threads;

    auto bench_start = std::chrono::steady_clock::now();
    for (int t = 0; t < threads; ++t)
        worker_threads.emplace_back(worker, addr, t, duration, read_ratio, num_keys, std::ref(stats[t]));
    for (auto& t : worker_threads)
        t.join();
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - bench_start).count();

    // Merge all per-thread latencies
    std::vector<int64_t> all;
    uint64_t total_errors = 0;
    for (auto& s : stats) {
        all.insert(all.end(), s.latencies_us.begin(), s.latencies_us.end());
        total_errors += s.errors;
    }

    if (all.empty()) {
        std::cout << "No successful operations.\n";
        return 1;
    }

    std::sort(all.begin(), all.end());

    auto pct = [&](double p) -> int64_t {
        size_t idx = static_cast<size_t>(p * all.size());
        return all[std::min(idx, all.size() - 1)];
    };

    double mean = 0;
    for (auto l : all) mean += l;
    mean /= all.size();

    std::cout << "=== Results ===\n";
    std::cout << "total ops:   " << all.size() << "\n";
    std::cout << "errors:      " << total_errors << "\n";
    std::cout << "elapsed:     " << std::fixed << std::setprecision(2) << elapsed << "s\n";
    std::cout << "throughput:  " << std::fixed << std::setprecision(0)
              << (all.size() / elapsed) << " ops/sec\n";
    std::cout << "latency (us):\n";
    std::cout << "  mean  " << std::fixed << std::setprecision(1) << mean << "\n";
    std::cout << "  p50   " << pct(0.50)  << "\n";
    std::cout << "  p95   " << pct(0.95)  << "\n";
    std::cout << "  p99   " << pct(0.99)  << "\n";
    std::cout << "  p999  " << pct(0.999) << "\n";
    std::cout << "  max   " << all.back() << "\n";
}
