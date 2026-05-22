#pragma once
#include <functional>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>

class Timer {
public:
    explicit Timer(std::function<void()> callback);
    Timer(uint64_t time_ms, std::function<void()> callback);
    ~Timer();
    void reset();
    void stop();
    void cancel();

private:
    void run();

    std::function<void()> callback;
    std::thread thread;
    std::chrono::milliseconds duration;
    bool randomize;
    std::mutex mu;
    std::condition_variable cv;
    bool stopped = false;
    bool reset_requested = false;
};
