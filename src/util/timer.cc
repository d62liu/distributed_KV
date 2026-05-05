#include "util/timer.h"
#include <random>

static std::chrono::milliseconds random_duration() {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(150, 300);
    return std::chrono::milliseconds(dist(rng));
}

Timer::Timer(std::function<void()> callback)
    : callback(std::move(callback))
    , duration(random_duration())
{
    thread = std::thread(&Timer::run, this);
}

Timer::Timer(uint64_t time_ms, std::function<void()> callback)
    : callback(std::move(callback))
    , duration(time_ms) {
    thread = std::thread(&Timer::run, this);
    }

void Timer::run() {
    while (true) {
        std::unique_lock<std::mutex> lock(mu);
        cv.wait_for(lock, duration, [this]() { return stopped || reset_requested; });
        if (stopped) return;
        if (reset_requested) {
            reset_requested = false;
            continue;
        }
        lock.unlock();
        callback();
    }
}

void Timer::reset() {
    std::unique_lock<std::mutex> lock(mu);
    reset_requested = true;
    cv.notify_one();
}

Timer::~Timer() {
    {
        std::unique_lock<std::mutex> lock(mu);
        stopped = true;
        cv.notify_one();
    }
    thread.join();
}
