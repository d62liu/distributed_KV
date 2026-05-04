#include "util/timer.h"

Timer::Timer(std::function<void()> callback) : callback(std::move(callback)) {
    thread = std::thread(&Timer::run, this);
}

void Timer::run() {
    std::this_thread::sleep_for(duration);
    callback();
}
