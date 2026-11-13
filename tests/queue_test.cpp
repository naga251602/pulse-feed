#include "queue.hpp"

#include <cstdlib>
#include <iostream>
#include <thread>

using namespace pulse;

void check(bool result) {
    if (!result) {
        std::cerr << "codec or queue assertion failed\n";
        std::abort();
    }
}

int main() {
    const auto frame = encode({0x0102030405060708ULL, 42, 500, 12, 9000});
    const auto update = decode(frame);
    check(frame[0] == 1 && frame[7] == 8);
    check(update.sequence == 0x0102030405060708ULL && update.instrument == 42 &&
          update.price_ticks == 500 && update.quantity == 12 && update.sent_ns == 9000);

    for (bool spsc : {false, true}) {
        Queue queue(7, spsc);
        size_t depth;
        Frame output;
        check(!queue.pop(output));
        for (size_t i = 1; i <= 7; ++i) check(queue.push(frame, depth) && depth == i);
        check(!queue.push(frame, depth));
        for (int i = 0; i < 7; ++i) check(queue.pop(output) && output == frame);
        constexpr uint64_t count = 300000;
        std::jthread producer([&] {
            size_t producer_depth;
            for (uint64_t i = 0; i < count; ++i) {
                auto input = encode({i, 0, 0, 0, 0});
                while (!queue.push(input, producer_depth)) std::this_thread::yield();
            }
        });
        for (uint64_t i = 0; i < count; ++i) {
            while (!queue.pop(output)) std::this_thread::yield();
            check(decode(output).sequence == i);
        }
        producer.join();
        check(!queue.pop(output));
    }
}
