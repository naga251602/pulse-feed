#include "common.hpp"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

using namespace pulse;

int main(int argc, char** argv) {
    try {
        if (argc < 2 || argc > 3) {
            std::cerr << "usage: subscriber PORT [READ_DELAY_MS]\n";
            return 2;
        }
        const auto delay = argc == 3 ? number(argv[2], 0, 10000) : 0;
        auto fd = connect_local(number(argv[1], 1, 65535), delay ? 1024 : 0);
        std::cout << "READY\n" << std::flush;
        std::array<uint8_t, 8192> buffer;
        size_t used = 0;
        uint64_t last = 0, first = 0, gaps = 0, out_of_order = 0;
        std::vector<uint64_t> latencies;
        for (;;) {
            if (delay) std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            const auto received = recv(fd.get(), buffer.data() + used,
                                       buffer.size() - used, 0);
            const auto received_at = now_ns();
            if (received < 0 && errno == EINTR) continue;
            if (received < 0 && errno != ECONNRESET) fail("recv");
            if (received <= 0) break;
            used += received;
            size_t offset = 0;
            Frame frame;
            while (used - offset >= frame.size()) {
                std::memcpy(frame.data(), buffer.data() + offset, frame.size());
                const auto update = decode(frame);
                offset += frame.size();
                if (latencies.empty()) first = update.sequence;
                else if (update.sequence <= last) ++out_of_order;
                else gaps += update.sequence - last - 1;
                last = update.sequence;
                latencies.push_back(received_at >= update.sent_ns ? received_at - update.sent_ns : 0);
            }
            used -= offset;
            std::memmove(buffer.data(), buffer.data() + offset, used);
        }
        std::sort(latencies.begin(), latencies.end());
        auto percentile = [&](size_t p) {
            return latencies.empty() ? 0 : latencies[(latencies.size() * p + 99) / 100 - 1];
        };
        std::cout << "{\"received\":" << latencies.size() << ",\"first_sequence\":" << first
                  << ",\"last_sequence\":" << last << ",\"internal_gaps\":" << gaps
                  << ",\"out_of_order\":" << out_of_order << ",\"partial_bytes\":" << used
                  << ",\"p50_ns\":" << percentile(50) << ",\"p95_ns\":" << percentile(95)
                  << ",\"p99_ns\":" << percentile(99) << "}\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
