#include "common.hpp"
#include <algorithm>
#include <iostream>
#include <thread>

using namespace pulse;

int main(int argc, char** argv) {
    try {
        if (argc != 4) {
            std::cerr << "usage: publisher PORT UPDATES_PER_SECOND SECONDS\n";
            return 2;
        }
        const auto port = number(argv[1], 1, 65535);
        const auto rate = number(argv[2], 1, 1'000'000);
        const auto seconds = number(argv[3], 1, 3600);
        auto fd = connect_local(port);
        socket_option(fd.get(), SOL_SOCKET, SO_SNDBUF, 16384);
        timeval timeout{2, 0};
        if (setsockopt(fd.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0)
            fail("setsockopt");
        const auto start = Clock::now();
        const auto finish = start + std::chrono::seconds(seconds);
        uint64_t sent = 0, max_lag = 0, late = 0;
        bool send_error = false;
        while (Clock::now() < finish && sent < uint64_t(rate) * seconds) {
            const auto due = start + std::chrono::nanoseconds(sent * 1'000'000'000ULL / rate);
            if (due - Clock::now() > 50us) std::this_thread::sleep_until(due - 50us);
            while (Clock::now() < due) {}
            const auto stamp = Clock::now();
            if (stamp >= finish) break;
            const auto lag = std::chrono::duration_cast<std::chrono::nanoseconds>(stamp - due).count();
            max_lag = std::max(max_lag, static_cast<uint64_t>(lag));
            late += lag > 100'000;
            const auto frame = encode({sent + 1, static_cast<uint32_t>(sent % 32),
                                       10'000 + sent % 500, 10, now_ns()});
            size_t offset = 0;
            while (offset < frame.size()) {
                const auto size = send(fd.get(), frame.data() + offset, frame.size() - offset, MSG_NOSIGNAL);
                if (size < 0 && errno == EINTR) continue;
                if (size <= 0) {
                    send_error = true;
                    break;
                }
                offset += size;
            }
            if (send_error) break;
            ++sent;
        }
        const double elapsed = std::chrono::duration<double>(Clock::now() - start).count();
        std::cout << "{\"sent\":" << sent << ",\"publish_seconds\":" << elapsed
                  << ",\"late_over_100us\":" << late << ",\"max_pacing_lag_ns\":" << max_lag
                  << ",\"send_error\":" << std::boolalpha << send_error << "}\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
