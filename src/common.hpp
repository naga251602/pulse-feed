#pragma once

#include <arpa/inet.h>
#include <array>
#include <charconv>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <netinet/tcp.h>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace pulse {
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
inline uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}

[[noreturn]] inline void fail(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

class Fd {
    int value_ = -1;
public:
    explicit Fd(int value = -1) : value_(value) {}
    ~Fd() { reset(); }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& other) noexcept : value_(std::exchange(other.value_, -1)) {}
    Fd& operator=(Fd&& other) noexcept {
        if (this != &other) reset(std::exchange(other.value_, -1));
        return *this;
    }
    int get() const { return value_; }
    explicit operator bool() const { return value_ >= 0; }
    void reset(int value = -1) {
        if (value_ >= 0) ::close(value_);
        value_ = value;
    }
};

inline unsigned number(std::string_view input, unsigned min, unsigned max) {
    unsigned value = 0;
    auto [end, error] = std::from_chars(input.data(), input.data() + input.size(), value);
    if (error != std::errc{} || end != input.data() + input.size() || value < min || value > max)
        throw std::invalid_argument("invalid numeric argument");
    return value;
}

using Frame = std::array<uint8_t, 32>;
struct Update {
    uint64_t sequence;
    uint32_t instrument;
    uint64_t price_ticks;
    uint32_t quantity;
    uint64_t sent_ns;
};

inline void put(Frame& frame, size_t offset, size_t width, uint64_t value) {
    for (size_t i = 0; i < width; ++i) {
        frame[offset + width - 1 - i] = static_cast<uint8_t>(value);
        value >>= 8;
    }
}
inline uint64_t get(const Frame& frame, size_t offset, size_t width) {
    uint64_t value = 0;
    for (size_t i = 0; i < width; ++i) value = (value << 8) | frame[offset + i];
    return value;
}
inline Frame encode(const Update& update) {
    Frame frame{};
    put(frame, 0, 8, update.sequence);
    put(frame, 8, 4, update.instrument);
    put(frame, 12, 8, update.price_ticks);
    put(frame, 20, 4, update.quantity);
    put(frame, 24, 8, update.sent_ns);
    return frame;
}
inline Update decode(const Frame& frame) {
    return {get(frame, 0, 8), static_cast<uint32_t>(get(frame, 8, 4)), get(frame, 12, 8),
            static_cast<uint32_t>(get(frame, 20, 4)), get(frame, 24, 8)};
}
inline void socket_option(int fd, int level, int option, int value) {
    if (setsockopt(fd, level, option, &value, sizeof(value)) < 0) fail("setsockopt");
}
inline sockaddr_in address(unsigned port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return addr;
}
inline Fd connect_local(unsigned port, int receive_buffer = 0) {
    Fd fd(socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    if (!fd) fail("socket");
    socket_option(fd.get(), IPPROTO_TCP, TCP_NODELAY, 1);
    if (receive_buffer) socket_option(fd.get(), SOL_SOCKET, SO_RCVBUF, receive_buffer);
    auto addr = address(port);
    if (connect(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) fail("connect");
    return fd;
}
}
