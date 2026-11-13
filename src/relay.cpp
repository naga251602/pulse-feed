#include "queue.hpp"

#include <algorithm>
#include <csignal>
#include <cstring>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>

using namespace pulse;

namespace {
class Epoll {
    Fd fd_{epoll_create1(EPOLL_CLOEXEC)};
public:
    Epoll() { if (!fd_) fail("epoll_create1"); }
    void watch(int op, int fd, uint32_t flags, uint64_t key) {
        epoll_event event{};
        event.events = flags;
        event.data.u64 = key;
        if (epoll_ctl(fd_.get(), op, fd, &event) < 0) fail("epoll_ctl");
    }
    template<size_t N>
    int wait(std::array<epoll_event, N>& events, int timeout) {
        int count;
        do {
            count = epoll_wait(fd_.get(), events.data(), N, timeout);
        } while (count < 0 && errno == EINTR);
        if (count < 0) fail("epoll_wait");
        return count;
    }
};

Fd listener(unsigned port) {
    Fd fd(socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!fd) fail("socket");
    socket_option(fd.get(), SOL_SOCKET, SO_REUSEADDR, 1);
    auto addr = address(port);
    if (bind(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) fail("bind");
    if (listen(fd.get(), 128) < 0) fail("listen");
    return fd;
}

Fd accept_client(int listener_fd) {
    for (;;) {
        Fd fd(accept4(listener_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC));
        if (fd || errno == EAGAIN || errno == EWOULDBLOCK) return fd;
        if (errno != EINTR && errno != ECONNABORTED) fail("accept4");
    }
}

Fd event_counter() {
    Fd fd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    if (!fd) fail("eventfd");
    return fd;
}
void event_io(int fd, bool write_event) {
    uint64_t value = 1;
    ssize_t result;
    do {
        result = write_event ? write(fd, &value, sizeof(value)) : read(fd, &value, sizeof(value));
    } while (result < 0 && errno == EINTR);
    if (result < 0 && errno != EAGAIN) fail("eventfd I/O");
}

struct Config {
    unsigned ingress = 9001;
    unsigned egress = 9002;
    unsigned queue_frames = 4096;
    unsigned pending_frames = 4096;
    unsigned max_clients = 64;
    unsigned socket_buffer = 16384;
    unsigned drain_ms = 1000;
    bool spsc = true;
};

struct Subscriber {
    Fd fd;
    std::vector<uint8_t> pending;
    size_t begin = 0;
    size_t used = 0;

    bool append(const Frame& frame) {
        if (frame.size() > pending.size() - used) return false;
        const size_t end = (begin + used) % pending.size();
        const size_t first = std::min(frame.size(), pending.size() - end);
        std::memcpy(pending.data() + end, frame.data(), first);
        std::memcpy(pending.data(), frame.data() + first, frame.size() - first);
        used += frame.size();
        return true;
    }
    bool flush() {
        size_t budget = 16 * 1024;
        while (used && budget) {
            const size_t size = std::min({budget, used, pending.size() - begin});
            const auto sent = send(fd.get(), pending.data() + begin, size, MSG_NOSIGNAL);
            if (sent < 0 && errno == EINTR) continue;
            if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
            if (sent <= 0) return false;
            begin = (begin + sent) % pending.size();
            used -= sent;
            budget -= sent;
        }
        return true;
    }
};

struct Stats {
    uint64_t ingested = 0, high_water = 0, pauses = 0, partial_frames = 0;
    uint64_t backpressure_disconnects = 0, subscriber_high_water_bytes = 0;
    uint64_t subscribers_accepted = 0, subscribers_rejected = 0;
    uint64_t discarded_frames = 0, discarded_deliveries = 0;
};

class Relay {
    const Config config_;
    Queue queue_;
    Fd data_ = event_counter();
    Fd space_ = event_counter();
    std::atomic<bool> paused_{false};
    std::atomic<bool> draining_{false};
    std::atomic<bool> fanout_failed_{false};
    std::atomic<uint64_t> deadline_ns_{0};
    std::exception_ptr fanout_error_;
    Stats stats_;

    void fanout(Fd server, std::stop_token stop) {
        Epoll poll;
        poll.watch(EPOLL_CTL_ADD, server.get(), EPOLLIN, 1);
        poll.watch(EPOLL_CTL_ADD, data_.get(), EPOLLIN, 2);
        std::unordered_map<uint64_t, Subscriber> clients;
        uint64_t next_key = 3;
        std::array<epoll_event, 64> events{};
        Frame frame;
        bool more = false;

        while (!stop.stop_requested()) {
            const uint64_t deadline = deadline_ns_.load(std::memory_order_acquire);
            if (draining_.load(std::memory_order_acquire) && server) server.reset();
            if (deadline && now_ns() >= deadline) {
                while (queue_.pop(frame)) ++stats_.discarded_frames;
                for (const auto& [key, client] : clients)
                    stats_.discarded_deliveries += (client.used + Frame{}.size() - 1) / Frame{}.size();
                return;
            }
            const int count = poll.wait(events, more ? 0 : 20);
            for (int i = 0; i < count; ++i) {
                const auto [flags, data] = events[i];
                const auto key = data.u64;
                if (key == 1 && server && !draining_.load(std::memory_order_acquire)) {
                    for (int accepted = 0; accepted < 32; ++accepted) {
                        auto fd = accept_client(server.get());
                        if (!fd) break;
                        if (clients.size() >= config_.max_clients) {
                            ++stats_.subscribers_rejected;
                            continue;
                        }
                        socket_option(fd.get(), IPPROTO_TCP, TCP_NODELAY, 1);
                        socket_option(fd.get(), SOL_SOCKET, SO_SNDBUF, config_.socket_buffer);
                        poll.watch(EPOLL_CTL_ADD, fd.get(), EPOLLIN, next_key);
                        clients.emplace(next_key++, Subscriber{
                            std::move(fd), std::vector<uint8_t>(config_.pending_frames * Frame{}.size())});
                        ++stats_.subscribers_accepted;
                    }
                } else if (key == 2) {
                    event_io(data_.get(), false);
                } else if (auto it = clients.find(key); it != clients.end()) {
                    auto& client = it->second;
                    if ((flags & (EPOLLERR | EPOLLHUP | EPOLLIN)) ||
                        ((flags & EPOLLOUT) && !client.flush())) {
                        clients.erase(it);
                    } else if (!client.used) {
                        poll.watch(EPOLL_CTL_MOD, client.fd.get(), EPOLLIN, key);
                    }
                }
            }

            unsigned batch = 0;
            for (; batch < 64 && queue_.pop(frame); ++batch) {
                for (auto it = clients.begin(); it != clients.end();) {
                    auto& client = it->second;
                    const bool empty = client.used == 0;
                    if (!client.append(frame)) {
                        ++stats_.backpressure_disconnects;
                        it = clients.erase(it);
                        continue;
                    }
                    stats_.subscriber_high_water_bytes =
                        std::max<uint64_t>(stats_.subscriber_high_water_bytes, client.used);
                    if (empty)
                        poll.watch(EPOLL_CTL_MOD, client.fd.get(), EPOLLIN | EPOLLOUT, it->first);
                    ++it;
                }
            }
            if (batch) event_io(space_.get(), true);
            more = batch == 64;
            if (deadline && !more &&
                std::none_of(clients.begin(), clients.end(), [](const auto& item) {
                    return item.second.used != 0;
                })) return;
        }
    }

    void ingest(Fd server, int signals) {
        Epoll poll;
        poll.watch(EPOLL_CTL_ADD, server.get(), EPOLLIN, 1);
        poll.watch(EPOLL_CTL_ADD, signals, EPOLLIN, 2);
        poll.watch(EPOLL_CTL_ADD, space_.get(), EPOLLIN, 3);
        Fd publisher;
        uint64_t publisher_key = 3;
        std::array<epoll_event, 8> events{};
        std::array<uint8_t, 8192> staging{};
        size_t begin = 0, end = 0;
        const size_t width = Frame{}.size();

        auto publish_staged = [&] {
            bool pushed = false;
            while (end - begin >= width) {
                Frame frame;
                std::memcpy(frame.data(), staging.data() + begin, width);
                size_t depth;
                if (!queue_.push(frame, depth)) {
                    if (!paused_.exchange(true)) ++stats_.pauses;
                    // Close the race where the consumer freed space before seeing paused.
                    if (queue_.producer_depth() <= queue_.capacity() / 2) {
                        paused_.store(false, std::memory_order_release);
                        continue;
                    }
                    poll.watch(EPOLL_CTL_DEL, publisher.get(), 0, publisher_key);
                    break;
                }
                begin += width;
                pushed = true;
                ++stats_.ingested;
                stats_.high_water = std::max<uint64_t>(stats_.high_water, depth);
            }
            if (pushed) event_io(data_.get(), true);
            if (!paused_.load() && begin) {
                end -= begin;
                std::memmove(staging.data(), staging.data() + begin, end);
                begin = 0;
            }
        };

        for (;;) {
            const int count = poll.wait(events, -1);
            if (fanout_failed_.load(std::memory_order_acquire)) break;
            bool stopping = false;
            for (int i = 0; i < count; ++i) stopping |= events[i].data.u64 == 2;
            if (stopping) break;

            for (int i = 0; i < count; ++i) {
                const auto key = events[i].data.u64;
                if (key == 1) {
                    for (int accepted = 0; accepted < 32; ++accepted) {
                        auto fd = accept_client(server.get());
                        if (!fd) break;
                        if (publisher) continue;
                        socket_option(fd.get(), SOL_SOCKET, SO_RCVBUF, config_.socket_buffer);
                        publisher = std::move(fd);
                        poll.watch(EPOLL_CTL_ADD, publisher.get(), EPOLLIN, ++publisher_key);
                    }
                } else if (key == 3) {
                    event_io(space_.get(), false);
                    if (paused_.load() && queue_.producer_depth() <= queue_.capacity() / 2) {
                        paused_.store(false, std::memory_order_release);
                        poll.watch(EPOLL_CTL_ADD, publisher.get(), EPOLLIN, publisher_key);
                        publish_staged();
                    }
                } else if (publisher && key == publisher_key && !paused_.load()) {
                    const auto size = recv(publisher.get(), staging.data() + end, staging.size() - end, 0);
                    if (size > 0) {
                        end += size;
                        publish_staged();
                    } else if (size == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                        if (end != begin) ++stats_.partial_frames;
                        begin = end = 0;
                        publisher.reset();
                    }
                }
            }
        }

        draining_.store(true, std::memory_order_release);
        event_io(data_.get(), true);
        server.reset();
        publisher.reset();
        const auto deadline = now_ns() + config_.drain_ms * 1'000'000ULL;
        while (end - begin >= width && now_ns() < deadline &&
               !fanout_failed_.load(std::memory_order_acquire)) {
            Frame frame;
            std::memcpy(frame.data(), staging.data() + begin, width);
            size_t depth;
            if (!queue_.push(frame, depth)) {
                std::this_thread::sleep_for(1ms);
                continue;
            }
            begin += width;
            ++stats_.ingested;
            stats_.high_water = std::max<uint64_t>(stats_.high_water, depth);
            event_io(data_.get(), true);
        }
        stats_.discarded_frames = (end - begin) / width;
        deadline_ns_.store(deadline, std::memory_order_release);
        event_io(data_.get(), true);
    }

public:
    explicit Relay(Config config) : config_(config), queue_(config.queue_frames, config.spsc) {}
    void run() {
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, SIGINT);
        sigaddset(&mask, SIGTERM);
        if (sigprocmask(SIG_BLOCK, &mask, nullptr) < 0) fail("sigprocmask");
        Fd signals(signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC));
        if (!signals) fail("signalfd");
        auto upstream = listener(config_.ingress);
        auto downstream = listener(config_.egress);

        std::jthread worker([this, server = std::move(downstream)](std::stop_token stop) mutable {
            try {
                fanout(std::move(server), stop);
            } catch (...) {
                fanout_error_ = std::current_exception();
                fanout_failed_.store(true, std::memory_order_release);
                event_io(space_.get(), true);
            }
        });
        std::cout << "READY\n" << std::flush;
        ingest(std::move(upstream), signals.get());
        worker.join();
        if (fanout_error_) std::rethrow_exception(fanout_error_);
        std::cout << "{\"ingested\":" << stats_.ingested
                  << ",\"queue_depth\":" << queue_.producer_depth()
                  << ",\"queue_high_water\":" << stats_.high_water
                  << ",\"upstream_pauses\":" << stats_.pauses
                  << ",\"partial_frames\":" << stats_.partial_frames
                  << ",\"subscribers_accepted\":" << stats_.subscribers_accepted
                  << ",\"subscribers_rejected\":" << stats_.subscribers_rejected
                  << ",\"subscriber_high_water_bytes\":" << stats_.subscriber_high_water_bytes
                  << ",\"backpressure_disconnects\":" << stats_.backpressure_disconnects
                  << ",\"discarded_frames\":" << stats_.discarded_frames
                  << ",\"discarded_deliveries\":" << stats_.discarded_deliveries << "}\n";
    }
};
}

int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string_view(argv[1]) == "--help") {
            std::cout << "pulsefeed [--ingress PORT] [--egress PORT] [--queue spsc|mutex]\n"
                         "          [--queue-frames N] [--pending-frames N] [--max-clients N]\n"
                         "          [--socket-buffer BYTES] [--drain-ms N]\n";
            return 0;
        }
        Config config;
        for (int i = 1; i < argc; i += 2) {
            if (i + 1 == argc) throw std::invalid_argument("missing option value");
            const std::string_view key = argv[i], value = argv[i + 1];
            if (key == "--ingress") config.ingress = number(value, 1, 65535);
            else if (key == "--egress") config.egress = number(value, 1, 65535);
            else if (key == "--queue-frames") config.queue_frames = number(value, 2, 1 << 20);
            else if (key == "--pending-frames") config.pending_frames = number(value, 1, 1 << 16);
            else if (key == "--max-clients") config.max_clients = number(value, 1, 256);
            else if (key == "--socket-buffer") config.socket_buffer = number(value, 1024, 1 << 20);
            else if (key == "--drain-ms") config.drain_ms = number(value, 1, 10000);
            else if (key == "--queue" && (value == "mutex" || value == "spsc"))
                config.spsc = value == "spsc";
            else throw std::invalid_argument("invalid option");
        }
        Relay(config).run();
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
