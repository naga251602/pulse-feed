import contextlib
import json
import signal
import socket
import struct
import subprocess
import sys
import time

RELAY = sys.argv[1]


def free_ports():
    with socket.socket() as first, socket.socket() as second:
        for sock in (first, second):
            sock.bind(("127.0.0.1", 0))
        return first.getsockname()[1], second.getsockname()[1]


def connect(port, slow=False):
    sock = socket.socket()
    sock.settimeout(5)
    if slow:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    sock.connect(("127.0.0.1", port))
    return sock


def frame(sequence):
    return struct.pack("!QIQIQ", sequence, 42, 1050, 8, time.monotonic_ns())


def exact(sock, size):
    received = bytearray()
    while len(received) < size:
        data = sock.recv(size - len(received))
        assert data, f"early EOF after {len(received)} of {size} bytes"
        received.extend(data)
    return received


def stop(relay):
    relay.send_signal(signal.SIGTERM)
    output, error = relay.communicate(timeout=5)
    assert relay.returncode == 0, error
    return json.loads(output)


def scenario(queue):
    ingress, egress = free_ports()
    with subprocess.Popen(
        [RELAY, "--ingress", str(ingress), "--egress", str(egress), "--queue", queue,
         "--queue-frames", "8", "--pending-frames", "256", "--socket-buffer", "4096"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) as relay, contextlib.ExitStack() as sockets:
        try:
            assert relay.stdout.readline().strip() == "READY"
            healthy = sockets.enter_context(connect(egress))
            slow = sockets.enter_context(connect(egress, slow=True))
            publisher = sockets.enter_context(connect(ingress))
            time.sleep(0.05)
            for sequence in range(1, 5):
                payload = frame(sequence)
                publisher.sendall(payload[:7])
                time.sleep(0.005)
                publisher.sendall(payload[7:])
                assert exact(healthy, 32) == payload
            for first in range(5, 4101, 32):
                batch = b"".join(frame(i) for i in range(first, first + 32))
                publisher.sendall(batch)
                assert exact(healthy, len(batch)) == batch
            publisher.sendall(b"unfinished")
            publisher.shutdown(socket.SHUT_WR)
            # The relay closes the input connection after recording the partial frame.
            assert publisher.recv(1) == b""
            stats = stop(relay)
            assert healthy.recv(1) == b""
            assert stats["ingested"] == 4100, stats
            assert stats["partial_frames"] == 1, stats
            assert 0 < stats["queue_high_water"] <= 8, stats
            assert stats["upstream_pauses"] > 0, stats
            assert stats["backpressure_disconnects"] == 1, stats
            assert stats["subscriber_high_water_bytes"] <= 256 * 32, stats
            assert stats["discarded_frames"] == stats["discarded_deliveries"] == 0, stats
            assert slow.fileno() >= 0
        finally:
            if relay.poll() is None:
                relay.kill()


for mode in ("spsc", "mutex"):
    scenario(mode)


def shutdown_deadline():
    ingress, egress = free_ports()
    with subprocess.Popen(
        [RELAY, "--ingress", str(ingress), "--egress", str(egress),
         "--pending-frames", "16384", "--socket-buffer", "4096", "--drain-ms", "100"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) as relay, contextlib.ExitStack() as sockets:
        try:
            assert relay.stdout.readline().strip() == "READY"
            sockets.enter_context(connect(egress, slow=True))
            publisher = sockets.enter_context(connect(ingress))
            time.sleep(0.05)
            publisher.sendall(b"".join(frame(i) for i in range(1, 4097)))
            publisher.shutdown(socket.SHUT_WR)
            assert publisher.recv(1) == b""
            start = time.monotonic()
            stats = stop(relay)
            assert time.monotonic() - start < 2
            assert stats["ingested"] == 4096, stats
            assert stats["discarded_deliveries"] > 0, stats
            assert stats["backpressure_disconnects"] == 0, stats
        finally:
            if relay.poll() is None:
                relay.kill()


shutdown_deadline()
