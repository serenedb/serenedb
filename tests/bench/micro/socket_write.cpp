////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <arpa/inet.h>
#include <benchmark/benchmark.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>
#include <vector>

namespace {

constexpr size_t kChunk = 16 * 1024;
constexpr size_t kMaxWrite = 1 << 20;

std::pair<int, int> MakeLoopbackPair() {
  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  ::listen(listener, 1);
  socklen_t len = sizeof(addr);
  ::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len);
  const int client = ::socket(AF_INET, SOCK_STREAM, 0);
  ::connect(client, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  const int server = ::accept(listener, nullptr, nullptr);
  ::close(listener);
  return {client, server};
}

// Measures the raw TCP-loopback write throughput ceiling: the speed our pg-wire
// SendWriter rides on, with zero serialization cost. A background reader drains
// the peer as fast as possible so the writer side is the measured bottleneck.
// The single-write sweep finds the throughput plateau (the floor for the send
// flush size); the writev variant mimics SendWriter draining a SequenceView of
// 16 KiB chunks, to confirm scatter-gather carries no penalty at our sizes.
class SocketFixture : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State&) override {
    auto [write_fd, read_fd] = MakeLoopbackPair();
    _write_fd = write_fd;
    _read_fd = read_fd;
    const int one = 1;
    ::setsockopt(_write_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    _payload.assign(kMaxWrite, 'x');
    _reader = std::thread{[fd = _read_fd] {
      std::vector<char> sink(1 << 20);
      while (::recv(fd, sink.data(), sink.size(), 0) > 0) {
      }
    }};
  }

  void TearDown(const benchmark::State&) override {
    ::shutdown(_write_fd, SHUT_RDWR);
    ::close(_write_fd);
    if (_reader.joinable()) {
      _reader.join();
    }
    ::close(_read_fd);
  }

  void WriteAll(size_t len) {
    size_t off = 0;
    while (off < len) {
      const ssize_t n = ::send(_write_fd, _payload.data() + off, len - off, 0);
      if (n <= 0) {
        return;
      }
      off += static_cast<size_t>(n);
    }
  }

  void WriteVChunks(size_t total) {
    const size_t count = total / kChunk;
    std::vector<iovec> iov(count);
    for (size_t i = 0; i < count; ++i) {
      iov[i].iov_base = _payload.data() + i * kChunk;
      iov[i].iov_len = kChunk;
    }
    size_t i = 0;
    while (i < count) {
      const ssize_t n = ::writev(_write_fd, iov.data() + i, count - i);
      if (n <= 0) {
        return;
      }
      auto advance = static_cast<size_t>(n);
      while (i < count && advance >= iov[i].iov_len) {
        advance -= iov[i].iov_len;
        ++i;
      }
      if (i < count && advance != 0) {
        iov[i].iov_base = static_cast<char*>(iov[i].iov_base) + advance;
        iov[i].iov_len -= advance;
      }
    }
  }

 protected:
  int _write_fd = -1;
  int _read_fd = -1;
  std::thread _reader;
  std::vector<char> _payload;
};

BENCHMARK_DEFINE_F(SocketFixture, BM_single_write)(benchmark::State& state) {
  const auto len = static_cast<size_t>(state.range(0));
  for (auto _ : state) {
    WriteAll(len);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(len));
}

BENCHMARK_DEFINE_F(SocketFixture, BM_writev_16k)(benchmark::State& state) {
  const auto len = static_cast<size_t>(state.range(0));
  for (auto _ : state) {
    WriteVChunks(len);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(len));
}

}  // namespace

BENCHMARK_REGISTER_F(SocketFixture, BM_single_write)
  ->RangeMultiplier(2)
  ->Range(4096, kMaxWrite);
BENCHMARK_REGISTER_F(SocketFixture, BM_writev_16k)
  ->RangeMultiplier(2)
  ->Range(kChunk, kMaxWrite);

BENCHMARK_MAIN();
