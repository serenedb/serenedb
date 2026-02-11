////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/random/random.h>
#include <absl/strings/cord.h>
#include <benchmark/benchmark.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>

#include <boost/circular_buffer.hpp>
#include <boost/container/devector.hpp>
#include <boost/container/string.hpp>

#include "basics/message_buffer.h"

namespace {

class BufferFixture : public benchmark::Fixture {
 public:
  enum class Action : uint8_t {
    Flush,
    FinishWrite,
  };

  struct DataStat {
    size_t size;
    double avg_packet_size;
    size_t median_packet_size;
    size_t min_packet_size;
    size_t max_packet_size;

    double avg_buffer_size;
    size_t median_buffer_size;
    size_t min_buffer_size;
    size_t max_buffer_size;

    DataStat(std::vector<std::string> data,
             const std::vector<size_t>& actions) {
      size_t actions_begin = 0;
      size_t current_size = 0;
      std::vector<size_t> buffer_sizes;
      buffer_sizes.reserve(actions.size() >> 1);
      for (size_t i = 0; i < data.size(); ++i) {
        current_size += data[i].size();
        if (actions_begin < actions.size() &&
            (actions[actions_begin] >> 1) == i) {
          buffer_sizes.push_back(current_size);
          actions_begin += 2;
          current_size = 0;
        }
      }
      std::sort(buffer_sizes.begin(), buffer_sizes.end());
      avg_buffer_size = static_cast<double>(std::accumulate(
                          buffer_sizes.begin(), buffer_sizes.end(), 0,
                          [](size_t sum, size_t size) { return sum + size; })) /
                        buffer_sizes.size();
      median_buffer_size = buffer_sizes[(buffer_sizes.size() - 1) >> 1];
      min_buffer_size = buffer_sizes[0];
      max_buffer_size = buffer_sizes.back();

      auto data_copy = data;
      std::sort(data_copy.begin(), data_copy.end());
      size = data.size();
      avg_packet_size =
        static_cast<double>(std::accumulate(
          data_copy.begin(), data_copy.end(), 0,
          [](size_t sum, const std::string& s) { return sum + s.size(); })) /
        size;
      median_packet_size = data_copy[(data_copy.size() - 1) >> 1].size();
      min_packet_size = data_copy[0].size();
      max_packet_size = data_copy.back().size();
    }

    void PrintStat() const {
      std::cout << "BENCHMARK DATA INFO: \n"
                << "Packets: " << size << "\n"
                << "Average packet size: " << avg_packet_size << "\n"
                << "Median packet size: " << median_packet_size << "\n"
                << "Minimum packet size: " << min_packet_size << "\n"
                << "Maximum packet size: " << max_packet_size << "\n"
                << "Average buffer size: " << avg_buffer_size << "\n"
                << "Median buffer size: " << median_buffer_size << "\n"
                << "Minimum buffer size: " << min_buffer_size << "\n"
                << "Maximum buffer size: " << max_buffer_size << "\n";
    }
  };

  static constexpr size_t kSize = 100'000;
  static constexpr size_t kPacketSize = 1024;
  static std::vector<std::string> gData;
  static std::vector<size_t> gActions;
  BufferFixture() {
    if (!gData.empty()) {
      return;
    }
    absl::BitGen bitgen;
    gData.reserve(kSize);
    gActions.reserve(kSize);
    Action last_action = Action::FinishWrite;
    size_t buffer_size = 0;
    for (size_t i = 0; i < kSize; ++i) {
      size_t sz = 0;
      // 0.95 chance of small packet
      bool short_packet = absl::Bernoulli(bitgen, .95);
      if (short_packet) {
        // size mean = 1/10 * kPacketSize
        sz = static_cast<size_t>(absl::Beta(bitgen, 1., 9.) * kPacketSize + 4);
      } else {
        // size mean = 9/10 * kPacketSize
        sz = static_cast<size_t>(absl::Beta(bitgen, 9., 1.) * kPacketSize + 4);
      }
      gData.emplace_back(sz, 'a');
      buffer_size += sz;
      bool act = absl::Bernoulli(bitgen, 0.1);
      if (buffer_size && act) {
        switch (last_action) {
          case Action::FinishWrite:
            gActions.push_back(i * 2);
            last_action = Action::Flush;
            buffer_size = 0;
            break;
          case Action::Flush:
            gActions.push_back(i * 2 + 1);
            last_action = Action::FinishWrite;
            break;
          default:
        }
      }
    }
    DataStat stat{gData, gActions};
    stat.PrintStat();
  }

  template<typename PushFunc, typename FlushFunc, typename FinishFunc>
  void Step(PushFunc push, FlushFunc flush, FinishFunc finish_write) {
    size_t last_flush = 0;
    size_t current_size = 0;
    size_t actions_begin = 0;
    size_t action_index = gActions[actions_begin] >> 1;
    for (size_t i = 0; i < gData.size(); ++i) {
      push(gData[i]);
      current_size += gData[i].size();
      if (action_index != i) {
        continue;
      }
      if (gActions[actions_begin] & 1) {
        finish_write(last_flush);
        last_flush = 0;
      } else {
        flush();
        last_flush = current_size;
        current_size = 0;
      }
      actions_begin++;
      if (actions_begin < gActions.size()) {
        action_index = (gActions[actions_begin] >> 1);
      }
    }
    flush();
    finish_write(current_size);
  }
};

std::vector<std::string> BufferFixture::gData = {};
std::vector<size_t> BufferFixture::gActions = {};

BENCHMARK_DEFINE_F(BufferFixture, BM_folly_appender)(benchmark::State& state) {
  for (auto _ : state) {
    folly::IOBufQueue buffer;
    folly::io::QueueAppender cursor{&buffer, 64, 4096};
    Step(
      [&](std::string_view str) {
        cursor.push((unsigned char*)str.data(), str.size());
      },
      [&]() {
        // Flushing
      },
      [&](size_t n) { buffer.trimStart(n); });
    benchmark::DoNotOptimize(buffer);
  }
}

BENCHMARK_DEFINE_F(BufferFixture, BM_folly_iobuf)(benchmark::State& state) {
  for (auto _ : state) {
    folly::IOBufQueue buffer;
    Step([&](std::string_view str) { buffer.append(str); },
         [&]() {
           // Flushing
         },
         [&](size_t n) { buffer.trimStart(n); });
    benchmark::DoNotOptimize(buffer);
  }
}

BENCHMARK_DEFINE_F(BufferFixture,
                   BM_folly_appender_with_mutex)(benchmark::State& state) {
  for (auto _ : state) {
    folly::IOBufQueue buffer;
    folly::io::QueueAppender cursor{&buffer, 64, 4096};
    absl::Mutex m;
    Step(
      [&](std::string_view str) {
        absl::MutexLock lock{&m};
        cursor.push((unsigned char*)str.data(), str.size());
      },
      [&]() {
        // Flushing
      },
      [&](size_t n) {
        absl::MutexLock lock{&m};
        buffer.trimStart(n);
      });
    benchmark::DoNotOptimize(buffer);
  }
}

BENCHMARK_DEFINE_F(BufferFixture, BM_absl_cord)(benchmark::State& state) {
  for (auto _ : state) {
    absl::Cord cord;
    Step([&](std::string_view str) { cord.Append(str); },
         [&]() {
           // Flushing
         },
         [&](size_t n) { cord.RemovePrefix(n); });
    benchmark::DoNotOptimize(cord);
  }
}

BENCHMARK_DEFINE_F(BufferFixture, BM_basic_string)(benchmark::State& state) {
  for (auto _ : state) {
    std::string write_str;
    std::string read_str;
    Step([&](std::string_view s) { write_str += s; },
         [&]() { std::swap(read_str, write_str); },
         [&](size_t n) {
           read_str.clear();
           read_str.shrink_to_fit();
         });
    benchmark::DoNotOptimize(write_str);
    benchmark::DoNotOptimize(read_str);
  }
}

BENCHMARK_DEFINE_F(BufferFixture, BM_boost_string)(benchmark::State& state) {
  for (auto _ : state) {
    boost::container::string write_str;
    boost::container::string read_str;
    Step([&](std::string_view s) { write_str += s; },
         [&]() { std::swap(write_str, read_str); },
         [&](size_t n) {
           read_str.clear();
           read_str.shrink_to_fit();
         });
    benchmark::DoNotOptimize(write_str);
    benchmark::DoNotOptimize(read_str);
  }
}

BENCHMARK_DEFINE_F(BufferFixture, BM_deque_char)(benchmark::State& state) {
  for (auto _ : state) {
    std::deque<char> write_deq;
    std::deque<char> read_deq;
    Step([&](std::string_view s) { write_deq.append_range(s); },
         [&]() { std::swap(write_deq, read_deq); },
         [&](size_t n) {
           read_deq.clear();
           read_deq.shrink_to_fit();
         });
    benchmark::DoNotOptimize(write_deq);
    benchmark::DoNotOptimize(read_deq);
  }
}

BENCHMARK_DEFINE_F(BufferFixture,
                   BM_boost_circular_buffer)(benchmark::State& state) {
  for (auto _ : state) {
    boost::circular_buffer<char> buf(1);
    Step(
      [&](std::string_view s) {
        for (char c : s) {
          buf.push_back(c);
        }
      },
      [&]() {}, [&](size_t n) { buf.erase_begin(n); });
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_DEFINE_F(BufferFixture, BM_sdb_buffer)(benchmark::State& state) {
  for (auto _ : state) {
    sdb::message::Buffer buffer(64, 4096);
    Step([&](std::string_view s) { buffer.Write(s, false); },
         [&]() { buffer.FlushStart(); }, [&](size_t n) { buffer.FlushDone(); });
    benchmark::DoNotOptimize(true);
  }
}

}  // namespace

// max_in_buffer - the maximum of packets in buffer
#define BUFFER_BENCHMARK(name) BENCHMARK_REGISTER_F(BufferFixture, name);

BUFFER_BENCHMARK(BM_folly_iobuf)
BUFFER_BENCHMARK(BM_folly_appender)
BUFFER_BENCHMARK(BM_folly_appender_with_mutex)
BUFFER_BENCHMARK(BM_absl_cord)
BUFFER_BENCHMARK(BM_deque_char)
BUFFER_BENCHMARK(BM_basic_string)
BUFFER_BENCHMARK(BM_boost_string)
BUFFER_BENCHMARK(BM_boost_circular_buffer)
BUFFER_BENCHMARK(BM_sdb_buffer)

BENCHMARK_MAIN();
