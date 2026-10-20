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

#include <absl/strings/str_cat.h>
#include <benchmark/benchmark.h>
#include <linux/perf_event.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <duckdb/common/allocator.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <iresearch/utils/json_leaf.hpp>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr size_t kBatch = 2048;

class Instructions {
 public:
  Instructions() {
    perf_event_attr attr{};
    attr.type = PERF_TYPE_HARDWARE;
    attr.size = sizeof(attr);
    attr.config = PERF_COUNT_HW_INSTRUCTIONS;
    attr.exclude_kernel = 1;
    attr.exclude_hv = 1;
    _fd = static_cast<int>(syscall(__NR_perf_event_open, &attr, 0, -1, -1, 0));
  }

  ~Instructions() {
    if (_fd >= 0) {
      close(_fd);
    }
  }

  bool Open() const noexcept { return _fd >= 0; }

  uint64_t Read() const {
    uint64_t value = 0;
    if (_fd < 0 || read(_fd, &value, sizeof(value)) != sizeof(value)) {
      return 0;
    }
    return value;
  }

 private:
  int _fd = -1;
};

constexpr std::string_view kWords[] = {
  "quick ", "brown ", "fox ", "jumps ", "over ", "the ", "lazy ", "dog ",
};

constexpr std::string_view kEscaped[] = {
  "quick ", "brown ", "fox ",  "jumps ", "over ", "the ",        "lazy ",
  "dog ",   R"(\n)",  R"(\")", R"(\\)",  R"(\t)", "caf\\u00e9 ",
};

std::vector<std::string> Strings(std::span<const std::string_view> pieces,
                                 std::string_view close, int64_t length) {
  std::mt19937 rng{20261015};
  std::vector<std::string> values;
  values.reserve(kBatch);
  for (size_t i = 0; i < kBatch; ++i) {
    std::string value = "\"";
    while (std::ssize(value) < length) {
      value += pieces[rng() % pieces.size()];
    }
    value += close;
    values.emplace_back(std::move(value));
  }
  return values;
}

std::vector<std::string> Numbers() {
  std::mt19937 rng{20261015};
  std::vector<std::string> values;
  values.reserve(kBatch);
  for (size_t i = 0; i < kBatch; ++i) {
    values.emplace_back(absl::StrCat(rng() % 1000000, ".", rng() % 1000));
  }
  return values;
}

void Read(benchmark::State& state, const std::vector<std::string>& values) {
  duckdb::ArenaAllocator strings{duckdb::Allocator::DefaultAllocator()};
  irs::JsonLeafReader reader{strings};
  Instructions counter;
  const auto before = counter.Read();
  for (auto _ : state) {
    strings.Reset();
    for (const auto& value : values) {
      const auto leaf = reader.Read(value);
      benchmark::DoNotOptimize(leaf);
    }
  }
  const auto after = counter.Read();
  const auto reads = static_cast<double>(state.iterations()) * kBatch;
  if (counter.Open()) {
    state.counters["insn_per_read"] =
      static_cast<double>(after - before) / reads;
  }
  state.SetItemsProcessed(static_cast<int64_t>(reads));
}

void BmReadPlain(benchmark::State& state) {
  Read(state, Strings(kWords, R"(")", state.range(0)));
}

void BmReadEscaped(benchmark::State& state) {
  Read(state, Strings(kEscaped, R"(\n")", state.range(0)));
}

void BmReadNumber(benchmark::State& state) { Read(state, Numbers()); }

}  // namespace

BENCHMARK(BmReadPlain)->Arg(16)->Arg(64)->Arg(256)->Arg(2048);
BENCHMARK(BmReadEscaped)->Arg(16)->Arg(64)->Arg(256)->Arg(2048);
BENCHMARK(BmReadNumber);
