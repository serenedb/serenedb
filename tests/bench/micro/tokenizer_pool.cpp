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

#include <absl/synchronization/mutex.h>
#include <benchmark/benchmark.h>

#include <atomic>
#include <duckdb.hpp>
#include <duckdb/storage/buffer/buffer_pool.hpp>
#include <duckdb/storage/buffer/buffer_pool_reservation.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "basics/object_pool.hpp"
#include "iresearch/analysis/keyword_tokenizer.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace {

using irs::analysis::Tokenizer;

constexpr size_t kBytesPerInstance = 8 << 10;
constexpr size_t kMaxIdle = 256;

duckdb::BufferPool& Pool() {
  static duckdb::DuckDB gDb{nullptr};
  static duckdb::BufferPool& gPool =
    duckdb::BufferManager::GetBufferManager(*gDb.instance).GetBufferPool();
  return gPool;
}

Tokenizer::ptr MakeTokenizer() {
  return std::make_unique<irs::KeywordTokenizer>();
}

template<bool Accounting>
class MutexPool {
 public:
  explicit MutexPool(duckdb::BufferPool& pool)
    : _reservation{std::make_unique<duckdb::TempBufferPoolReservation>(
        duckdb::MemoryTag::OBJECT_CACHE, pool, 0)} {}

  Tokenizer::ptr Acquire() {
    absl::MutexLock lock{&_m};
    if (_idle.empty()) {
      return nullptr;
    }
    auto tokenizer = std::move(_idle.back().first);
    _bytes -= _idle.back().second;
    _idle.pop_back();
    if constexpr (Accounting) {
      _reservation->Resize(_bytes);
    }
    return tokenizer;
  }

  void Release(Tokenizer::ptr tokenizer) {
    absl::MutexLock lock{&_m};
    if (_idle.size() >= kMaxIdle) {
      return;
    }
    _bytes += kBytesPerInstance;
    if constexpr (Accounting) {
      _reservation->Resize(_bytes);
    }
    _idle.emplace_back(std::move(tokenizer), kBytesPerInstance);
  }

 private:
  absl::Mutex _m;
  std::vector<std::pair<Tokenizer::ptr, size_t>> _idle ABSL_GUARDED_BY(_m);
  size_t _bytes ABSL_GUARDED_BY(_m) = 0;
  std::unique_ptr<duckdb::TempBufferPoolReservation> _reservation
    ABSL_GUARDED_BY(_m);
};

template<bool Accounting>
class LockFreePool {
 public:
  explicit LockFreePool(duckdb::BufferPool& pool) : _pool{pool} {}

  ~LockFreePool() {
    while (auto* node = _idle.pop()) {
      node->value.tokenizer.reset();
      delete node;
    }
    while (auto* node = _spare.pop()) {
      delete node;
    }
    _pool.UpdateUsedMemory(duckdb::MemoryTag::OBJECT_CACHE,
                           -static_cast<int64_t>(_bytes.load()));
  }

  Tokenizer::ptr Acquire() {
    auto* node = _idle.pop();
    if (node == nullptr) {
      return nullptr;
    }
    auto tokenizer = std::move(node->value.tokenizer);
    const auto bytes = node->value.bytes;
    _spare.push(*node);
    _count.fetch_sub(1, std::memory_order_relaxed);
    if constexpr (Accounting) {
      _bytes.fetch_sub(bytes, std::memory_order_relaxed);
      _pool.UpdateUsedMemory(duckdb::MemoryTag::OBJECT_CACHE,
                             -static_cast<int64_t>(bytes));
    }
    return tokenizer;
  }

  void Release(Tokenizer::ptr tokenizer) {
    if (_count.fetch_add(1, std::memory_order_relaxed) >= kMaxIdle) {
      _count.fetch_sub(1, std::memory_order_relaxed);
      return;
    }
    auto* node = _spare.pop();
    if (node == nullptr) {
      node = new Stack::NodeType{};
    }
    node->value.tokenizer = std::move(tokenizer);
    node->value.bytes = kBytesPerInstance;
    if constexpr (Accounting) {
      _bytes.fetch_add(kBytesPerInstance, std::memory_order_relaxed);
      _pool.UpdateUsedMemory(duckdb::MemoryTag::OBJECT_CACHE,
                             kBytesPerInstance);
    }
    _idle.push(*node);
  }

 private:
  struct Parked {
    Tokenizer::ptr tokenizer;
    size_t bytes = 0;
  };
  using Stack = irs::ConcurrentStack<Parked>;

  duckdb::BufferPool& _pool;
  Stack _idle;
  Stack _spare;
  std::atomic<size_t> _count{0};
  std::atomic<size_t> _bytes{0};
};

template<typename PoolT>
PoolT& SharedPool() {
  static PoolT gPool{Pool()};
  return gPool;
}

IRS_FORCE_INLINE void Spin(size_t rounds) {
  size_t sum = 0;
  for (size_t i = 0; i < rounds; ++i) {
    benchmark::DoNotOptimize(sum += i);
  }
}

template<typename PoolT, size_t WorkRounds>
void BM_LeaseRelease(benchmark::State& state) {
  auto& pool = SharedPool<PoolT>();
  for (auto _ : state) {
    auto tokenizer = pool.Acquire();
    if (!tokenizer) {
      tokenizer = MakeTokenizer();
    }
    if constexpr (WorkRounds != 0) {
      Spin(WorkRounds);
    }
    pool.Release(std::move(tokenizer));
  }
  state.SetItemsProcessed(state.iterations());
}

}  // namespace

BENCHMARK(BM_LeaseRelease<MutexPool<true>, 0>)
  ->Name("mutex_bare")
  ->ThreadRange(1, 32)
  ->UseRealTime();
BENCHMARK(BM_LeaseRelease<LockFreePool<true>, 0>)
  ->Name("lockfree_bare")
  ->ThreadRange(1, 32)
  ->UseRealTime();
BENCHMARK(BM_LeaseRelease<MutexPool<false>, 0>)
  ->Name("mutex_noacct")
  ->ThreadRange(1, 32)
  ->UseRealTime();
BENCHMARK(BM_LeaseRelease<LockFreePool<false>, 0>)
  ->Name("lockfree_noacct")
  ->ThreadRange(1, 32)
  ->UseRealTime();
BENCHMARK(BM_LeaseRelease<MutexPool<true>, 128>)
  ->Name("mutex_work")
  ->ThreadRange(1, 32)
  ->UseRealTime();
BENCHMARK(BM_LeaseRelease<LockFreePool<true>, 128>)
  ->Name("lockfree_work")
  ->ThreadRange(1, 32)
  ->UseRealTime();

BENCHMARK_MAIN();
