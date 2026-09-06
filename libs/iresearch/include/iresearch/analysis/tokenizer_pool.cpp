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

#include "iresearch/analysis/tokenizer_pool.hpp"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/storage/buffer_manager.hpp>
#include <thread>

namespace irs::analysis {

struct TokenizerPool::Core {
  Core(duckdb::BufferPool& buffer_pool, size_t max_idle)
    : max_idle{max_idle},
      reservation{duckdb::MemoryTag::OBJECT_CACHE, buffer_pool} {}

  std::vector<std::pair<Tokenizer::ptr, size_t>> Drain(
    bool buffer_pool_alive) noexcept {
    std::vector<std::pair<Tokenizer::ptr, size_t>> victims;
    absl::MutexLock lock{&m};
    std::swap(victims, idle);
    bytes = 0;
    if (buffer_pool_alive) [[likely]] {
      reservation.Resize(0);
    }
    handle_registered = false;
    return victims;
  }

  const size_t max_idle;
  absl::Mutex m;
  std::vector<std::pair<Tokenizer::ptr, size_t>> idle ABSL_GUARDED_BY(m);
  duckdb::BufferPoolReservation reservation ABSL_GUARDED_BY(m);
  size_t bytes ABSL_GUARDED_BY(m) = 0;
  bool handle_registered ABSL_GUARDED_BY(m) = false;
};

class TokenizerPool::ShrinkHandle final : public duckdb::ObjectCacheEntry {
 public:
  static constexpr std::string_view ObjectType() {
    return "sdb_tokenizer_pool_shrink";
  }

  ShrinkHandle(duckdb::weak_ptr<Core> core, size_t bytes)
    : _core{std::move(core)}, _bytes{std::max<size_t>(bytes, 4096)} {}

  ~ShrinkHandle() final {
    if (auto core = _core.lock()) {
      core->Drain(true);
    }
  }

  std::string GetObjectType() final { return std::string{ObjectType()}; }

  duckdb::optional_idx GetEstimatedCacheMemory() const final { return _bytes; }

 private:
  duckdb::weak_ptr<Core> _core;
  size_t _bytes;
};

duckdb::shared_ptr<TokenizerPool> TokenizerPool::Get(
  duckdb::DatabaseInstance& db, std::string_view id) {
  return db.GetSharedObjectCache().GetOrBuild<TokenizerPool>(
    id, [&] { return duckdb::make_uniq<TokenizerPool>(db, std::string{id}); });
}

size_t TokenizerPool::MaxIdle() noexcept {
  static const size_t max_idle =
    std::max<size_t>(4, std::thread::hardware_concurrency());
  return max_idle;
}

TokenizerPool::TokenizerPool(duckdb::DatabaseInstance& db, std::string id,
                             size_t max_idle)
  : _db{db.shared_from_this()},
    _handle_key{absl::StrCat(ShrinkHandle::ObjectType(), "-", id)},
    _core{duckdb::make_shared_ptr<Core>(
      duckdb::BufferManager::GetBufferManager(db).GetBufferPool(), max_idle)} {}

TokenizerPool::~TokenizerPool() noexcept {
  auto db = _db.lock();
  auto victims = _core->Drain(db != nullptr);
  if (db) {
    db->GetObjectCache().Delete(_handle_key);
  }
}

Tokenizer::ptr TokenizerPool::Acquire() {
  absl::MutexLock lock{&_core->m};
  if (_core->idle.empty()) {
    return nullptr;
  }
  auto tokenizer = std::move(_core->idle.back().first);
  _core->bytes -= _core->idle.back().second;
  _core->idle.pop_back();
  _core->reservation.Resize(_core->bytes);
  return tokenizer;
}

void TokenizerPool::Release(Tokenizer::ptr tokenizer) noexcept {
  SDB_ASSERT(tokenizer);
  tokenizer->Unbind();
  auto db = _db.lock();
  if (!db) {
    return;
  }
  size_t handle_bytes = 0;
  {
    absl::MutexLock lock{&_core->m};
    if (_core->idle.size() >= _core->max_idle) {
      return;
    }
    const size_t bytes = tokenizer->MemoryUsage();
    _core->bytes += bytes;
    _core->reservation.Resize(_core->bytes);
    _core->idle.emplace_back(std::move(tokenizer), bytes);
    if (!_core->handle_registered) {
      _core->handle_registered = true;
      handle_bytes = std::max<size_t>(_core->bytes, 1);
    }
  }
  if (handle_bytes != 0) {
    db->GetObjectCache().Put(_handle_key,
                             duckdb::make_shared_ptr<ShrinkHandle>(
                               duckdb::weak_ptr<Core>{_core}, handle_bytes));
  }
}

size_t TokenizerPool::IdleCount() const {
  absl::MutexLock lock{&_core->m};
  return _core->idle.size();
}

}  // namespace irs::analysis
