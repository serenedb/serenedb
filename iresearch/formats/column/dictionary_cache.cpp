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

#include "iresearch/formats/column/dictionary_cache.hpp"

#include <unistd.h>

#include <atomic>
#include <mutex>
#include <vector>

namespace irs {

struct DictionaryCacheLru {
  std::mutex mutex;
  BlockDictionaryCache* head = nullptr;
  BlockDictionaryCache* tail = nullptr;
  uint64_t used = 0;

  void Unlink(BlockDictionaryCache& slot) noexcept {
    (slot._prev ? slot._prev->_next : head) = slot._next;
    (slot._next ? slot._next->_prev : tail) = slot._prev;
    slot._prev = slot._next = nullptr;
  }

  void PushFront(BlockDictionaryCache& slot) noexcept {
    slot._prev = nullptr;
    slot._next = head;
    (head ? head->_prev : tail) = &slot;
    head = &slot;
  }
};

namespace {

DictionaryCacheLru& Lru() {
  static DictionaryCacheLru lru;
  return lru;
}

uint64_t PhysicalMemory() noexcept {
  const auto pages = ::sysconf(_SC_PHYS_PAGES);
  const auto page = ::sysconf(_SC_PAGE_SIZE);
  if (pages <= 0 || page <= 0) {
    return 0;
  }
  return static_cast<uint64_t>(pages) * static_cast<uint64_t>(page);
}

std::atomic<uint64_t> gLimit{PhysicalMemory() / 16};

}  // namespace

void BlockDictionaryCache::SetLimit(int64_t bytes) noexcept {
  gLimit.store(bytes < 0 ? PhysicalMemory() / 16
                         : static_cast<uint64_t>(bytes));
}

uint64_t BlockDictionaryCache::Limit() noexcept { return gLimit.load(); }

uint64_t BlockDictionaryCache::Used() noexcept {
  auto& lru = Lru();
  std::lock_guard lock{lru.mutex};
  return lru.used;
}

BlockDictionaryCache::~BlockDictionaryCache() {
  duckdb::buffer_ptr<duckdb::DictionaryEntry> released;
  auto& lru = Lru();
  std::lock_guard lock{lru.mutex};
  if (_dictionary) {
    lru.Unlink(*this);
    lru.used -= _bytes;
    released = std::move(_dictionary);
  }
}

duckdb::buffer_ptr<duckdb::DictionaryEntry> BlockDictionaryCache::Get() {
  auto& lru = Lru();
  std::lock_guard lock{lru.mutex};
  if (!_dictionary) {
    return nullptr;
  }
  if (lru.head != this) {
    lru.Unlink(*this);
    lru.PushFront(*this);
  }
  return _dictionary;
}

void BlockDictionaryCache::Put(
  const duckdb::buffer_ptr<duckdb::DictionaryEntry>& dictionary,
  duckdb::idx_t bytes) {
  const auto limit = Limit();
  if (bytes > limit) {
    return;
  }
  std::vector<duckdb::buffer_ptr<duckdb::DictionaryEntry>> evicted;
  auto& lru = Lru();
  std::lock_guard lock{lru.mutex};
  if (_dictionary) {
    return;
  }
  while (lru.used + bytes > limit && lru.tail != nullptr) {
    auto& victim = *lru.tail;
    lru.Unlink(victim);
    lru.used -= victim._bytes;
    victim._bytes = 0;
    evicted.push_back(std::move(victim._dictionary));
  }
  _dictionary = dictionary;
  _bytes = bytes;
  lru.used += bytes;
  lru.PushFront(*this);
}

}  // namespace irs
