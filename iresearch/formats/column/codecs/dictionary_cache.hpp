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

#pragma once

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <cstring>
#include <duckdb/common/optional_idx.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

class DecodedDictionary final : public duckdb::ObjectCacheEntry {
 public:
  static std::string ObjectType() { return "col-dict"; }

  DecodedDictionary(duckdb::buffer_ptr<duckdb::DictionaryEntry> dictionary,
                    uint64_t bytes) noexcept
    : _dictionary{std::move(dictionary)}, _bytes{bytes} {}

  std::string GetObjectType() final { return ObjectType(); }

  duckdb::optional_idx GetEstimatedCacheMemory() const final { return _bytes; }

  const duckdb::buffer_ptr<duckdb::DictionaryEntry>& Dictionary()
    const noexcept {
    return _dictionary;
  }

 private:
  duckdb::buffer_ptr<duckdb::DictionaryEntry> _dictionary;
  uint64_t _bytes;
};

inline std::string DictionaryCacheKey(uint64_t scope, size_t block) {
  return absl::StrCat("col-dict:", scope, ":", block);
}

class DecodedFrame final : public duckdb::ObjectCacheEntry {
 public:
  static std::string ObjectType() { return "col-frame"; }

  explicit DecodedFrame(ScratchVector<uint8_t> bytes) noexcept
    : _bytes{std::move(bytes)} {}

  std::string GetObjectType() final { return ObjectType(); }

  duckdb::optional_idx GetEstimatedCacheMemory() const final {
    return _bytes.size();
  }

  std::span<const uint8_t> Bytes() const noexcept { return _bytes; }

 private:
  ScratchVector<uint8_t> _bytes;
};

inline void FrameCacheKey(std::string_view segment, uint32_t frame,
                          std::string& key) {
  key.assign(segment);
  absl::StrAppend(&key, "#", frame);
}

template<typename T>
class CachedSymbolTable final : public duckdb::ObjectCacheEntry {
 public:
  static std::string ObjectType() { return std::string{T::kCacheType}; }

  explicit CachedSymbolTable(std::string_view bytes) : _bytes{bytes} {}

  std::string GetObjectType() final { return ObjectType(); }

  duckdb::optional_idx GetEstimatedCacheMemory() const final {
    return sizeof(*this) + 2 * _bytes.size();
  }

  bool Open() { return _table.Open(_bytes); }

  std::string_view Bytes() const noexcept { return _bytes; }

  const T& Table() const noexcept { return _table; }

 private:
  std::string _bytes;
  T _table;
};

inline std::string SymbolTableCacheKey(std::string_view type,
                                       std::string_view bytes) {
  constexpr size_t kWords = 16;
  uint64_t sampled = bytes.size();
  if (bytes.size() >= sizeof(uint64_t)) {
    const auto step = (bytes.size() - sizeof(uint64_t)) / (kWords - 1);
    for (size_t i = 0; i < kWords; ++i) {
      uint64_t word;
      std::memcpy(&word, bytes.data() + i * step, sizeof(word));
      sampled = (sampled ^ word) * 0x9E3779B97F4A7C15;
    }
  }
  return absl::StrCat(type, ":", sampled);
}

template<typename T>
duckdb::shared_ptr<CachedSymbolTable<T>> OpenSymbolTable(
  duckdb::ObjectCache* cache, std::string_view bytes) {
  std::string key;
  if (cache) {
    key = SymbolTableCacheKey(T::kCacheType, bytes);
    if (auto cached = cache->Get<CachedSymbolTable<T>>(key);
        cached && cached->Bytes() == bytes) {
      return cached;
    }
  }
  auto opened = duckdb::make_shared_ptr<CachedSymbolTable<T>>(bytes);
  if (!opened->Open()) {
    return nullptr;
  }
  if (cache) {
    cache->Put(std::move(key), opened);
  }
  return opened;
}

}  // namespace irs::codecs
