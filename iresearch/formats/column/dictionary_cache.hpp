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

#include <cstdint>
#include <duckdb/storage/compression/dict_fsst/dictionary_cache.hpp>

namespace irs {

class BlockDictionaryCache final : public duckdb::DictFSSTDictionaryCache {
 public:
  ~BlockDictionaryCache() override;

  duckdb::buffer_ptr<duckdb::DictionaryEntry> Get() override;
  void Put(const duckdb::buffer_ptr<duckdb::DictionaryEntry>& dictionary,
           duckdb::idx_t bytes) override;

  static void SetLimit(int64_t bytes) noexcept;
  static uint64_t Limit() noexcept;
  static uint64_t Used() noexcept;

 private:
  friend struct DictionaryCacheLru;

  BlockDictionaryCache* _prev = nullptr;
  BlockDictionaryCache* _next = nullptr;
  duckdb::buffer_ptr<duckdb::DictionaryEntry> _dictionary;
  uint64_t _bytes = 0;
};

}  // namespace irs
