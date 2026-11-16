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

#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/functional/function_ref.h>
#include <absl/hash/hash.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/common/types/string_heap.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <string_view>

namespace sdb::search {

inline std::string_view KeyView(const duckdb::string_t& key) noexcept {
  return {key.GetData(), key.GetSize()};
}

struct HashedKey {
  mutable duckdb::string_t key;
  size_t hash = 0;
};

inline HashedKey MakeHashedKey(std::string_view key) {
  return {
    .key = {key.data(), static_cast<uint32_t>(key.size())},
    .hash = absl::HashOf(key),
  };
}

struct HashedKeyHash {
  size_t operator()(const HashedKey& key) const noexcept { return key.hash; }
};

struct HashedKeyEq {
  bool operator()(const HashedKey& lhs, const HashedKey& rhs) const noexcept {
    return lhs.hash == rhs.hash && lhs.key == rhs.key;
  }
};

// The constraint keys of the rows a transaction still buffers, which have no
// term dictionary yet, per key field.
class HotKeys {
 public:
  void Add(irs::field_id field, std::string_view key, irs::doc_id_t doc);
  void Clear() noexcept;

  // Visits the rows holding `key`, newest first, until `visit` returns false.
  void ForEach(irs::field_id field, std::string_view key,
               absl::FunctionRef<bool(irs::doc_id_t)> visit) const;

 private:
  struct Field {
    irs::containers::FlatHashMap<HashedKey, irs::doc_id_t, HashedKeyHash,
                                 HashedKeyEq>
      newest;
    absl::flat_hash_map<irs::doc_id_t, irs::doc_id_t> previous;
  };

  mutable absl::Mutex _mutex;
  absl::flat_hash_map<irs::field_id, Field> _fields ABSL_GUARDED_BY(_mutex);
  duckdb::StringHeap _keys ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::search
