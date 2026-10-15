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

#include <absl/algorithm/container.h>
#include <absl/container/node_hash_map.h>

#include <cstddef>
#include <cstdint>
#include <duckdb/main/prepared_statement.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <utility>

namespace sdb::query {

template<typename Key,
         typename Hash = typename absl::node_hash_map<Key, int>::hasher,
         typename Eq = typename absl::node_hash_map<Key, int, Hash>::key_equal>
class PreparedCache {
 public:
  template<typename Lookup>
  duckdb::PreparedStatement* Find(const Lookup& key) {
    const auto it = _entries.find(key);
    if (it == _entries.end()) {
      return nullptr;
    }
    it->second.last_use = ++_tick;
    return it->second.statement.get();
  }

  duckdb::PreparedStatement& Insert(
    Key key, duckdb::unique_ptr<duckdb::PreparedStatement> statement,
    size_t capacity) {
    SDB_ASSERT(capacity != 0);
    SDB_ASSERT(statement);
    if (_entries.size() >= capacity) {
      _entries.erase(
        absl::c_min_element(_entries, [](const auto& l, const auto& r) {
          return l.second.last_use < r.second.last_use;
        }));
    }
    auto& entry =
      _entries
        .insert_or_assign(std::move(key), Entry{std::move(statement), ++_tick})
        .first->second;
    return *entry.statement;
  }

  template<typename Predicate>
  void EraseIf(Predicate predicate) {
    absl::erase_if(_entries,
                   [&](const auto& entry) { return predicate(entry.first); });
  }

  size_t Size() const noexcept { return _entries.size(); }

 private:
  struct Entry {
    duckdb::unique_ptr<duckdb::PreparedStatement> statement;
    uint64_t last_use = 0;
  };

  irs::containers::NodeHashMap<Key, Entry, Hash, Eq> _entries;
  uint64_t _tick = 0;
};

}  // namespace sdb::query
