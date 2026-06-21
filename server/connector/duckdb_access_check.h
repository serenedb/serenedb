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
#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "catalog/object.h"

namespace sdb {
class ConnectionContext;
namespace catalog {
class Table;
}
}  // namespace sdb

namespace sdb::connector {

struct RelationAccess {
  std::shared_ptr<catalog::Table> table;
  catalog::AclMode action = catalog::AclMode::NoRights;
  absl::flat_hash_set<uint64_t> selected;
  absl::flat_hash_set<uint64_t> returned;
  absl::flat_hash_set<uint64_t> updated;
  absl::flat_hash_set<uint64_t> inserted;
  bool table_read = false;
  bool inside_view = false;
  bool dml_projection = false;
};

class AccessRecord {
 public:
  RelationAccess& ForTable(uint64_t table_index) { return _by_table[table_index]; }

  void Clear() { _by_table.clear(); }

  void BeginStatement(uint64_t generation) {
    if (generation != _generation) {
      _generation = generation;
      _by_table.clear();
    }
  }

  void MarkWriteTarget(uint64_t table_index, std::shared_ptr<catalog::Table> table,
                       catalog::AclMode action,
                       const std::vector<uint64_t>& write_columns = {});

  template<typename Fn>
  void ForEach(Fn&& fn) {
    for (auto& [table_index, rel] : _by_table) {
      fn(table_index, rel);
    }
  }

  void Enforce(ConnectionContext& ctx) const;

 private:
  absl::flat_hash_map<uint64_t, RelationAccess> _by_table;
  uint64_t _generation = 0;
};

}  // namespace sdb::connector
