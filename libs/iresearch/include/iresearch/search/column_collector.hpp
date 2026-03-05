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

#include <span>

#include "basics/containers/flat_hash_map.h"
#include "iresearch/formats/norm_reader.hpp"

namespace irs {

struct ColumnReader;

class ColumnArgsFetcher {
 public:
  const uint32_t* AddNorms(const ColumnReader* field);

  void Clear() noexcept { _columns.clear(); }

  void Fetch(std::span<doc_id_t> docs) {
    for (auto& [_, entry] : _columns) {
      auto& [reader, norms] = entry;
      reader->Get(docs, norms);
    }
  }

  void Fetch(doc_id_t doc) {
    for (auto& [_, entry] : _columns) {
      auto& [reader, norms] = entry;
      SDB_ASSERT(!norms.empty());
      norms[0] = reader->Get(doc);
    }
  }

 private:
  struct Entry {
    NormReader::ptr reader;
    std::vector<uint32_t> norms;
  };

  mutable sdb::containers::FlatHashMap<field_id, Entry> _columns;
};

}  // namespace irs
