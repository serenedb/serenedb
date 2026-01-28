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

#include "basics/containers/flat_hash_map.h"
#include "iresearch/formats/formats.hpp"

namespace irs {

class ColumnCollector {
 public:
  const uint32_t* AddNorms(const ColumnReader* field);

  void Clear() noexcept { _columns.clear(); }

  void Collect(std::span<doc_id_t> docs) {
    for (auto& [_, entry] : _columns) {
      auto& [reader, norms] = entry;
      reader->Collect(docs, norms);
    }
  }

  void Collect(doc_id_t doc) { Collect(std::span{&doc, 1}); }

 private:
  struct Entry {
    NormReader::ptr reader;
    std::vector<uint32_t> norms;
  };

  mutable sdb::containers::FlatHashMap<field_id, Entry> _columns;
};

}  // namespace irs
