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
#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs {

class ColumnArgsFetcher {
 public:
  // Caches the norm reader for `field`. The first call materialises the
  // per-doc norm scratch buffer; subsequent calls hit the cache. Returns
  // a pointer to the scratch buffer the Fetch* methods fill, or nullptr
  // when `reader` is empty.
  const uint32_t* AddNorms(field_id field, NormReader::ptr reader) {
    if (!reader) {
      return nullptr;
    }
    auto [it, inserted] = _columns.try_emplace(field);
    auto& entry = it->second;
    if (inserted) {
      entry.reader = std::move(reader);
      entry.norms.resize(kPostingBlock);
    }
    return entry.norms.data();
  }

  void Clear() noexcept { _columns.clear(); }

  void FetchPostingBlock(std::span<const doc_id_t, kPostingBlock> docs) {
    for (auto& [_, entry] : _columns) {
      auto& [reader, norms] = entry;
      reader->GetPostingBlock(
        docs, std::span<uint32_t, kPostingBlock>{norms.data(), kPostingBlock});
    }
  }

  void Fetch(std::span<const doc_id_t> docs) {
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
