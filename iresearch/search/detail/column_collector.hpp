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
#include <vector>

#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/query_memory.hpp"

namespace irs {

class ColumnArgsFetcher {
 public:
  const uint32_t* AddNorms(PlanMemory& memory, field_id field,
                           NormReader::ptr reader) {
    if (!reader) {
      return nullptr;
    }
    auto [it, emplaced] = _index.try_emplace(field, nullptr);
    if (!emplaced) {
      return it->second;
    }
    auto* norms = memory.Scratch().Allocate<uint32_t>(kPostingBlock);
    _columns.emplace_back(std::move(reader), norms);
    it->second = norms;
    return norms;
  }

  void Clear() noexcept {
    _columns.clear();
    _index.clear();
  }

  void FetchScoreBlock(std::span<const doc_id_t, kScoreBlock> docs) {
    for (auto& entry : _columns) {
      entry.reader->GetScoreBlock(
        docs, std::span<uint32_t, kScoreBlock>{entry.norms, kScoreBlock});
    }
  }

  void FetchPostingBlock(std::span<const doc_id_t, kPostingBlock> docs) {
    for (auto& entry : _columns) {
      entry.reader->GetPostingBlock(
        docs, std::span<uint32_t, kPostingBlock>{entry.norms, kPostingBlock});
    }
  }

  void Fetch(std::span<const doc_id_t> docs) {
    for (auto& entry : _columns) {
      entry.reader->Get(docs, std::span<uint32_t>{entry.norms, kPostingBlock});
    }
  }

  void Fetch(doc_id_t doc) {
    for (auto& entry : _columns) {
      entry.norms[0] = entry.reader->Get(doc);
    }
  }

 private:
  struct Entry {
    NormReader::ptr reader;
    uint32_t* norms = nullptr;
  };

  std::vector<Entry> _columns;
  irs::containers::FlatHashMap<field_id, uint32_t*> _index;
};

}  // namespace irs
