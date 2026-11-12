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

#include <memory>
#include <span>

#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

class ColumnArgsFetcher {
 public:
  ColumnArgsFetcher() = default;
  ColumnArgsFetcher(ColumnArgsFetcher&&) noexcept = default;
  ColumnArgsFetcher& operator=(ColumnArgsFetcher&& rhs) noexcept {
    if (this != &rhs) {
      std::swap(_columns, rhs._columns);
      std::swap(_index, rhs._index);
    }
    return *this;
  }
  ~ColumnArgsFetcher() { Free(); }

  const uint32_t* AddNorms(field_id field, NormReader::ptr reader) {
    if (!reader) {
      return nullptr;
    }
    auto [it, emplaced] = _index.try_emplace(field, nullptr);
    if (!emplaced) {
      return it->second;
    }
    auto& entry = _columns.emplace_back(std::move(reader), nullptr);
    auto* norms = std::allocator<uint32_t>{}.allocate(kPostingBlock);
    entry.norms = norms;
    it->second = norms;
    return norms;
  }

  void Clear() noexcept {
    Free();
    _columns.clear();
    _index.clear();
  }

  void Fetch(const doc_id_t* docs, scores_size_t n) {
    SDB_ASSERT(n <= kPostingBlock);
    for (auto& entry : _columns) {
      entry.reader->Get(std::span<const doc_id_t>{docs, n},
                        std::span<uint32_t>{entry.norms, kPostingBlock});
    }
  }

  void FetchBlock(const doc_id_t* docs) {
    for (auto& entry : _columns) {
      entry.reader->GetScoreBlock(
        std::span<const doc_id_t, kScoreBlock>{docs, kScoreBlock},
        std::span<uint32_t, kScoreBlock>{entry.norms, kScoreBlock});
    }
  }

  void FetchPostingBlock(const doc_id_t* docs, doc_id_t next_last) {
    for (auto& entry : _columns) {
      entry.reader->GetPostingBlock(
        std::span<const doc_id_t, kPostingBlock>{docs, kPostingBlock},
        std::span<uint32_t, kPostingBlock>{entry.norms, kPostingBlock},
        next_last);
    }
  }

  void Fetch(doc_id_t doc) {
    for (auto& entry : _columns) {
      entry.norms[0] = entry.reader->Get(doc);
    }
  }

 private:
  void Free() noexcept {
    for (auto& entry : _columns) {
      std::allocator<uint32_t>{}.deallocate(entry.norms, kPostingBlock);
    }
  }

  struct Entry {
    NormReader::ptr reader;
    uint32_t* norms = nullptr;
  };

  std::vector<Entry> _columns;
  irs::containers::FlatHashMap<field_id, uint32_t*> _index;
};

inline IRS_FORCE_INLINE void FetchAndScore(ColumnArgsFetcher& fetcher,
                                           const ScoreFunction& score,
                                           const doc_id_t* docs,
                                           score_t* scores, auto n) {
  fetcher.Fetch(docs, static_cast<scores_size_t>(n));
  score.Score(scores, static_cast<scores_size_t>(n));
}

inline IRS_FORCE_INLINE void FetchAndScoreBlock(ColumnArgsFetcher& fetcher,
                                                const ScoreFunction& score,
                                                const doc_id_t* docs,
                                                score_t* scores) {
  fetcher.FetchBlock(docs);
  score.ScoreBlock(scores);
}

inline IRS_FORCE_INLINE void FetchAndScorePostingBlock(
  ColumnArgsFetcher& fetcher, const ScoreFunction& score, const doc_id_t* docs,
  score_t* scores, doc_id_t next_last) {
  fetcher.FetchPostingBlock(docs, next_last);
  score.ScorePostingBlock(scores);
}

}  // namespace irs
