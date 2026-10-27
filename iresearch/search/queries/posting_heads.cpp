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

#include "iresearch/search/queries/posting_heads.hpp"

#include <vector>

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/search/queries/multiterm_state.hpp"
#include "iresearch/search/queries/phrase_state.hpp"
#include "iresearch/search/queries/prepared_state_visitor.hpp"
#include "iresearch/search/queries/term_state.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/file_utils_ext.hpp"

namespace irs {
namespace {

class PostingHeads final : public PreparedStateVisitor {
 public:
  bool Visit(const BooleanQuery&, score_t) final { return true; }
  bool Visit(const ByNestedQuery&, score_t) final { return true; }
  bool Visit(const TermState& state, score_t) final {
    Add(state.reader, state.cookie, false);
    return true;
  }
  bool Visit(const MultiTermQuery&, const MultiTermState& state,
             score_t) final {
    for (const auto& entry : state.Terms()) {
      Add(state.Reader(), entry.cookie, false);
    }
    return true;
  }
  bool Visit(const FixedPhraseQuery&, const PhraseState& state, score_t) final {
    AddAll(state);
    return true;
  }
  bool Visit(const VariadicPhraseQuery&, const PhraseState& state,
             score_t) final {
    AddAll(state);
    return true;
  }
  bool Visit(const NGramSimilarityQuery&, const NGramState& state,
             score_t) final {
    AddAll(state);
    return true;
  }

  void Prefetch(uint32_t min_lists) {
    if (_lists < min_lists) {
      return;
    }
    for (auto& [in, extents] : _extents) {
      if (const auto* file = in->File()) {
        file->PrefetchExtents(extents);
      }
    }
  }

 private:
  void AddAll(const PostingsState& state) {
    for (const auto& meta : state.metas) {
      Add(state.reader, meta, true);
    }
  }

  void Add(const TermReader* reader, const PostingMeta& meta, bool positions) {
    if (reader == nullptr) {
      return;
    }
    const auto handles = reader->Handles();
    if (handles.doc && DocExtent(meta) != 0) {
      ++_lists;
      auto& extents = _extents[handles.doc];
      extents.push_back({.offset = meta.doc_start, .size = file_utils::kPage});
      if (meta.docs_count > doc_limits::kBlockSize) {
        extents.push_back({.offset = meta.doc_start + meta.doc_delta,
                           .size = file_utils::kPage});
      }
    }
    if (positions && handles.pos && meta.docs_count != 0) {
      ++_lists;
      _extents[handles.pos].push_back(
        {.offset = meta.pos_start, .size = file_utils::kPage});
    }
  }

  containers::FlatHashMap<const IndexInput*, std::vector<ByteExtent>> _extents;
  uint32_t _lists = 0;
};

}  // namespace

void PrefetchPostingHeads(const QueryBuilder& query, uint32_t min_lists) {
  PostingHeads heads;
  query.Visit(heads, kNoBoost);
  heads.Prefetch(min_lists);
}

}  // namespace irs
