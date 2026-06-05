////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/common/types.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/prepared_state_visitor.hpp>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include "connector/highlight/highlight_types.h"

namespace irs {

class FixedPhraseQuery;
class VariadicPhraseQuery;
class NGramSimilarityQuery;
struct PosAttr;
struct OffsAttr;
struct SeekCookie;
struct SubReader;
struct TermReader;

}  // namespace irs
namespace sdb::connector {

struct FilterEntry {
  std::variant<const irs::SeekCookie*, const irs::FixedPhraseQuery*,
               const irs::VariadicPhraseQuery*,
               const irs::NGramSimilarityQuery*>
    filter;

  // Lazy: null until first use in a segment, then reused for all docs.
  irs::DocIterator::ptr docs;
  irs::PosAttr* pos = nullptr;
  const irs::OffsAttr* offs = nullptr;
};

// Per-field state, rebuilt on each segment transition.
struct FieldState {
  const irs::TermReader* reader = nullptr;
  std::vector<FilterEntry> entries;
  containers::FlatHashSet<const irs::SeekCookie*> seen_cookies;

  void Clear() noexcept {
    reader = nullptr;
    entries.clear();
    seen_cookies.clear();
  }
};

struct FieldEntry {
  duckdb::idx_t output_idx = 0;
  irs::field_id id{irs::field_limits::invalid()};
  FieldState state;
};

void PrepareFilterEntry(FilterEntry& entry, const irs::TermReader* reader,
                        const irs::SubReader& segment);

void FillRowOffsets(FieldState& state, const irs::SubReader& segment,
                    irs::doc_id_t doc_id, size_t max_pairs,
                    std::vector<highlight::HitRange>& hits);

class OffsetsCollector final : public irs::PreparedStateVisitor {
 public:
  explicit OffsetsCollector(std::span<FieldEntry> entries) noexcept
    : _entries{entries} {}

  bool Visit(const irs::BooleanQuery&, irs::score_t) final { return true; }
  bool Visit(const irs::ByNestedQuery&, irs::score_t) final { return false; }
  bool Visit(const irs::TermQuery&, const irs::TermState& state,
             irs::score_t) final;
  bool Visit(const irs::MultiTermQuery&, const irs::MultiTermState& state,
             irs::score_t) final;
  bool Visit(const irs::FixedPhraseQuery& query,
             const irs::FixedPhraseState& state, irs::score_t) final;
  bool Visit(const irs::VariadicPhraseQuery& query,
             const irs::VariadicPhraseState& state, irs::score_t) final;
  bool Visit(const irs::NGramSimilarityQuery& query,
             const irs::NGramState& state, irs::score_t) final;

 private:
  FieldState* FindFieldState(const irs::TermReader* reader) noexcept;

  template<typename Q>
  void RecordQuery(const Q& query, const irs::TermReader* reader);

  std::span<FieldEntry> _entries;
};

}  // namespace sdb::connector
