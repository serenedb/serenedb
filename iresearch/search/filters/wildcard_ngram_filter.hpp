////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2024 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Valery Mironov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/regexp_ngram.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "re2/re2.h"

namespace irs {
namespace analysis {

class WildcardTokenizer;

}  // namespace analysis

class WildcardNGramVerifier {
 public:
  WildcardNGramVerifier(std::shared_ptr<const re2::RE2> matcher,
                        const ColumnReader& stored_field,
                        const ColReader& col_reader) noexcept
    : _matcher{std::move(matcher)}, _cursor{col_reader, stored_field} {
    SDB_ASSERT(_matcher);
  }

  bool Check(doc_id_t doc) {
    const auto value = _cursor.FetchDoc(doc);
    if (value.empty()) {
      return false;
    }
    auto* terms_begin = value.data();
    auto* terms_end = terms_begin + value.size();
    while (terms_begin != terms_end) {
      auto size = vread<uint32_t>(terms_begin);
      ++terms_begin;

      if (_matcher->Match(ViewCast<char>(bytes_view{terms_begin, size}), 0,
                          size, re2::RE2::ANCHOR_BOTH, nullptr, 0)) {
        return true;
      }

      terms_begin += size + 1;
    }

    return false;
  }

 private:
  std::shared_ptr<const re2::RE2> _matcher;
  ColumnReader::BlobPointReader _cursor;
};

class WildcardNGramQuery : public QueryBuilderImpl<WildcardNGramQuery> {
 public:
  WildcardNGramQuery(const SubReader& segment,
                     std::shared_ptr<const re2::RE2> matcher,
                     QueryBuilder::ptr&& approx, field_id store_field_id,
                     score_t boost)
    : QueryBuilderImpl{segment, approx->EstimateMax(), QueryKind::Other},
      _matcher{std::move(matcher)},
      _approx{std::move(approx)},
      _store_field_id{store_field_id},
      _boost{boost} {
    SDB_ASSERT(_approx);
    SDB_ASSERT(!QueryBuilder::IsEmpty(*_approx));
  }

  struct Recipe {
    std::shared_ptr<const re2::RE2> matcher;
    const ColumnReader* column = nullptr;
    const ColReader* col_reader = nullptr;

    WildcardNGramVerifier Make() const {
      return WildcardNGramVerifier{matcher, *column, *col_reader};
    }
  };

  bool HasMatcher() const noexcept { return _matcher != nullptr; }

  const QueryBuilder& NGrams() const noexcept { return *_approx; }

  Recipe MakeRecipe() const {
    SDB_ASSERT(_matcher);
    SDB_ASSERT(irs::field_limits::valid(_store_field_id));
    const auto* col_reader = _segment.GetColReader();
    SDB_ASSERT(col_reader != nullptr);
    const auto* column = col_reader->Column(_store_field_id);
    SDB_ASSERT(column != nullptr);
    return Recipe{_matcher, column, col_reader};
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  std::shared_ptr<const re2::RE2> _matcher;
  QueryBuilder::ptr _approx;
  field_id _store_field_id;
  score_t _boost;
};

class ByWildcardNGram;

// `grams` holds the n-grams of every `Literal` of `query`, in depth-first
// order. A pattern RE2 rejects gets a `None` query and no matcher.
struct ByWildcardNGramOptions {
  using FilterType = ByWildcardNGram;

  bstring pattern;
  RegexpSyntax syntax{RegexpSyntax::Perl};
  GramQuery query;
  std::vector<ByPhraseOptions> grams;
  bool has_pos{false};
  std::shared_ptr<const re2::RE2> matcher;
  field_id store_field_id{irs::field_limits::invalid()};

  bool operator==(const ByWildcardNGramOptions& other) const noexcept {
    return pattern == other.pattern && syntax == other.syntax &&
           has_pos == other.has_pos && store_field_id == other.store_field_id &&
           query == other.query;
  }

  ByWildcardNGramOptions() noexcept = default;
  ByWildcardNGramOptions(ByWildcardNGramOptions&&) noexcept = default;
  ByWildcardNGramOptions& operator=(ByWildcardNGramOptions&&) noexcept =
    default;

  ByWildcardNGramOptions(std::string_view like,
                         analysis::WildcardTokenizer& analyzer,
                         bool has_positions);
  ByWildcardNGramOptions(bytes_view regexp, RegexpSyntax regexp_syntax,
                         analysis::WildcardTokenizer& analyzer,
                         bool has_positions);
};

class ByWildcardNGram final : public FilterWithField<ByWildcardNGramOptions> {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                          StatsArena& stats,
                                          uint32_t threads) const final;
};

}  // namespace irs
