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
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/token_phrase.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"

namespace irs {

class TokenPhraseQuery : public QueryBuilderImpl<TokenPhraseQuery> {
 public:
  TokenPhraseQuery(const SubReader& segment, const TermReader& reader,
                   QueryBuilder::ptr&& approx,
                   std::shared_ptr<const PhraseTokens> tokens,
                   std::vector<const ColumnReader*> columns,
                   const ByPhraseOptions& phrase,
                   std::span<const std::vector<bstring>> expanded,
                   score_t boost)
    : QueryBuilderImpl{segment, approx->EstimateMax(), QueryKind::Other},
      _approx{std::move(approx)},
      _reader{&reader},
      _tokens{std::move(tokens)},
      _columns{std::move(columns)},
      _matcher{phrase, expanded, reader},
      _boost{boost} {
    _estimate_matches = _approx->EstimateMatches();
    _postings = _approx->Postings();
    _leaves = _approx->Leaves();
  }

  const QueryBuilder& Approx() const noexcept { return *_approx; }

  const TermReader& Reader() const noexcept { return *_reader; }

  const TokenPhraseMatcher& Matcher() const noexcept { return _matcher; }

  const PhraseTokens& Tokens() const noexcept { return *_tokens; }

  std::span<const ColumnReader* const> Columns() const noexcept {
    return _columns;
  }

  bool Sloppy() const noexcept { return _matcher.Sloppy(); }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  QueryBuilder::ptr _approx;
  const TermReader* _reader;
  std::shared_ptr<const PhraseTokens> _tokens;
  std::vector<const ColumnReader*> _columns;
  TokenPhraseMatcher _matcher;
  score_t _boost;
};

}  // namespace irs
