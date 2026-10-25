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
#include <optional>
#include <span>
#include <vector>

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/compiled_phrase.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"

namespace irs {

class TokenPhraseQuery : public QueryBuilderImpl<TokenPhraseQuery> {
 public:
  TokenPhraseQuery(const SubReader& segment, const TermReader& reader,
                   QueryBuilder::ptr&& approx,
                   ByPhraseOptions::TokenizerFactory tokenizer,
                   const ColumnReader& column, const ByPhraseOptions& phrase,
                   std::vector<std::vector<bstring>> expanded,
                   std::span<const PhraseWordStat> words,
                   std::span<const PostingMeta* const> word_metas,
                   score_t boost)
    : QueryBuilderImpl{segment, approx->EstimateMax(), QueryKind::Other},
      _approx{std::move(approx)},
      _reader{&reader},
      _tokenizer{std::move(tokenizer)},
      _explicit_pos{phrase.explicit_pos()},
      _column{&column},
      _compiled{phrase, std::move(expanded), words},
      _boost{boost} {
    _estimate_matches = _approx->EstimateMatches();
    _postings = _approx->Postings();
    _leaves = _approx->Leaves();
    if (const auto& anchor = _compiled.anchor;
        anchor && anchor->slot < word_metas.size() &&
        word_metas[anchor->slot] && word_metas[anchor->slot]->docs_count != 0 &&
        FeaturesHaveFreq(reader.meta().index_features)) {
      _anchor = *word_metas[anchor->slot];
    }
  }

  const QueryBuilder& Approx() const noexcept { return *_approx; }

  const PostingMeta* Anchor() const noexcept {
    return _anchor ? &*_anchor : nullptr;
  }

  const TermReader& Reader() const noexcept { return *_reader; }

  const CompiledPhrase& Compiled() const noexcept { return _compiled; }

  const ByPhraseOptions::TokenizerFactory& Tokenizer() const noexcept {
    return _tokenizer;
  }

  bool ExplicitPos() const noexcept { return _explicit_pos; }

  const ColumnReader& Column() const noexcept { return *_column; }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  QueryBuilder::ptr _approx;
  const TermReader* _reader;
  ByPhraseOptions::TokenizerFactory _tokenizer;
  bool _explicit_pos;
  const ColumnReader* _column;
  CompiledPhrase _compiled;
  std::optional<PostingMeta> _anchor;
  score_t _boost;
};

extern template class QueryBuilderImpl<TokenPhraseQuery>;

}  // namespace irs
