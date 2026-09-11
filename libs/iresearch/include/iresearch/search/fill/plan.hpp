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

#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/collect_scored.hpp"
#include "iresearch/search/detail/fill_posting_scored.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/set_leaves.hpp"

namespace irs::fill {

Node::ptr MakeFixedPhraseDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseIntervalsDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseSlopDocs(const FixedPhraseQuery& query);
Node::ptr MakeVariadicPhraseDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseIntervalsDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseSlopDocs(const VariadicPhraseQuery& query);

Node::ptr MakeFixedPhraseScored(const FixedPhraseQuery& query,
                                const detail::ScoredCtx& ctx, ScoreMergeType merge);
Node::ptr MakeFixedPhraseIntervalsScored(const FixedPhraseQuery& query,
                                         const detail::ScoredCtx& ctx,
                                         ScoreMergeType merge);
Node::ptr MakeFixedPhraseSlopScored(const FixedPhraseQuery& query,
                                    const detail::ScoredCtx& ctx, ScoreMergeType merge);
Node::ptr MakeVariadicPhraseScored(const VariadicPhraseQuery& query,
                                   const detail::ScoredCtx& ctx, ScoreMergeType merge);
Node::ptr MakeVariadicPhraseIntervalsScored(const VariadicPhraseQuery& query,
                                            const detail::ScoredCtx& ctx,
                                            ScoreMergeType merge);
Node::ptr MakeVariadicPhraseSlopScored(const VariadicPhraseQuery& query,
                                       const detail::ScoredCtx& ctx,
                                       ScoreMergeType merge);

Node::ptr MakeNGramDocs(const NGramSimilarityQuery& query);
Node::ptr MakeNGramAllDocs(const NGramSimilarityQuery& query);
Node::ptr MakeNGramScored(const NGramSimilarityQuery& query,
                          const detail::ScoredCtx& ctx, ScoreMergeType merge);
Node::ptr MakeNGramAllScored(const NGramSimilarityQuery& query,
                             const detail::ScoredCtx& ctx, ScoreMergeType merge);

}  // namespace irs::fill
