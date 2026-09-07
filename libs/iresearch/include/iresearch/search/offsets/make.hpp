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

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/offsets/root.hpp"

namespace irs {

class FixedPhraseQuery;
class VariadicPhraseQuery;
class NGramSimilarityQuery;
struct TermReader;

}  // namespace irs
namespace irs::offsets {

using Handles = search::PhraseHandles;

bool Resolve(const TermReader* field, Handles& out);

Root::ptr MakePosting(const PostingMeta& meta, const Handles& handles);

Root::ptr Make(const FixedPhraseQuery& query);
Root::ptr Make(const VariadicPhraseQuery& query);
Root::ptr Make(const NGramSimilarityQuery& query);

Root::ptr MakeFixedPhrase(const FixedPhraseQuery& query);
Root::ptr MakeFixedPhraseIntervals(const FixedPhraseQuery& query);
Root::ptr MakeFixedPhraseSlop(const FixedPhraseQuery& query);

Root::ptr MakeVariadicPhrase(const VariadicPhraseQuery& query);
Root::ptr MakeVariadicPhraseIntervals(const VariadicPhraseQuery& query);
Root::ptr MakeVariadicPhraseSlop(const VariadicPhraseQuery& query);

Root::ptr MakeNGram(const NGramSimilarityQuery& query);
Root::ptr MakeNGramAll(const NGramSimilarityQuery& query);

}  // namespace irs::offsets
