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
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/offsets/root.hpp"
#include "iresearch/utils/query_memory.hpp"

namespace irs {

class FixedPhraseQuery;
class VariadicPhraseQuery;
class NGramSimilarityQuery;
struct TermReader;

}  // namespace irs
namespace irs::offsets {

using Handles = detail::PhraseHandles;

bool Resolve(const TermReader* field, Handles& out);

Root::ptr MakePosting(PlanMemory& memory, const PostingMeta& meta,
                      const Handles& handles);

Root::ptr Make(PlanMemory& memory, const FixedPhraseQuery& query);
Root::ptr Make(PlanMemory& memory, const VariadicPhraseQuery& query);
Root::ptr Make(PlanMemory& memory, const NGramSimilarityQuery& query);

template<detail::PhraseMatch M, typename Query>
Root::ptr MakePhrase(PlanMemory& memory, const Query& query);

template<detail::NGramMatch M>
Root::ptr MakeNGram(PlanMemory& memory, const NGramSimilarityQuery& query);

}  // namespace irs::offsets
