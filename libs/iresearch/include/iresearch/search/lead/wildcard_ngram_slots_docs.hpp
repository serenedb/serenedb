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

#include <utility>

#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename NGrams>
class WildcardNgramSlotsDocs {
 public:
  using Recipe = WildcardNgramQuery::Recipe;

  template<typename NGramsArgs>
  WildcardNgramSlotsDocs(std::piecewise_construct_t, NGramsArgs&& ngrams,
                         const Recipe& recipe)
    : _ngrams{std::make_from_tuple<NGrams>(std::forward<NGramsArgs>(ngrams))},
      _pattern{recipe.Make()} {}

  doc_id_t Seek(doc_id_t target) { return _ngrams.Seek(target); }

  doc_id_t Next(doc_id_t) { return _ngrams.Advance(); }

  bool Match(doc_id_t doc) { return _pattern.Check(doc); }

 private:
  NGrams _ngrams;
  WildcardNgramVerifier _pattern;
};

}  // namespace irs::lead
