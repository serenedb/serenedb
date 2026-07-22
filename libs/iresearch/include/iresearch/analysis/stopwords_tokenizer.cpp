////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "stopwords_tokenizer.hpp"

#include <string_view>

#include "iresearch/analysis/batch/token_batch.hpp"

namespace irs::analysis {

StopwordsTokenizer::StopwordsTokenizer(
  StopwordsTokenizer::stopwords_set&& stopwords)
  : _stopwords{std::move(stopwords)} {
  for (const auto& w : _stopwords) {
    _prefilter.Add(w);
  }
}

Tokenizer::ptr StopwordsTokenizer::Make(Options opts) {
  return std::make_unique<StopwordsTokenizer>(std::move(opts.mask));
}

template<TokenLayout Layout>
bool StopwordsTokenizer::DoFill(std::string_view value, TokenEmitter& sink) {
  if (!IsStopword(value)) {
    const auto size = static_cast<uint32_t>(value.size());
    sink.Emit<Layout>(duckdb::string_t{value.data(), size}, 0, size);
  }
  return true;
}

template class TypedTokenizer<StopwordsTokenizer>;

}  // namespace irs::analysis
