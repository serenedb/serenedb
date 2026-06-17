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

namespace irs::analysis {

StopwordsTokenizer::StopwordsTokenizer(
  StopwordsTokenizer::stopwords_set&& stopwords)
  : _stopwords{std::move(stopwords)} {}

Analyzer::ptr StopwordsTokenizer::Make(Options opts) {
  return std::make_unique<StopwordsTokenizer>(std::move(opts.mask));
}

bool StopwordsTokenizer::next() {
  if (_term_eof) {
    return false;
  }

  _term_eof = true;

  return true;
}

bool StopwordsTokenizer::reset(std::string_view data) {
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = uint32_t(data.size());
  auto& term = std::get<TermAttr>(_attrs);
  term.value = ViewCast<byte_type>(data);
  _term_eof = _stopwords.contains(data);
  return true;
}

}  // namespace irs::analysis
