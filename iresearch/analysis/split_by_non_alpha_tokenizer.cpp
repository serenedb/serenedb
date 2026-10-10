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

#include "split_by_non_alpha_tokenizer.hpp"

#include "iresearch/analysis/text/words/split_fill.hpp"

namespace irs::analysis {

Tokenizer::ptr SplitByNonAlphaTokenizer::Make(Options opts) {
  return std::make_unique<SplitByNonAlphaTokenizer>(opts);
}

bool SplitByNonAlphaTokenizer::Scan(const duckdb::string_t& value,
                                    TokenSink& sink, TokenPoll& poll,
                                    BlockTraits known) {
  return ScanPolled(*this, value, sink, poll, known);
}

template<TokenLayout Layout, Case C, SplitByNonAlphaTokenizer::Options::Chars W,
         bool KnownAscii, typename Poll>
bool SplitByNonAlphaTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink,
                                      Poll poll) {
  using Chars = Options::Chars;
  if constexpr (W == Chars::Ascii) {
    words::SplitByNonAlphaFill<Layout, C, false>(raw, sink, poll);
  } else if constexpr (W == Chars::AsciiBytes) {
    words::SplitByNonAlphaFill<Layout, C, !KnownAscii>(raw, sink, poll);
  } else if constexpr (W == Chars::Whitespace) {
    words::SplitByNonSpaceFill<Layout, C, KnownAscii>(raw, sink, poll);
  } else {
    words::SplitByNonAlnumFill<Layout, C, W == Chars::Letters, KnownAscii>(
      raw, sink, poll);
  }
  return true;
}

template class TypedTokenizer<SplitByNonAlphaTokenizer>;

}  // namespace irs::analysis
