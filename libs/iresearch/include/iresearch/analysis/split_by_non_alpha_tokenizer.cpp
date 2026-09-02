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

#include "iresearch/analysis/text/words/split_by_non_alpha.hpp"

namespace irs::analysis {

Tokenizer::ptr SplitByNonAlphaTokenizer::Make(Options opts) {
  return std::make_unique<SplitByNonAlphaTokenizer>(opts);
}

template<TokenLayout Layout, Case C>
bool SplitByNonAlphaTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const char* const base = raw.GetData();
  const char* const limit = base + raw.GetSize();
  words::SplitByNonAlpha(raw, [&](size_t begin, size_t end) IRS_FORCE_INLINE {
    const Offs offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)};
    if constexpr (C == Case::None) {
      sink.EmitSlice<Layout>(base, limit, offs);
    } else {
      sink.EmitSliceCaseConverted<Layout, C == Case::Lower>(base, limit, offs);
    }
  });
  return true;
}

template class TypedTokenizer<SplitByNonAlphaTokenizer>;

}  // namespace irs::analysis
