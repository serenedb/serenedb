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

#include <absl/strings/ascii.h>

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/split_by_non_alpha.hpp"

namespace irs::analysis {

Tokenizer::ptr SplitByNonAlphaTokenizer::Make(Options opts) {
  return std::make_unique<SplitByNonAlphaTokenizer>(opts);
}

template<TokenLayout Layout, Case C>
bool SplitByNonAlphaTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const char* const base = raw.GetData();
  const char* const end = base + raw.GetSize();
  if constexpr (C == Case::None) {
    SplitByNonAlpha(raw, [&](std::string_view token) {
      const auto size = static_cast<uint32_t>(token.size());
      const auto start = static_cast<uint32_t>(token.data() - base);
      // token is a view into the input, stable for the whole chunk: no copy
      sink.Emit<Layout>(MakeTermView(token.data(), size, end),
                        Offs{start, start + size});
    });
    return true;
  }
  SplitByNonAlpha(raw, [&](std::string_view token) {
    const auto size = static_cast<uint32_t>(token.size());
    const auto start = static_cast<uint32_t>(token.data() - base);
    if (size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      // inline term: fold the built slot in-register, no arena traffic
      sink.Emit<Layout>(FoldTermViewAscii<C == Case::Lower>(
                          MakeTermView(token.data(), size, end)),
                        Offs{start, start + size});
    } else {
      // fold from the input straight into the sink in a single pass
      sink.Emit<Layout>(
        size,
        [&](byte_type* dst) IRS_FORCE_INLINE {
          if constexpr (C == Case::Lower) {
            absl::ascii_internal::AsciiStrToLower(reinterpret_cast<char*>(dst),
                                                  token.data(), size);
          } else {
            absl::ascii_internal::AsciiStrToUpper(reinterpret_cast<char*>(dst),
                                                  token.data(), size);
          }
          return size;
        },
        Offs{start, start + size});
    }
  });
  return true;
}

template class TypedTokenizer<SplitByNonAlphaTokenizer>;

}  // namespace irs::analysis
