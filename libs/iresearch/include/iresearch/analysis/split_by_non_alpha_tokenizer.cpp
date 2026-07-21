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

#include <duckdb/common/vector/flat_vector.hpp>

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/split_by_non_alpha.hpp"

namespace irs::analysis {

Tokenizer::ptr SplitByNonAlphaTokenizer::Make(Options opts) {
  return std::make_unique<SplitByNonAlphaTokenizer>(opts);
}

template<TokenLayout Layout, bool ToLower>
void SplitByNonAlphaTokenizer::FillValue(TokenEmitter& sink) {
  auto& buf = sink.buf;
  const char* const base = _data.data();
  SplitByNonAlpha(_data, [&](std::string_view token) {
    const auto i = sink.Next();
    if constexpr (ToLower) {
      auto* mem = sink.Reserve(token.size());
      for (size_t j = 0; j < token.size(); ++j) {
        mem[j] = static_cast<byte_type>(
          absl::ascii_tolower(static_cast<unsigned char>(token[j])));
      }
      buf.terms[i] = duckdb::string_t{reinterpret_cast<const char*>(mem),
                                      static_cast<uint32_t>(token.size())};
    } else {
      buf.terms[i] = sink.Intern(ViewCast<byte_type>(token));
    }
    if constexpr (Layout == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = static_cast<uint32_t>(token.data() - base);
      buf.offs_end[i] =
        static_cast<uint32_t>(token.data() - base + token.size());
    }
  });
}

template<TokenLayout Layout>
bool SplitByNonAlphaTokenizer::DoFill(std::string_view value,
                                      TokenEmitter& sink) {
  _data = value;
  if (_options.to_lower) {
    FillValue<Layout, true>(sink);
  } else {
    FillValue<Layout, false>(sink);
  }
  return true;
}

template class TypedTokenizer<SplitByNonAlphaTokenizer>;

}  // namespace irs::analysis
