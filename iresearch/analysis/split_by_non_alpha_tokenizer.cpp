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

#include "iresearch/analysis/text/case/case.hpp"
#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/words/masks.hpp"
#include "iresearch/analysis/text/words/split_by_non_alpha.hpp"

namespace irs::analysis {

Tokenizer::ptr SplitByNonAlphaTokenizer::Make(Options opts) {
  return std::make_unique<SplitByNonAlphaTokenizer>(opts);
}

template<TokenLayout Layout, Case C>
bool SplitByNonAlphaTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const char* const base = raw.GetData();
  const size_t size = raw.GetSize();
  const char* const limit = base + size;
  if constexpr (C == Case::None) {
    words::SplitByNonAlpha(raw, [&](size_t begin, size_t end) IRS_FORCE_INLINE {
      sink.EmitSlice<Layout>(
        base, limit,
        Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
    });
    return true;
  }
  constexpr size_t kBlock = classify::kClassifyBlock;
  const auto* const bytes = reinterpret_cast<const byte_type*>(base);
  casing::AsciiFoldRing<C == Case::Lower> ring;
  classify::ForEachRun(
    bytes, size,
    [&](const byte_type* block) IRS_FORCE_INLINE {
      const auto b = classify::Load(block);
      const uint32_t mask = words::ClassifyAlnum(b);
      if (mask == ~uint32_t{0}) {
        return mask;
      }
      const size_t offset =
        size < kBlock ? 0 : static_cast<size_t>(block - bytes);
      if (offset % kBlock == 0) {
        ring.Fold(offset, b);
        return mask;
      }
      for (size_t at = offset & ~(kBlock - 1); at < size; at += kBlock) {
        ring.FoldAt(bytes, size, at);
      }
      return mask;
    },
    [&](size_t begin, size_t end) IRS_FORCE_INLINE {
      const Offs offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)};
      const uint32_t n = offs.end - offs.start;
      if (n > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
        if (n < casing::kCaseLane) {
          sink.EmitSliceCaseConverted<Layout, C == Case::Lower>(base, limit,
                                                                offs);
          return;
        }
        sink.Emit<Layout>(
          n,
          [&](byte_type* out) IRS_FORCE_INLINE {
            casing::CaseConvertAsciiWide<C == Case::Lower>(
              reinterpret_cast<char*>(out), base + offs.start, n);
            return n;
          },
          offs);
        return;
      }
      const char* const view = ring.Bytes(begin);
      sink.Emit<Layout>(view, n, view + kTermViewSlack, offs);
    });
  return true;
}

template class TypedTokenizer<SplitByNonAlphaTokenizer>;

}  // namespace irs::analysis
