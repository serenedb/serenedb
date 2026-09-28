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

#include <duckdb/common/types/string_type.hpp>
#include <string_view>

#include "iresearch/analysis/text/case/case.hpp"
#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/words/masks.hpp"
#include "iresearch/analysis/text/words/split_by_non_alpha.hpp"
#include "iresearch/analysis/token_sink.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace irs::analysis::words {

template<TokenLayout Layout, Case C, bool KeepNonAscii>
class CaseRuns {
 public:
  CaseRuns(const char* base, size_t size, TokenSink& sink) noexcept
    : _base{base}, _limit{base + size}, _sink{sink} {}

  IRS_FORCE_INLINE void Fold(size_t offset, classify::Block b) noexcept {
    _ring.Fold(offset, b);
  }

  IRS_FORCE_INLINE void FoldAt(const byte_type* bytes, size_t size,
                               size_t at) noexcept {
    _ring.FoldAt(bytes, size, at);
  }

  IRS_FORCE_INLINE void operator()(size_t begin, size_t end) {
    const Offs offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)};
    const uint32_t n = offs.end - offs.start;
    if constexpr (KeepNonAscii) {
      if (!classify::IsAsciiValue(_base + offs.start, n)) {
        _sink.EmitCaseConvertedUtf8<Layout, C == Case::Lower>(
          std::string_view{_base + offs.start, n}, offs);
        return;
      }
    }
    if (n > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
      if (n < casing::kCaseLane) {
        _sink.EmitSliceCaseConverted<Layout, C == Case::Lower>(_base, _limit,
                                                               offs);
        return;
      }
      _sink.Emit<Layout>(
        n,
        [&](byte_type* out) IRS_FORCE_INLINE {
          casing::CaseConvertAsciiWide<C == Case::Lower>(
            reinterpret_cast<char*>(out), _base + offs.start, n);
          return n;
        },
        offs);
      return;
    }
    const char* const view = _ring.Bytes(begin);
    _sink.Emit<Layout>(view, n, view + kTermViewSlack, offs);
  }

 private:
  const char* _base;
  const char* _limit;
  TokenSink& _sink;
  casing::AsciiFoldRing<C == Case::Lower> _ring;
};

template<TokenLayout Layout, Case C, bool KeepNonAscii>
IRS_NO_INLINE IRS_ALIGN_HOT void SplitByNonAlphaFill(duckdb::string_t raw,
                                                     TokenSink& sink) {
  const char* const base = raw.GetData();
  const size_t size = raw.GetSize();
  const char* const limit = base + size;
  const auto* const bytes = reinterpret_cast<const byte_type*>(base);
  if constexpr (C == Case::None) {
    classify::ForEachRun(
      bytes, size,
      [](const byte_type* block)
        IRS_FORCE_INLINE { return ClassifyAlnumBlock<KeepNonAscii>(block); },
      [&](size_t begin, size_t end) IRS_FORCE_INLINE {
        sink.EmitSlice<Layout>(
          base, limit,
          Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
      });
    return;
  }
  constexpr size_t kBlock = classify::kClassifyBlock;
  CaseRuns<Layout, C, KeepNonAscii> runs{base, size, sink};
  classify::ForEachRun(
    bytes, size,
    [&](const byte_type* block) IRS_FORCE_INLINE {
      const auto b = classify::Load(block);
      const uint32_t mask = ClassifyAlnum<KeepNonAscii>(b);
      if (mask == ~uint32_t{0}) {
        return mask;
      }
      const size_t offset =
        size < kBlock ? 0 : static_cast<size_t>(block - bytes);
      if (offset % kBlock == 0) {
        runs.Fold(offset, b);
        return mask;
      }
      for (size_t at = offset & ~(kBlock - 1); at < size; at += kBlock) {
        runs.FoldAt(bytes, size, at);
      }
      return mask;
    },
    [&](size_t begin, size_t end) IRS_FORCE_INLINE { runs(begin, end); });
}

template<TokenLayout Layout, Case C, bool KnownAscii>
void SplitByNonSpaceFill(duckdb::string_t raw, TokenSink& sink) {
  const char* const base = raw.GetData();
  const auto* const bytes = reinterpret_cast<const byte_type*>(base);
  const size_t size = raw.GetSize();
  if constexpr (C == Case::None) {
    const char* const limit = base + size;
    ForEachNonSpaceRunBest<KnownAscii>(
      bytes, size, [](size_t, classify::Block) IRS_FORCE_INLINE {},
      [&](size_t begin, size_t end) IRS_FORCE_INLINE {
        sink.EmitSlice<Layout>(
          base, limit,
          Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
      });
  } else {
    CaseRuns<Layout, C, !KnownAscii> runs{base, size, sink};
    ForEachNonSpaceRunBest<KnownAscii>(
      bytes, size,
      [&](size_t offset, classify::Block b)
        IRS_FORCE_INLINE { runs.Fold(offset, b); },
      [&](size_t begin, size_t end) IRS_FORCE_INLINE { runs(begin, end); });
  }
}

template<TokenLayout Layout, Case C, bool Letters, bool KnownAscii>
void SplitByNonAlnumFill(duckdb::string_t raw, TokenSink& sink) {
  if constexpr (KnownAscii && !Letters) {
    SplitByNonAlphaFill<Layout, C, false>(raw, sink);
  } else {
    const char* const base = raw.GetData();
    const char* const limit = base + raw.GetSize();
    const auto emit = [&](size_t begin, size_t end) IRS_FORCE_INLINE {
      const Offs offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)};
      const uint32_t n = offs.end - offs.start;
      if constexpr (C == Case::None) {
        sink.EmitSlice<Layout>(base, limit, offs);
      } else if (KnownAscii || classify::IsAsciiValue(base + offs.start, n)) {
        sink.EmitSliceCaseConverted<Layout, C == Case::Lower>(base, limit,
                                                              offs);
      } else {
        sink.EmitCaseConvertedUtf8<Layout, C == Case::Lower>(
          std::string_view{base + offs.start, n}, offs);
      }
    };
    if constexpr (KnownAscii) {
      SplitByNonLetter(raw, emit);
    } else {
      ForEachAlnumRunBest<Letters>(reinterpret_cast<const byte_type*>(base),
                                   raw.GetSize(), emit);
    }
  }
}

}  // namespace irs::analysis::words
