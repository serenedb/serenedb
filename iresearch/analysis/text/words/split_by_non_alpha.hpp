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

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <duckdb/common/types/string_type.hpp>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/words/masks.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/utf8_character_utils.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis::words {
namespace detail {

struct WordCodePoints {
  static constexpr uint32_t kEnd = 0x30000;

  std::array<uint64_t, kEnd / 64> alnum{};
  std::array<uint64_t, kEnd / 64> letters{};
};

inline const WordCodePoints& WordCodePointTable() noexcept {
  static const WordCodePoints kTable = [] {
    WordCodePoints table;
    for (uint32_t c = 0x80; c < WordCodePoints::kEnd; ++c) {
      const char category = utf8_utils::CharPrimaryCategory(c);
      const uint64_t bit = uint64_t{1} << (c & 63);
      if (category == 'L' || category == 'M') {
        table.alnum[c >> 6] |= bit;
        table.letters[c >> 6] |= bit;
      } else if (category == 'N') {
        table.alnum[c >> 6] |= bit;
      }
    }
    return table;
  }();
  return kTable;
}

template<bool Letters>
IRS_FORCE_INLINE bool IsWordCodePoint(const WordCodePoints& table,
                                      uint32_t c) noexcept {
  if (c < WordCodePoints::kEnd) [[likely]] {
    const auto& bits = Letters ? table.letters : table.alnum;
    return ((bits[c >> 6] >> (c & 63)) & 1) != 0;
  }
  if (c > 0x10FFFF) {
    return false;
  }
  const char category = utf8_utils::CharPrimaryCategory(c);
  return category == 'L' || category == 'M' || (!Letters && category == 'N');
}

IRS_FORCE_INLINE inline uint32_t AsciiSpaceBits(classify::Block v) noexcept {
  return classify::MoveMask((v == uint8_t{' '}) |
                            (v - uint8_t{0x09} <= uint8_t{0x04}));
}

IRS_FORCE_INLINE inline uint64_t WideSpaceBits(classify::Block v0,
                                               classify::Block v1,
                                               classify::Block v2) noexcept {
  const classify::Cmp next80 = v1 == uint8_t{0x80};
  const classify::Cmp last80 = v2 == uint8_t{0x80};
  const classify::Cmp e2 = v0 == uint8_t{0xE2};
  const classify::Cmp two =
    (v0 == uint8_t{0xC2}) & ((v1 == uint8_t{0x85}) | (v1 == uint8_t{0xA0}));
  const classify::Cmp three =
    ((v0 == uint8_t{0xE1}) & (v1 == uint8_t{0x9A}) & last80) |
    (e2 & next80 &
     ((v2 - uint8_t{0x80} <= uint8_t{0x0A}) |
      ((v2 & uint8_t{0xFE}) == uint8_t{0xA8}) | (v2 == uint8_t{0xAF}))) |
    (e2 & (v1 == uint8_t{0x81}) & (v2 == uint8_t{0x9F})) |
    ((v0 == uint8_t{0xE3}) & next80 & last80);
  const uint64_t s2 = classify::MoveMask(two);
  const uint64_t s3 = classify::MoveMask(three);
  return s2 | (s2 << 1) | s3 | (s3 << 1) | (s3 << 2);
}

IRS_FORCE_INLINE inline bool HasSpaceLead(classify::Block v) noexcept {
  return classify::MoveMask((v == uint8_t{0xC2}) |
                            (v - uint8_t{0xE1} <= uint8_t{0x02})) != 0;
}

IRS_FORCE_INLINE inline classify::Block LoadUpTo(const byte_type* data,
                                                 size_t size) noexcept {
  return size >= classify::kClassifyBlock ? classify::Load(data)
                                          : classify::LoadPadded(data, size);
}

}  // namespace detail

template<typename OnRun>
class RunSteps {
 public:
  explicit RunSteps(OnRun& on_run) noexcept : _on_run{on_run} {}

  template<typename Mask>
  IRS_FORCE_INLINE void Step(Mask mask, size_t base) {
    const Mask before = (mask << 1) | Mask{_open};
    Mask starts = mask & ~before;
    Mask ends = ~mask & before;
    if (_open && ends != 0) {
      _on_run(_begin, base + std::countr_zero(ends));
      ends &= ends - 1;
      _open = false;
    }
    while (ends != 0) {
      _on_run(base + std::countr_zero(starts), base + std::countr_zero(ends));
      starts &= starts - 1;
      ends &= ends - 1;
    }
    if (starts != 0) {
      _begin = base + std::countr_zero(starts);
      _open = true;
    }
  }

  IRS_FORCE_INLINE void Finish(size_t size) {
    if (_open) {
      _on_run(_begin, size);
    }
  }

 private:
  OnRun& _on_run;
  size_t _begin = 0;
  bool _open = false;
};

template<bool KnownAscii, typename OnBlock, typename OnRun>
IRS_FORCE_INLINE void ForEachNonSpaceRun(const byte_type* data, size_t size,
                                         OnBlock&& on_block, OnRun&& on_run) {
  constexpr size_t kBlock = classify::kClassifyBlock;
  constexpr size_t kAhead = KnownAscii ? 0 : 2;
  RunSteps runs{on_run};
  uint64_t carry = 0;
  const auto wide_of = [&](classify::Block v0, classify::Block v1,
                           classify::Block v2) IRS_FORCE_INLINE {
    const uint64_t wide = detail::WideSpaceBits(v0, v1, v2) | carry;
    carry = wide >> kBlock;
    return static_cast<uint32_t>(wide);
  };
  const auto spill = [&] IRS_FORCE_INLINE {
    const auto bits = static_cast<uint32_t>(carry);
    carry = 0;
    return bits;
  };
  const auto full = [&](size_t at) IRS_FORCE_INLINE {
    const auto* p = data + at;
    const auto v0 = classify::Load(p);
    uint32_t space = detail::AsciiSpaceBits(v0);
    if constexpr (!KnownAscii) {
      space |= detail::HasSpaceLead(v0)
                 ? wide_of(v0, classify::Load(p + 1), classify::Load(p + 2))
                 : spill();
    }
    if (space != 0) {
      on_block(at, v0);
    }
    return ~space;
  };
  size_t base = 0;
  for (; base + 2 * kBlock + kAhead <= size; base += 2 * kBlock) {
    const uint32_t lo = full(base);
    const uint32_t hi = full(base + kBlock);
    runs.Step(uint64_t{lo} | (uint64_t{hi} << kBlock), base);
  }
  for (; base < size; base += kBlock) {
    const size_t left = size - base;
    const auto* p = data + base;
    const auto v0 = detail::LoadUpTo(p, left);
    uint32_t space = detail::AsciiSpaceBits(v0);
    if constexpr (!KnownAscii) {
      space |= detail::HasSpaceLead(v0)
                 ? wide_of(v0, detail::LoadUpTo(p + 1, left - 1),
                           detail::LoadUpTo(p + 2, left - 1 - (left > 1)))
                 : spill();
    }
    on_block(base, v0);
    runs.Step(
      ~space & (left >= kBlock ? ~uint32_t{0} : classify::LowBits(left)), base);
  }
  runs.Finish(size);
}

#if defined(__x86_64__)
namespace detail {

IRS_TARGET_AVX512 IRS_FORCE_INLINE inline __m512i Load64UpTo(
  const byte_type* data, size_t size) noexcept {
  return size >= 64
           ? _mm512_loadu_si512(data)
           : _mm512_maskz_loadu_epi8(
               _bzhi_u64(~uint64_t{0}, static_cast<uint32_t>(size)), data);
}

IRS_TARGET_AVX512 IRS_FORCE_INLINE inline uint64_t AsciiSpaceBits64(
  __m512i v) noexcept {
  return _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(' ')) |
         _mm512_cmple_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8(0x09)),
                                _mm512_set1_epi8(0x04));
}

IRS_TARGET_AVX512 IRS_FORCE_INLINE inline uint64_t ByteMask(
  __m512i v, uint8_t b) noexcept {
  return _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(static_cast<char>(b)));
}

IRS_TARGET_AVX512 IRS_FORCE_INLINE inline unsigned __int128 WideSpaceBits64(
  __m512i v0, __m512i v1, __m512i v2) noexcept {
  const uint64_t next80 = ByteMask(v1, 0x80);
  const uint64_t last80 = ByteMask(v2, 0x80);
  const uint64_t e2 = ByteMask(v0, 0xE2);
  const uint64_t spaces =
    _mm512_cmple_epu8_mask(_mm512_sub_epi8(v2, _mm512_set1_epi8(-0x80)),
                           _mm512_set1_epi8(0x0A)) |
    ByteMask(_mm512_and_si512(v2, _mm512_set1_epi8(static_cast<char>(0xFE))),
             0xA8) |
    ByteMask(v2, 0xAF);
  const unsigned __int128 two =
    ByteMask(v0, 0xC2) & (ByteMask(v1, 0x85) | ByteMask(v1, 0xA0));
  const unsigned __int128 three =
    (ByteMask(v0, 0xE1) & ByteMask(v1, 0x9A) & last80) |
    (e2 & next80 & spaces) | (e2 & ByteMask(v1, 0x81) & ByteMask(v2, 0x9F)) |
    (ByteMask(v0, 0xE3) & next80 & last80);
  return two | (two << 1) | three | (three << 1) | (three << 2);
}

}  // namespace detail

template<bool KnownAscii, typename OnBlock, typename OnRun>
IRS_TARGET_AVX512 IRS_FORCE_INLINE void ForEachNonSpaceRun512(
  const byte_type* data, size_t size, OnBlock&& on_block, OnRun&& on_run) {
  constexpr size_t kBlock = 64;
  RunSteps runs{on_run};
  uint64_t carry = 0;
  const auto full = [&](const byte_type* p) IRS_TARGET_AVX512 IRS_FORCE_INLINE {
    const __m512i v0 = _mm512_loadu_si512(p);
    uint64_t space = detail::AsciiSpaceBits64(v0);
    if constexpr (!KnownAscii) {
      if (_mm512_movepi8_mask(v0) != 0) {
        const auto wide = detail::WideSpaceBits64(v0, _mm512_loadu_si512(p + 1),
                                                  _mm512_loadu_si512(p + 2)) |
                          carry;
        space |= static_cast<uint64_t>(wide);
        carry = static_cast<uint64_t>(wide >> 64);
      }
    }
    if (space != 0) {
      const auto at = static_cast<size_t>(p - data);
      on_block(at, std::bit_cast<classify::Block>(_mm512_castsi512_si256(v0)));
      on_block(
        at + classify::kClassifyBlock,
        std::bit_cast<classify::Block>(_mm512_extracti64x4_epi64(v0, 1)));
    }
    return ~space;
  };
  size_t base = 0;
  for (; base + 2 * kBlock + 2 <= size; base += 2 * kBlock) {
    const uint64_t lo = full(data + base);
    const uint64_t hi = full(data + base + kBlock);
    runs.Step(lo, base);
    runs.Step(hi, base + kBlock);
  }
  for (; base < size; base += kBlock) {
    const size_t left = size - base;
    const auto* p = data + base;
    const bool full = left >= kBlock + 2;
    const __m512i v0 =
      full ? _mm512_loadu_si512(p) : detail::Load64UpTo(p, left);
    uint64_t space = detail::AsciiSpaceBits64(v0);
    if constexpr (!KnownAscii) {
      if (_mm512_movepi8_mask(v0) != 0) {
        const __m512i v1 = full ? _mm512_loadu_si512(p + 1)
                                : detail::Load64UpTo(p + 1, left - 1);
        const __m512i v2 = full
                             ? _mm512_loadu_si512(p + 2)
                             : detail::Load64UpTo(p + 2, left - 1 - (left > 1));
        const auto wide = detail::WideSpaceBits64(v0, v1, v2) | carry;
        space |= static_cast<uint64_t>(wide);
        carry = static_cast<uint64_t>(wide >> 64);
      }
    }
    if (space != 0 || !full) {
      on_block(base,
               std::bit_cast<classify::Block>(_mm512_castsi512_si256(v0)));
      on_block(
        base + classify::kClassifyBlock,
        std::bit_cast<classify::Block>(_mm512_extracti64x4_epi64(v0, 1)));
    }
    runs.Step(
      ~space &
        (left >= kBlock ? ~uint64_t{0}
                        : _bzhi_u64(~uint64_t{0}, static_cast<uint32_t>(left))),
      base);
  }
  runs.Finish(size);
}

template<bool KnownAscii, typename OnBlock, typename OnRun>
IRS_TARGET_AVX512 IRS_NO_INLINE void ForEachNonSpaceRunWide(
  const byte_type* data, size_t size, OnBlock& on_block, OnRun& on_run) {
  ForEachNonSpaceRun512<KnownAscii>(data, size, on_block, on_run);
}
#endif

template<bool KnownAscii, typename OnBlock, typename OnRun>
IRS_FORCE_INLINE void ForEachNonSpaceRunBest(const byte_type* data, size_t size,
                                             OnBlock&& on_block,
                                             OnRun&& on_run) {
#if defined(__x86_64__)
  if (classify::HasAvx512Bw()) {
    ForEachNonSpaceRunWide<KnownAscii>(data, size, on_block, on_run);
    return;
  }
#endif
  ForEachNonSpaceRun<KnownAscii>(data, size, on_block, on_run);
}

template<bool KeepNonAscii = false, typename EmitFn>
IRS_FORCE_INLINE void SplitByNonAlpha(duckdb::string_t data, EmitFn&& emit) {
  classify::ForEachRun(
    reinterpret_cast<const byte_type*>(data.GetData()), data.GetSize(),
    [](const byte_type* block)
      IRS_FORCE_INLINE { return ClassifyAlnumBlock<KeepNonAscii>(block); },
    emit);
}

template<typename EmitFn>
IRS_FORCE_INLINE void SplitByNonLetter(duckdb::string_t data, EmitFn&& emit) {
  classify::ForEachRun(
    reinterpret_cast<const byte_type*>(data.GetData()), data.GetSize(),
    [](const byte_type* block)
      IRS_FORCE_INLINE { return ClassifyAlphaBlock(block); },
    emit);
}

namespace detail {

struct SureWordLeads {
  uint32_t two;
  uint32_t three;
};

IRS_FORCE_INLINE inline SureWordLeads SureWordLeadsOf(
  classify::Block v0, classify::Block v1) noexcept {
  const classify::Block low1 = v1 & uint8_t{0x3F};
  const classify::Cmp two =
    (v0 - uint8_t{0xC4} <= uint8_t{0x05}) |
    (v0 - uint8_t{0xD0} <= uint8_t{0x01}) | (v0 == uint8_t{0xD3}) |
    ((v0 == uint8_t{0xC3}) & ((low1 & uint8_t{0x1F}) != uint8_t{0x17}));
  const classify::Cmp three =
    (v0 - uint8_t{0xE5} <= uint8_t{0x04}) |
    (v0 - uint8_t{0xEB} <= uint8_t{0x01}) |
    ((v0 == uint8_t{0xE4}) & (low1 != uint8_t{0x37})) |
    ((v0 == uint8_t{0xEA}) & (low1 >= uint8_t{0x30})) |
    ((v0 == uint8_t{0xED}) & (low1 <= uint8_t{0x1D}));
  return {classify::MoveMask(two), classify::MoveMask(three)};
}

template<bool Letters>
IRS_FORCE_INLINE inline uint32_t WordLength(const WordCodePoints& table,
                                            const byte_type* it,
                                            const byte_type* end) noexcept {
  const uint32_t b0 = it[0];
  const auto left = end - it;
  const auto& bits = Letters ? table.letters : table.alnum;
  if (b0 < 0xE0) {
    if (left < 2) {
      return 0;
    }
    return (bits[b0 & 0x1F] >> (it[1] & 0x3F)) & 1 ? 2 : 0;
  }
  if (b0 < 0xF0) {
    if (left < 3) {
      return 0;
    }
    return (bits[((b0 & 0x0F) << 6) | (it[1] & 0x3F)] >> (it[2] & 0x3F)) & 1
             ? 3
             : 0;
  }
  if (b0 >= 0xF8 || left < 4) {
    return 0;
  }
  const uint32_t cp = ((b0 & 0x07) << 18) | ((it[1] & 0x3F) << 12) |
                      ((it[2] & 0x3F) << 6) | (it[3] & 0x3F);
  return IsWordCodePoint<Letters>(table, cp) ? 4 : 0;
}

}  // namespace detail

template<bool Letters, typename OnRun>
IRS_FORCE_INLINE void ForEachAlnumRun(const byte_type* data, size_t size,
                                      OnRun&& on_run) {
  constexpr size_t kBlock = classify::kClassifyBlock;
  constexpr size_t kAhead = 3;
  const auto& table = detail::WordCodePointTable();
  RunSteps runs{on_run};
  uint64_t carry = 0;
  const auto word_of = [&](const byte_type* p, classify::Block v0,
                           classify::Block v1, uint32_t two_valid,
                           uint32_t three_valid) IRS_FORCE_INLINE {
    const auto cmps = detail::WordCmpsOf(v0);
    const uint32_t ascii = Letters
                             ? classify::MoveMask(cmps.alpha)
                             : classify::MoveMask(cmps.alpha | cmps.digit);
    const uint32_t high =
      classify::MoveMask(std::bit_cast<classify::Cmp>(v0) < 0);
    if (high == 0) {
      const auto bits = ascii | static_cast<uint32_t>(carry);
      carry = 0;
      return bits;
    }
    const uint32_t cont =
      classify::MoveMask((v0 & uint8_t{0xC0}) == uint8_t{0x80});
    const auto sure = detail::SureWordLeadsOf(v0, v1);
    const uint64_t two = sure.two & two_valid;
    const uint64_t three = sure.three & three_valid;
    uint64_t word =
      carry | two | (two << 1) | three | (three << 1) | (three << 2);
    uint32_t rest = high & ~cont & ~static_cast<uint32_t>(two | three);
    while (rest != 0) {
      const auto at = static_cast<uint32_t>(std::countr_zero(rest));
      rest &= rest - 1;
      const uint32_t len =
        detail::WordLength<Letters>(table, p + at, data + size);
      word |= ((uint64_t{1} << len) - 1) << at;
    }
    carry = word >> kBlock;
    return ascii | static_cast<uint32_t>(word);
  };
  size_t base = 0;
  for (; base + 2 * kBlock + kAhead <= size; base += 2 * kBlock) {
    const auto* p = data + base;
    const uint32_t lo = word_of(p, classify::Load(p), classify::Load(p + 1),
                                ~uint32_t{0}, ~uint32_t{0});
    const uint32_t hi =
      word_of(p + kBlock, classify::Load(p + kBlock),
              classify::Load(p + kBlock + 1), ~uint32_t{0}, ~uint32_t{0});
    runs.Step(uint64_t{lo} | (uint64_t{hi} << kBlock), base);
  }
  for (; base < size; base += kBlock) {
    const size_t left = size - base;
    const auto* p = data + base;
    const auto valid = [](size_t n) IRS_FORCE_INLINE {
      return n >= kBlock ? ~uint32_t{0} : classify::LowBits(n);
    };
    const uint32_t mask =
      word_of(p, detail::LoadUpTo(p, left), detail::LoadUpTo(p + 1, left - 1),
              valid(left - 1), left >= 2 ? valid(left - 2) : 0);
    runs.Step(mask & valid(left), base);
  }
  runs.Finish(size);
}

}  // namespace irs::analysis::words
