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
///
/// Normalization transforms are provided by the vendored StringZilla UAX#15
/// engine (third_party/stringzilla, Apache-2.0), conformance-gated by
/// normalize_tests.cpp against UCD 17 NormalizationTest.txt.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/sz/stringzilla.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis::normalize {
namespace detail {

inline constexpr auto kLeadSuspicious = uint8_t{1};
inline constexpr auto kLeadPair = uint8_t{2};

using LeadRange = std::pair<uint8_t, uint8_t>;

// Per-form UTF-8 lead-byte sets derived from UCD 17: kQcRanges = <form>_QC
// != Yes or ccc != 0 under the lead; kPairLeads = refined by second byte;
// kStripRanges adds decompositions and Mn, so outside them accent stripping
// is an identity.
template<sz_normal_form_t Form>
struct FormSpec;

template<>
struct FormSpec<sz_normal_form_nfc_k> {
  static constexpr sz_normal_form_t kDecomposed = sz_normal_form_nfd_k;
  static constexpr LeadRange kQcRanges[] = {{0xCC, 0xCD}, {0xD6, 0xD9},
                                            {0xDB, 0xDD}, {0xDF, 0xE3},
                                            {0xEA, 0xEA}, {0xEF, 0xF0}};
  static constexpr uint8_t kPairLeads[] = {0xCE, 0xD2};
  static constexpr LeadRange kStripRanges[] = {
    {0xC3, 0xC8}, {0xCC, 0xD3}, {0xD6, 0xD9}, {0xDB, 0xE3},
    {0xEA, 0xED}, {0xEF, 0xF0}, {0xF3, 0xF3}};
  // U+0387 = CE 87; U+0483..0489 = D2 83..89
  static constexpr bool PairIsUnsafeByte(uint8_t lead, uint8_t next) {
    if (lead == 0xCE) {
      return next == 0x87;
    }
    return next >= 0x83 && next <= 0x89;
  }
};

template<>
struct FormSpec<sz_normal_form_nfkc_k> {
  static constexpr sz_normal_form_t kDecomposed = sz_normal_form_nfkd_k;
  static constexpr LeadRange kQcRanges[] = {
    {0xC2, 0xC2}, {0xC4, 0xC5}, {0xC7, 0xC7}, {0xCA, 0xCD}, {0xD6, 0xD9},
    {0xDB, 0xDD}, {0xDF, 0xE3}, {0xEA, 0xEA}, {0xEF, 0xF0}};
  static constexpr uint8_t kPairLeads[] = {0xCE, 0xCF, 0xD2};
  static constexpr LeadRange kStripRanges[] = {
    {0xC2, 0xC8}, {0xCA, 0xD3}, {0xD6, 0xD9}, {0xDB, 0xE3},
    {0xEA, 0xED}, {0xEF, 0xF0}, {0xF3, 0xF3}};
  // U+0384/0385/0387 = CE 84/85/87; Greek symbol variants start at U+03D0 =
  // CF 90 (lowercase Greek CF 80..8F stays fast); U+0483..0487 = D2 83..87
  static constexpr bool PairIsUnsafeByte(uint8_t lead, uint8_t next) {
    if (lead == 0xCE) {
      return next == 0x84 || next == 0x85 || next == 0x87;
    }
    if (lead == 0xCF) {
      return next >= 0x90;
    }
    return next >= 0x83 && next <= 0x87;
  }
};

template<sz_normal_form_t Form>
inline constexpr auto kLeadClassOf = [] {
  std::array<uint8_t, 256> t{};
  for (const auto& [lo, hi] : FormSpec<Form>::kQcRanges) {
    for (int b = lo; b <= hi; ++b) {
      t[b] = kLeadSuspicious;
    }
  }
  for (const uint8_t lead : FormSpec<Form>::kPairLeads) {
    t[lead] = kLeadPair;
  }
  return t;
}();

template<sz_normal_form_t Form>
inline constexpr auto kStripUnsafeLeadOf = [] {
  std::array<bool, 256> t{};
  for (const auto& [lo, hi] : FormSpec<Form>::kStripRanges) {
    for (int b = lo; b <= hi; ++b) {
      t[b] = true;
    }
  }
  return t;
}();

struct LeadMasks {
  uint32_t suspicious;
  uint32_t pair;
};

template<sz_normal_form_t Form>
IRS_FORCE_INLINE inline LeadMasks ClassifyLeadBlock(
  const byte_type* block) noexcept {
  const auto b = classify::Load(block);
  classify::Cmp suspicious{};
  for (const auto& [lo, hi] : FormSpec<Form>::kQcRanges) {
    suspicious |= (b >= lo) & (b <= hi);
  }
  return {classify::MoveMask(suspicious),
          classify::ClassifyAnyEqBlock(block, FormSpec<Form>::kPairLeads)};
}

template<sz_normal_form_t Form>
IRS_FORCE_INLINE inline uint32_t StripUnsafeMask(
  const byte_type* block) noexcept {
  const auto b = classify::Load(block);
  classify::Cmp unsafe_lead{};
  for (const auto& [lo, hi] : FormSpec<Form>::kStripRanges) {
    unsafe_lead |= (b >= lo) & (b <= hi);
  }
  return classify::MoveMask(unsafe_lead);
}

template<sz_normal_form_t Form>
inline bool PairIsUnsafe(const char* data, size_t n, size_t pos) noexcept {
  if (pos + 1 >= n) {
    return true;
  }
  return FormSpec<Form>::PairIsUnsafeByte(static_cast<uint8_t>(data[pos]),
                                          static_cast<uint8_t>(data[pos + 1]));
}

template<sz_normal_form_t Form>
IRS_FORCE_INLINE inline uint32_t ResolvePairs(const char* data, size_t n,
                                              size_t base, uint32_t suspicious,
                                              uint32_t pair) noexcept {
  classify::VisitSetBits(pair, [&](uint32_t k) {
    if (PairIsUnsafe<Form>(data, n, base + k)) {
      suspicious |= uint32_t{1} << k;
    }
  });
  return suspicious;
}

inline size_t CpStartBefore(const char* data, size_t i) noexcept {
  while (i > 0 && (static_cast<uint8_t>(data[i]) & 0xC0) == 0x80) {
    --i;
  }
  return i;
}

}  // namespace detail

// Destination bound for Compose/Decompose per sz_utf8_norm's contract: the
// compatibility forms decompose a single codepoint into up to 18x its bytes
// and the kernel writes unchecked, so an NFKC/NFKD destination must carry the
// full factor; canonical forms stay within 4x.
template<sz_normal_form_t Form>
constexpr size_t Bound(size_t n) noexcept {
  constexpr bool kCompat =
    Form == sz_normal_form_nfkc_k || Form == sz_normal_form_nfkd_k;
  return 64 + n * (kCompat ? 18 : 4);
}

// Suspicious-lead SIMD prefilter passes whole windows without decoding;
// dirty runs resolve via the per-codepoint engine with ONE codepoint of left
// context, which suffices because clean windows hold only ccc=0 starters.
template<sz_normal_form_t Form>
inline bool Denormalized(const char* data, size_t n) noexcept {
  using namespace detail;
  size_t i = 0;
  while (i + classify::kClassifyBlock <= n) {
    const auto m =
      ClassifyLeadBlock<Form>(reinterpret_cast<const byte_type*>(data) + i);
    if (ResolvePairs<Form>(data, n, i, m.suspicious, m.pair) == 0) {
      i += classify::kClassifyBlock;
      continue;
    }
    size_t end = i + classify::kClassifyBlock;
    while (end + classify::kClassifyBlock <= n) {
      const auto next =
        ClassifyLeadBlock<Form>(reinterpret_cast<const byte_type*>(data) + end);
      if (ResolvePairs<Form>(data, n, end, next.suspicious, next.pair) == 0) {
        break;
      }
      end += classify::kClassifyBlock;
    }
    while (end < n && (static_cast<uint8_t>(data[end]) & 0xC0) == 0x80) {
      ++end;
    }
    const size_t start = CpStartBefore(data, i == 0 ? 0 : i - 1);
    if (sz_utf8_find_denormalized_serial(data + start, end - start, Form) !=
        nullptr) {
      return true;
    }
    i = end;
  }
  for (size_t j = i; j < n; ++j) {
    const uint8_t cls = kLeadClassOf<Form>[static_cast<uint8_t>(data[j])];
    if (cls == kLeadSuspicious ||
        (cls == kLeadPair && PairIsUnsafe<Form>(data, n, j))) {
      const size_t start = CpStartBefore(data, i == 0 ? 0 : i - 1);
      if (sz_utf8_find_denormalized_serial(data + start, n - start, Form) !=
          nullptr) {
        return true;
      }
      break;
    }
  }
  return false;
}

// True iff accent stripping is an identity; implies already composed.
template<sz_normal_form_t Form>
inline bool StripSafe(const char* data, size_t n) noexcept {
  size_t i = 0;
  while (i + classify::kClassifyBlock <= n) {
    if (detail::StripUnsafeMask<Form>(reinterpret_cast<const byte_type*>(data) +
                                      i) != 0) {
      return false;
    }
    i += classify::kClassifyBlock;
  }
  for (; i < n; ++i) {
    if (detail::kStripUnsafeLeadOf<Form>[static_cast<uint8_t>(data[i])]) {
      return false;
    }
  }
  return true;
}

// `out` capacity must be at least Bound<Form>(in.size()).
template<sz_normal_form_t Form>
inline size_t Compose(std::string_view in, char* out) noexcept {
  return sz::Norm(in.data(), in.size(), Form, out);
}

template<sz_normal_form_t Form>
inline size_t Decompose(std::string_view in, char* out) noexcept {
  return sz::Norm(in.data(), in.size(), detail::FormSpec<Form>::kDecomposed,
                  out);
}

void StripNonspacingMarks(std::string_view in, std::string& out);

}  // namespace irs::analysis::normalize
