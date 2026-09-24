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

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/sz/stringzilla.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis::normalize {
namespace detail {

inline constexpr auto kLeadSuspicious = uint8_t{1};
inline constexpr auto kLeadPair = uint8_t{2};

constexpr classify::ByteRange Range(uint8_t lo, uint8_t hi) noexcept {
  return {lo, static_cast<byte_type>(hi - lo)};
}

template<sz_normal_form_t Form>
struct FormSpec;

template<>
struct FormSpec<sz_normal_form_nfc_k> {
  static constexpr sz_normal_form_t kDecomposed = sz_normal_form_nfd_k;
  static constexpr classify::ByteRange kQcRanges[] = {
    Range(0xCC, 0xCD), Range(0xD6, 0xD9), Range(0xDB, 0xDD),
    Range(0xDF, 0xE2), Range(0xEA, 0xEA), Range(0xEF, 0xEF)};
  static constexpr uint8_t kPairLeads[] = {0xCE, 0xD2, 0xE3, 0xF0};
  static constexpr classify::ByteRange kStripRanges[] = {
    Range(0xC3, 0xC8), Range(0xCC, 0xD3), Range(0xD6, 0xD9), Range(0xDB, 0xE3),
    Range(0xEA, 0xED), Range(0xEF, 0xF0), Range(0xF3, 0xF3)};
  static constexpr bool PairIsUnsafeByte(uint8_t lead, uint8_t next,
                                         uint8_t third) {
    if (lead == 0xCE) {
      return next == 0x87;
    }
    if (lead == 0xE3) {
      return (next == 0x80 && third >= 0xAA && third <= 0xAF) ||
             (next == 0x82 && (third == 0x99 || third == 0x9A));
    }
    if (lead == 0xF0) {
      return next < 0x9F || next == 0xAF;
    }
    return next >= 0x83 && next <= 0x89;
  }
};

template<>
struct FormSpec<sz_normal_form_nfkc_k> {
  static constexpr sz_normal_form_t kDecomposed = sz_normal_form_nfkd_k;
  static constexpr classify::ByteRange kQcRanges[] = {
    Range(0xC2, 0xC2), Range(0xC4, 0xC5), Range(0xC7, 0xC7),
    Range(0xCA, 0xCD), Range(0xD6, 0xD9), Range(0xDB, 0xDD),
    Range(0xDF, 0xE3), Range(0xEA, 0xEA), Range(0xEF, 0xF0)};
  static constexpr uint8_t kPairLeads[] = {0xCE, 0xCF, 0xD2};
  static constexpr classify::ByteRange kStripRanges[] = {
    Range(0xC2, 0xC8), Range(0xCA, 0xD3), Range(0xD6, 0xD9), Range(0xDB, 0xE3),
    Range(0xEA, 0xED), Range(0xEF, 0xF0), Range(0xF3, 0xF3)};
  static constexpr bool PairIsUnsafeByte(uint8_t lead, uint8_t next, uint8_t) {
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
  for (const auto [lo, span] : FormSpec<Form>::kQcRanges) {
    for (int b = lo; b <= lo + span; ++b) {
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
  for (const auto [lo, span] : FormSpec<Form>::kStripRanges) {
    for (int b = lo; b <= lo + span; ++b) {
      t[b] = true;
    }
  }
  return t;
}();

template<sz_normal_form_t Form>
inline bool PairIsUnsafe(const char* data, size_t n, size_t pos) noexcept {
  if (pos + 1 >= n) {
    return true;
  }
  return FormSpec<Form>::PairIsUnsafeByte(
    static_cast<uint8_t>(data[pos]), static_cast<uint8_t>(data[pos + 1]),
    pos + 2 < n ? static_cast<uint8_t>(data[pos + 2]) : uint8_t{0});
}

template<sz_normal_form_t Form>
inline constexpr auto kQcLeadSet = [] {
  classify::NibbleSet set;
  for (const auto [lo, span] : FormSpec<Form>::kQcRanges) {
    for (int b = lo; b <= lo + span; ++b) {
      set.Add(static_cast<byte_type>(b));
    }
  }
  return set;
}();

template<sz_normal_form_t Form>
IRS_FORCE_INLINE inline uint32_t SuspiciousMask(const char* data, size_t n,
                                                size_t base) noexcept {
  static_assert(kQcLeadSet<Form>.Blockable());
  const auto* block = reinterpret_cast<const byte_type*>(data) + base;
  uint32_t suspicious = classify::ClassifyNibbleBlock(block, kQcLeadSet<Form>);
  classify::VisitSetBits(
    classify::ClassifyAnyEqBlock(block, FormSpec<Form>::kPairLeads),
    [&](uint32_t k) {
      if (PairIsUnsafe<Form>(data, n, base + k)) {
        suspicious |= uint32_t{1} << k;
      }
    });
  return suspicious;
}

inline size_t ContextStart(const char* data, size_t i) noexcept {
  if (i == 0) {
    return 0;
  }
  --i;
  while (i > 0 && (static_cast<uint8_t>(data[i]) & 0xC0) == 0x80) {
    --i;
  }
  return i;
}

IRS_FORCE_INLINE inline size_t SkipAscii(const char* data, size_t n,
                                         size_t i) noexcept {
  const auto* bytes = reinterpret_cast<const byte_type*>(data);
  constexpr size_t kBlock = classify::kClassifyBlock;
  while (i + 2 * kBlock <= n &&
         classify::MoveMask(std::bit_cast<classify::Cmp>(
                              classify::Load(bytes + i) |
                              classify::Load(bytes + i + kBlock)) < 0) == 0) {
    i += 2 * kBlock;
  }
  return i;
}

template<sz_normal_form_t Form>
inline bool SuspiciousLead(const char* data, size_t n, size_t pos) noexcept {
  const uint8_t cls = kLeadClassOf<Form>[static_cast<uint8_t>(data[pos])];
  return cls == kLeadSuspicious ||
         (cls == kLeadPair && PairIsUnsafe<Form>(data, n, pos));
}

template<sz_normal_form_t Form>
inline size_t SafeStarterAfter(const char* data, size_t n,
                               size_t pos) noexcept {
  const auto* bytes = reinterpret_cast<const byte_type*>(data);
  constexpr size_t kBlock = classify::kClassifyBlock;
  for (; pos + kBlock <= n; pos += kBlock) {
    const auto block = classify::Load(bytes + pos);
    const uint32_t safe =
      ~classify::MoveMask((block & uint8_t{0xC0}) == uint8_t{0x80}) &
      ~SuspiciousMask<Form>(data, n, pos);
    if (safe != 0) {
      return pos + std::countr_zero(safe);
    }
  }
  for (; pos < n; ++pos) {
    if ((bytes[pos] & 0xC0) != 0x80 && !SuspiciousLead<Form>(data, n, pos)) {
      return pos;
    }
  }
  return n;
}

}  // namespace detail

template<sz_normal_form_t Form>
constexpr size_t Bound(size_t n) noexcept {
  constexpr bool kCompat =
    Form == sz_normal_form_nfkc_k || Form == sz_normal_form_nfkd_k;
  return 64 + n * (kCompat ? 18 : 4);
}

template<sz_normal_form_t Form>
inline bool Denormalized(const char* data, size_t n) noexcept {
  using namespace detail;
  size_t i = 0;
  while (i + classify::kClassifyBlock <= n) {
    i = SkipAscii(data, n, i);
    if (i + classify::kClassifyBlock > n) {
      break;
    }
    const uint32_t suspicious = SuspiciousMask<Form>(data, n, i);
    if (suspicious == 0) {
      i += classify::kClassifyBlock;
      continue;
    }
    const size_t first = i + std::countr_zero(suspicious);
    const size_t end = SafeStarterAfter<Form>(data, n, first + 1);
    const size_t start = ContextStart(data, first);
    if (sz::FindDenormalized(data + start, end - start, Form) != nullptr) {
      return true;
    }
    i = end;
  }
  for (size_t j = i; j < n; ++j) {
    if (SuspiciousLead<Form>(data, n, j)) {
      const size_t start = ContextStart(data, j);
      return sz::FindDenormalized(data + start, n - start, Form) != nullptr;
    }
  }
  return false;
}

template<sz_normal_form_t Form>
inline bool StripSafe(const char* data, size_t n) noexcept {
  const auto* bytes = reinterpret_cast<const byte_type*>(data);
  size_t i = 0;
  while (i + classify::kClassifyBlock <= n) {
    i = detail::SkipAscii(data, n, i);
    if (i + classify::kClassifyBlock > n) {
      break;
    }
    if (classify::ClassifyAnyInRangeBlock(
          bytes + i, detail::FormSpec<Form>::kStripRanges) != 0) {
      return false;
    }
    i += classify::kClassifyBlock;
  }
  for (; i < n; ++i) {
    if (detail::kStripUnsafeLeadOf<Form>[bytes[i]]) {
      return false;
    }
  }
  return true;
}

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
