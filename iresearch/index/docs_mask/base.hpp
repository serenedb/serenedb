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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <bit>
#include <concepts>
#include <cstdint>
#include <type_traits>

#include "iresearch/index/docs_mask/kernels.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

template<MaskKind K>
class DocsMask;

struct MaskedSpan {
  doc_id_t first;
  doc_id_t last;
};

template<typename T>
concept DocsMaskType =
  requires { T::kKind; } && std::same_as<T, DocsMask<T::kKind>> &&
  !std::is_polymorphic_v<T> &&
  requires(T& mask, doc_id_t doc, doc_id_t* docs, score_t* scores, uint32_t len,
           uint64_t* words, uint32_t* out) {
    { T::kSkipsSpans } -> std::convertible_to<bool>;
    { mask.Test(doc) } -> std::same_as<bool>;
    { mask.Probe(doc) } -> std::same_as<doc_id_t>;
    { mask.NextLive(doc) } -> std::same_as<doc_id_t>;
    { mask.NextSpan(doc) } -> std::same_as<MaskedSpan>;
    { mask.FilterBlock(docs, scores, len) } -> std::same_as<uint32_t>;
    { mask.CountMasked(docs, len) } -> std::same_as<uint32_t>;
    { mask.Remove(doc, doc, words) } -> std::same_as<void>;
    { mask.Remove(doc, doc, words, scores, score_t{}) } -> std::same_as<void>;
    { mask.FillOr(doc, doc, words) } -> std::same_as<doc_id_t>;
    { mask.FillRange(doc, doc, words) } -> std::same_as<void>;
    { mask.AndNot(doc, doc, words) } -> std::same_as<void>;
    { mask.CountIn(doc, doc) } -> std::same_as<uint64_t>;
    { mask.FillLive(doc, len, out) } -> std::same_as<uint32_t>;
  };

template<typename Derived>
class DocsMaskBase {
 public:
  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t doc) noexcept {
    return SpanProbe(doc);
  }

  IRS_FORCE_INLINE bool Test(doc_id_t doc) noexcept {
    return SpanProbe(doc) == doc;
  }

  IRS_FORCE_INLINE doc_id_t NextLive(doc_id_t doc) noexcept {
    while (!doc_limits::eof(doc) && SpanProbe(doc) == doc) {
      doc = _hi;
    }
    return doc;
  }

  IRS_FORCE_INLINE MaskedSpan NextSpan(doc_id_t doc) noexcept {
    const auto first = SpanProbe(doc);
    return {first, doc_limits::eof(first) ? first : _hi};
  }

  uint32_t FilterBlock(doc_id_t* IRS_RESTRICT docs,
                       score_t* IRS_RESTRICT scores, uint32_t len) noexcept {
    if constexpr (Derived::kSkipsSpans) {
      uint32_t kept = 0;
      uint32_t i = 0;
      while (i != len) {
        const auto masked = SpanProbe(docs[i]);
        for (; i != len && docs[i] < masked; ++i, ++kept) {
          docs[kept] = docs[i];
          scores[kept] = scores[i];
        }
        if (i == len) {
          break;
        }
        const auto end = _hi;
        while (i != len && docs[i] < end) {
          ++i;
        }
      }
      return kept;
    } else {
      if (len == 0) {
        return 0;
      }
      return Self().WithBlockTest(docs[0], docs[len - 1], [&](auto test) {
        uint32_t kept = 0;
        for (uint32_t i = 0; i != len; ++i) {
          const auto doc = docs[i];
          docs[kept] = doc;
          scores[kept] = scores[i];
          kept += static_cast<uint32_t>(!test(doc));
        }
        return kept;
      });
    }
  }

  uint32_t CountMasked(const doc_id_t* IRS_RESTRICT docs,
                       uint32_t len) noexcept {
    if (len == 0) {
      return 0;
    }
    return Self().WithBlockTest(docs[0], docs[len - 1], [&](auto test) {
      uint32_t masked = 0;
      for (uint32_t i = 0; i != len; ++i) {
        masked += static_cast<uint32_t>(test(docs[i]));
      }
      return masked;
    });
  }

  template<typename Fn>
  IRS_FORCE_INLINE auto WithBlockTest(doc_id_t, doc_id_t, Fn&& fn) {
    return fn([this](doc_id_t doc) noexcept { return Self().Test(doc); });
  }

  doc_id_t FillOr(doc_id_t min, doc_id_t max,
                  uint64_t* IRS_RESTRICT words) noexcept {
    Self().template Apply<false>(min, max, words);
    return Self().Probe(max);
  }

  void FillRange(doc_id_t min, doc_id_t max,
                 uint64_t* IRS_RESTRICT words) noexcept {
    Self().template Apply<false>(min, max, words);
  }

  void AndNot(doc_id_t min, doc_id_t max,
              uint64_t* IRS_RESTRICT words) noexcept {
    Self().template Apply<true>(min, max, words);
  }

  void Remove(doc_id_t min, doc_id_t max,
              uint64_t* IRS_RESTRICT words) noexcept {
    if (Self().Probe(min) < max) {
      Self().template Apply<true>(min, max, words);
    }
  }

  void Remove(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words,
              score_t* IRS_RESTRICT scores, score_t reset) noexcept {
    if (Self().Probe(min) >= max) {
      return;
    }
    uint64_t dead[detail::kWindowWords];
    const auto count = DeadWords<false>(min, max, dead);
    for (size_t w = 0; w != count; ++w) {
      auto cleared = words[w] & dead[w];
      words[w] &= ~dead[w];
      while (cleared != 0) {
        scores[w * 64 + static_cast<size_t>(std::countr_zero(cleared))] = reset;
        cleared &= cleared - 1;
      }
    }
  }

  uint32_t FillLive(doc_id_t min, uint32_t count,
                    uint32_t* IRS_RESTRICT out) noexcept {
    SDB_ASSERT(count != 0);
    SDB_ASSERT(count <= detail::kWindowDocs);
    if (Self().Probe(min) >= min + count) {
      for (uint32_t i = 0; i != count; ++i) {
        out[i] = i;
      }
      return count;
    }
    uint64_t dead[detail::kWindowWords];
    const auto used = DeadWords<true>(min, min + count, dead);
    auto* const begin = out;
    for (uint32_t w = 0; w != used; ++w) {
      out = MaterializeWord(w * 64, ~dead[w], out);
    }
    return static_cast<uint32_t>(out - begin);
  }

  template<bool kPadTail>
  size_t DeadWords(doc_id_t min, doc_id_t max,
                   uint64_t* IRS_RESTRICT dead) noexcept {
    const auto count = detail::WindowWords(min, max);
    std::fill_n(dead, count, uint64_t{0});
    Self().template Apply<false>(min, max, dead);
    if constexpr (kPadTail) {
      if (const auto rest = (max - min) % 64; rest != 0) {
        dead[count - 1] |= ~uint64_t{0} << rest;
      }
    }
    return count;
  }

  template<typename Fn>
  void VisitLiveRanges(doc_id_t begin, doc_id_t end, Fn&& fn) {
    for (auto doc = begin; doc < end;) {
      const auto masked = SpanProbe(doc);
      if (doc < masked) {
        fn(doc, std::min(masked, end));
      }
      if (masked >= end) {
        return;
      }
      doc = _hi;
    }
  }

 protected:
  IRS_FORCE_INLINE doc_id_t SpanProbe(doc_id_t doc) noexcept {
    const doc_id_t offset = doc - _from;
    if (offset < _gap) {
      return _lo;
    }
    if (offset < _span) {
      return doc;
    }
    return Refill(doc);
  }

 private:
  IRS_NO_INLINE doc_id_t Refill(doc_id_t doc) noexcept {
    if (doc_limits::eof(doc)) {
      return doc;
    }
    const auto span = Self().NextMasked(doc);
    SDB_ASSERT(doc <= span.first);
    SDB_ASSERT(span.first < span.last || doc_limits::eof(span.first));
    _from = doc;
    _lo = span.first;
    _hi = span.last;
    _gap = span.first - doc;
    _span = span.last - doc;
    return span.first;
  }

  IRS_FORCE_INLINE Derived& Self() noexcept {
    return static_cast<Derived&>(*this);
  }

  doc_id_t _from = 0;
  doc_id_t _lo = 0;
  doc_id_t _hi = 0;
  doc_id_t _gap = 0;
  doc_id_t _span = 0;
};

}  // namespace irs
