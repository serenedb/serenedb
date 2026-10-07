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
#include <array>
#include <bit>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "iresearch/index/docs_mask/base.hpp"
#include "iresearch/index/docs_mask/chunks.hpp"
#include "iresearch/index/docs_mask/kernels.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

template<typename Make>
decltype(auto) ResolveDocsMask(const DocumentMask* mask, doc_id_t visible_end,
                               Make&& make, bool single = true);

inline DocsMask<MaskKind::Mixed> MakeGenericDocsMask(
  const DocumentMask* mask, doc_id_t visible_end) noexcept;

template<MaskKind K>
class DocsMask final : public docs_mask::Chunked<DocsMask<K>, K> {
  using Base = docs_mask::Chunked<DocsMask<K>, K>;

  template<typename Make>
  friend decltype(auto) ResolveDocsMask(const DocumentMask* mask,
                                        doc_id_t visible_end, Make&& make,
                                        bool single);
  friend DocsMask<MaskKind::Mixed> MakeGenericDocsMask(
    const DocumentMask* mask, doc_id_t visible_end) noexcept;

 public:
  static constexpr MaskKind kKind = K;

  uint32_t FilterBlock(doc_id_t* IRS_RESTRICT docs,
                       score_t* IRS_RESTRICT scores, uint32_t len) noexcept {
    if (len == 0) {
      return 0;
    }
    auto span = this->NextSpan(docs[0]);
    if (span.first > docs[len - 1]) {
      return len;
    }
    uint32_t kept = 0;
    for (uint32_t i = 0; i != len; ++i) {
      const auto doc = docs[i];
      if (doc >= span.last) {
        span = this->NextSpan(doc);
      }
      docs[kept] = doc;
      scores[kept] = scores[i];
      kept += static_cast<uint32_t>(doc < span.first);
    }
    return kept;
  }

  uint32_t CountMasked(const doc_id_t* IRS_RESTRICT docs,
                       uint32_t len) noexcept {
    if (len == 0) {
      return 0;
    }
    auto span = this->NextSpan(docs[0]);
    if (span.first > docs[len - 1]) {
      return 0;
    }
    uint32_t masked = 0;
    for (uint32_t i = 0; i != len; ++i) {
      const auto doc = docs[i];
      if (doc >= span.last) {
        span = this->NextSpan(doc);
      }
      masked += static_cast<uint32_t>(doc >= span.first);
    }
    return masked;
  }

 private:
  DocsMask(const DocumentMask* mask, doc_id_t visible_end) noexcept
    : Base{mask, visible_end} {}
};

namespace docs_mask {

alignas(64) inline constexpr uint64_t kNoWords[kChunkWords] = {};
alignas(64) inline constexpr auto kAllWords = [] {
  std::array<uint64_t, kChunkWords> words{};
  words.fill(~uint64_t{0});
  return words;
}();

}  // namespace docs_mask

template<MaskKind K>
  requires(docs_mask::Plural(K) == MaskKind::Bitsets)
class DocsMask<K> final : public docs_mask::Chunked<DocsMask<K>, K> {
  using Base = docs_mask::Chunked<DocsMask<K>, K>;
  using Base::_end;
  using Base::_layout;

  template<typename Make>
  friend decltype(auto) ResolveDocsMask(const DocumentMask* mask,
                                        doc_id_t visible_end, Make&& make,
                                        bool single);
  friend DocsMask<MaskKind::Mixed> MakeGenericDocsMask(
    const DocumentMask* mask, doc_id_t visible_end) noexcept;

 public:
  static constexpr MaskKind kKind = K;

  using Base::Remove;

  IRS_FORCE_INLINE bool Test(doc_id_t doc) noexcept {
    doc_id_t offset = doc - _base;
    if (offset >= _limit) [[unlikely]] {
      Rebase(doc);
      offset = doc - _base;
    }
    return Bit(_words, offset);
  }

  IRS_NO_INLINE uint32_t FilterBlock(doc_id_t* IRS_RESTRICT docs,
                                     score_t* IRS_RESTRICT scores,
                                     uint32_t len) noexcept {
    if (len == 0) {
      return 0;
    }
    uint32_t kept = 0;
    if (PinBlock(docs[0], docs[len - 1])) {
      const auto* words = _words;
      const auto base = _base;
      // The loop body fits one 64-byte uop cache window only when aligned;
      // straddling two windows measured ~15% slower.
      [[clang::code_align(64)]] for (uint32_t i = 0; i != len; ++i) {
        const auto doc = docs[i];
        docs[kept] = doc;
        scores[kept] = scores[i];
        kept += static_cast<uint32_t>(!Bit(words, doc - base));
      }
    } else {
      for (uint32_t i = 0; i != len; ++i) {
        const auto doc = docs[i];
        docs[kept] = doc;
        scores[kept] = scores[i];
        kept += static_cast<uint32_t>(!Test(doc));
      }
    }
    return kept;
  }

  IRS_NO_INLINE uint32_t CountMasked(const doc_id_t* IRS_RESTRICT docs,
                                     uint32_t len) noexcept {
    if (len == 0) {
      return 0;
    }
    uint32_t masked = 0;
    if (PinBlock(docs[0], docs[len - 1])) {
      const auto* words = _words;
      const auto base = _base;
      // The loop body fits one 64-byte uop cache window only when aligned;
      // straddling two windows measured ~15% slower.
      [[clang::code_align(64)]] for (uint32_t i = 0; i != len; ++i) {
        masked += static_cast<uint32_t>(Bit(words, docs[i] - base));
      }
    } else {
      for (uint32_t i = 0; i != len; ++i) {
        masked += static_cast<uint32_t>(Test(docs[i]));
      }
    }
    return masked;
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t doc) const noexcept {
    if (doc >= _end) [[unlikely]] {
      return doc;
    }
    const auto begin = _layout.Begin();
    const doc_id_t offset = doc - begin;
    const auto chunk = offset >> docs_mask::kChunkShift;
    if (chunk >= _layout.Count()) [[unlikely]] {
      return doc < begin ? std::min(begin, _end) : _end;
    }
    const auto low = offset & docs_mask::kChunkLow;
    const auto rest =
      _layout.At(chunk).Words()[low / 64] & (~uint64_t{0} << (low % 64));
    const auto found =
      uint64_t{doc} - low % 64 + static_cast<uint64_t>(std::countr_zero(rest));
    return static_cast<doc_id_t>(std::min<uint64_t>(found, _end));
  }

  IRS_FORCE_INLINE doc_id_t NextLive(doc_id_t doc) const noexcept {
    const auto begin = _layout.Begin();
    uint64_t at = doc;
    while (at < _end) {
      const auto offset = at - begin;
      if (at < begin || (offset >> docs_mask::kChunkShift) >= _layout.Count()) {
        return static_cast<doc_id_t>(at);
      }
      const auto low = static_cast<uint32_t>(offset & docs_mask::kChunkLow);
      const auto live =
        _layout.At(static_cast<uint32_t>(offset >> docs_mask::kChunkShift))
          .NextUnset(low);
      at += live - low;
      if (live != docs_mask::kChunkDocs) {
        return at < _end ? static_cast<doc_id_t>(at) : doc_limits::eof();
      }
    }
    return doc_limits::eof();
  }

  void Remove(doc_id_t min, doc_id_t max,
              uint64_t* IRS_RESTRICT words) noexcept {
    this->template Apply<true>(min, max, words);
  }

 private:
  DocsMask(const DocumentMask* mask, doc_id_t visible_end) noexcept
    : Base{mask, visible_end} {
    Rebase(doc_limits::min());
  }

  IRS_FORCE_INLINE static bool Bit(const uint64_t* words,
                                   doc_id_t offset) noexcept {
    return ((words[offset / 64] >> (offset % 64)) & 1) != 0;
  }

  IRS_FORCE_INLINE bool PinBlock(doc_id_t first, doc_id_t last) noexcept {
    if (first - _base >= _limit) {
      Rebase(first);
    }
    return last - _base < _limit;
  }

  IRS_NO_INLINE void Rebase(doc_id_t doc) noexcept {
    if (doc >= _end) {
      _base = doc;
      _limit = static_cast<doc_id_t>(docs_mask::kChunkDocs);
      _words = docs_mask::kAllWords.data();
      return;
    }
    _base = doc & ~docs_mask::kChunkLow;
    _limit =
      std::min(static_cast<doc_id_t>(docs_mask::kChunkDocs), _end - _base);
    const auto begin = _layout.Begin();
    const doc_id_t offset = doc - begin;
    const auto chunk = offset >> docs_mask::kChunkShift;
    _words = doc >= begin && chunk < _layout.Count() ? _layout.At(chunk).Words()
                                                     : docs_mask::kNoWords;
  }

  doc_id_t _base = 0;
  doc_id_t _limit = 0;
  const uint64_t* _words = docs_mask::kNoWords;
};

using GenericDocsMask = DocsMask<MaskKind::Mixed>;

template<typename Make>
decltype(auto) ResolveDocsMask(const DocumentMask* mask, doc_id_t visible_end,
                               Make&& make, bool single) {
  if (mask == nullptr || mask->Empty()) {
    return make(DocsMask<MaskKind::Runs>{nullptr, visible_end});
  }
  switch (single ? mask->Kind() : docs_mask::Plural(mask->Kind())) {
    case MaskKind::Bitsets:
      return make(DocsMask<MaskKind::Bitsets>{mask, visible_end});
    case MaskKind::Arrays:
      return make(DocsMask<MaskKind::Arrays>{mask, visible_end});
    case MaskKind::Runs:
      return make(DocsMask<MaskKind::Runs>{mask, visible_end});
    case MaskKind::Bitset:
      return make(DocsMask<MaskKind::Bitset>{mask, visible_end});
    case MaskKind::Array:
      return make(DocsMask<MaskKind::Array>{mask, visible_end});
    case MaskKind::Run:
      return make(DocsMask<MaskKind::Run>{mask, visible_end});
    case MaskKind::Mixed:
      break;
  }
  return make(DocsMask<MaskKind::Mixed>{mask, visible_end});
}

template<typename Make>
decltype(auto) ResolveDocsMask(const SubReader& segment, Make&& make) {
  return ResolveDocsMask(segment.docs_mask(), segment.Meta().visible_end,
                         std::forward<Make>(make));
}

inline GenericDocsMask MakeGenericDocsMask(const DocumentMask* mask,
                                           doc_id_t visible_end) noexcept {
  return GenericDocsMask{mask, visible_end};
}

inline GenericDocsMask MakeGenericDocsMask(const SubReader& segment) noexcept {
  return MakeGenericDocsMask(segment.docs_mask(), segment.Meta().visible_end);
}

inline doc_id_t LiveEnd(const SubReader& segment) noexcept {
  return static_cast<doc_id_t>(doc_limits::min() + segment.docs_count());
}

template<typename Fn>
void VisitLiveRanges(const DocumentMask* mask, doc_id_t visible_end,
                     doc_id_t begin, doc_id_t end, Fn&& fn) {
  ResolveDocsMask(mask, visible_end, [&]<DocsMaskType Mask>(Mask docs_mask) {
    docs_mask.VisitLiveRanges(begin, end, fn);
  });
}

}  // namespace irs
