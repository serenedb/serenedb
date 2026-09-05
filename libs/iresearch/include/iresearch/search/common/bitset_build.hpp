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
#include <span>
#include <utility>
#include <vector>

#include "basics/bit_utils.hpp"
#include "basics/down_cast.h"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/bitset_storage.hpp"
#include "iresearch/search/common/enc_buf.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

inline constexpr uint64_t kBitsetDensity = 8;

inline uint64_t SegmentWords(doc_id_t docs_count) noexcept {
  return uint64_t{docs_count} / BitsetStorage::kBits + 1;
}

inline bool DensePosting(uint64_t docs, doc_id_t docs_count) noexcept {
  return docs * kBitsetDensity >= docs_count;
}

inline uint64_t FoldRead(uint64_t docs, doc_id_t docs_count,
                         uint64_t words) noexcept {
  return DensePosting(docs, docs_count) ? words : docs;
}

template<typename Term>
bool AppliedInPlace(std::span<const Term> clause,
                    doc_id_t docs_count) noexcept {
  return clause.size() == 1 &&
         DensePosting(CookieOf(clause.front()).docs_count, docs_count);
}

inline IRS_FORCE_INLINE void ClearBitRange(uint64_t* IRS_RESTRICT words,
                                           uint64_t begin,
                                           uint64_t end) noexcept {
  constexpr auto kBits = BitsRequired<uint64_t>();
  if (begin >= end) {
    return;
  }
  const auto first = begin / kBits;
  const auto last = (end - 1) / kBits;
  const uint64_t head = ~uint64_t{0} << (begin % kBits);
  const uint64_t tail = ~uint64_t{0} >> (kBits - 1 - (end - 1) % kBits);
  if (first == last) {
    words[first] &= ~(head & tail);
    return;
  }
  words[first] &= ~head;
  std::fill(words + first + 1, words + last, uint64_t{0});
  words[last] &= ~tail;
}

inline IRS_FORCE_INLINE void AndNotBitsetAt(uint64_t* IRS_RESTRICT dst,
                                            uint64_t prev,
                                            const uint64_t* IRS_RESTRICT src,
                                            uint32_t words) noexcept {
  constexpr auto kBits = BitsRequired<uint64_t>();
  SDB_ASSERT(words != 0);
  dst += prev / kBits;
  const auto shift = prev % kBits;
  if (shift == 0) {
    for (uint32_t i = 0; i != words; ++i) {
      dst[i] &= ~src[i];
    }
    return;
  }
  uint64_t carry = 0;
  for (uint32_t i = 0; i != words; ++i) {
    const auto word = src[i];
    dst[i] &= ~((word << shift) | carry);
    carry = word >> (kBits - shift);
  }
  dst[words] &= ~carry;
}

inline IRS_FORCE_INLINE void AndBitsetAt(uint64_t* IRS_RESTRICT dst,
                                         uint64_t prev,
                                         const uint64_t* IRS_RESTRICT src,
                                         uint32_t words,
                                         uint64_t max) noexcept {
  constexpr auto kBits = BitsRequired<uint64_t>();
  SDB_ASSERT(words != 0);
  SDB_ASSERT(max > prev);
  const auto shift = prev % kBits;
  auto* const base = dst + prev / kBits;
  const auto stop = static_cast<uint32_t>(max / kBits - prev / kBits);
  const auto top = max % kBits;
  const uint64_t above =
    top == kBits - 1 ? uint64_t{0} : (~uint64_t{0} << (top + 1));
  uint64_t keep = (uint64_t{2} << shift) - 1;
  uint64_t carry = 0;
  for (uint32_t i = 0; i <= stop; ++i) {
    const auto word = i < words ? src[i] : uint64_t{0};
    uint64_t mask;
    if (shift == 0) {
      mask = word;
    } else {
      mask = (word << shift) | carry;
      carry = word >> (kBits - shift);
    }
    mask |= keep;
    keep = 0;
    if (i == stop) {
      mask |= above;
    }
    base[i] &= mask;
  }
}

struct OrBits {
  static constexpr auto kBits = BitsRequired<uint64_t>();
  static constexpr bool kOrdered = false;

  uint64_t* IRS_RESTRICT words;

  IRS_FORCE_INLINE void Run(uint64_t prev, uint32_t len) noexcept {
    SetBitRange(words, prev + 1, prev + 1 + len);
  }

  IRS_FORCE_INLINE void Bitset(uint64_t prev, const uint64_t* IRS_RESTRICT src,
                               uint32_t n, uint64_t) noexcept {
    OrBitsetAt(words, prev, src, n);
  }

  IRS_FORCE_INLINE void Doc(size_t doc) noexcept {
    SetBit(words[doc / kBits], doc % kBits);
  }

  IRS_FORCE_INLINE void Finish(uint32_t) noexcept {}
};

struct ClearBits {
  static constexpr auto kBits = BitsRequired<uint64_t>();
  static constexpr bool kOrdered = false;

  uint64_t* IRS_RESTRICT words;

  IRS_FORCE_INLINE void Run(uint64_t prev, uint32_t len) noexcept {
    ClearBitRange(words, prev + 1, prev + 1 + len);
  }

  IRS_FORCE_INLINE void Bitset(uint64_t prev, const uint64_t* IRS_RESTRICT src,
                               uint32_t n, uint64_t) noexcept {
    AndNotBitsetAt(words, prev, src, n);
  }

  IRS_FORCE_INLINE void Doc(size_t doc) noexcept {
    UnsetBit(words[doc / kBits], doc % kBits);
  }

  IRS_FORCE_INLINE void Finish(uint32_t) noexcept {}
};

struct RetainBits {
  static constexpr auto kBits = BitsRequired<uint64_t>();
  static constexpr bool kOrdered = true;

  uint64_t* IRS_RESTRICT words;
  uint32_t at = 0;
  uint64_t keep = 0;

  static IRS_FORCE_INLINE uint64_t Between(uint64_t first,
                                           uint64_t last) noexcept {
    return ((uint64_t{2} << last) - 1) & (~uint64_t{0} << first);
  }

  static constexpr uint32_t kBulkGap = 16;

  IRS_FORCE_INLINE void Reach(uint64_t doc) noexcept {
    const auto word = static_cast<uint32_t>(doc / kBits);
    if (word == at) {
      return;
    }
    words[at] &= keep;
    keep = 0;
    auto gap = at + 1;
    at = word;
    if (word - gap >= kBulkGap) [[unlikely]] {
      std::fill(words + gap, words + word, uint64_t{0});
      return;
    }
    for (; gap != word; ++gap) {
      words[gap] = 0;
    }
  }

  IRS_FORCE_INLINE void Run(uint64_t prev, uint32_t len) noexcept {
    const uint64_t first = prev + 1;
    const uint64_t last = prev + len;
    Reach(first);
    const auto word = static_cast<uint32_t>(last / kBits);
    if (word == at) {
      keep |= Between(first % kBits, last % kBits);
      return;
    }
    words[at] &= keep | (~uint64_t{0} << (first % kBits));
    at = word;
    keep = (uint64_t{2} << (last % kBits)) - 1;
  }

  IRS_FORCE_INLINE void Bitset(uint64_t prev, const uint64_t* IRS_RESTRICT src,
                               uint32_t n, uint64_t max) noexcept {
    Reach(prev + 1);
    words[at] &= keep | (~uint64_t{0} << ((prev + 1) % kBits));
    AndBitsetAt(words, prev, src, n, max);
    at = static_cast<uint32_t>(max / kBits);
    keep = (uint64_t{2} << (max % kBits)) - 1;
  }

  IRS_FORCE_INLINE void Doc(size_t doc) noexcept {
    Reach(doc);
    keep |= uint64_t{1} << (doc % kBits);
  }

  IRS_FORCE_INLINE void Finish(uint32_t word_count) noexcept {
    if (at >= word_count) {
      return;
    }
    words[at] &= keep;
    std::fill(words + at + 1, words + word_count, uint64_t{0});
    at = word_count;
    keep = 0;
  }
};

template<typename Input, typename Sink>
void ReadPosting(const PostingMeta& meta, Input& in, uint32_t* IRS_RESTRICT enc,
                 doc_id_t* IRS_RESTRICT docs, bool has_score_bounds,
                 bool has_freq, Sink& sink) {
  SDB_ASSERT(meta.docs_count > 1);

  in.Seek(meta.doc_start);
  if (meta.docs_count < doc_limits::kBlockSize) {
    SkipScoreBounds(has_score_bounds, in);
  }

  const auto read_leaf = [&]<size_t N>(uint32_t len,
                                       doc_id_t prev) IRS_FORCE_INLINE {
    const auto leaf =
      FormatTraits128::ReadTailForFill(len, in, enc, docs, prev);
    if (leaf.IsRun()) {
      sink.Run(prev, len);
    } else if (leaf.IsBitset()) {
      sink.Bitset(prev, leaf.bitset, leaf.words, leaf.max);
    } else {
      const auto* const data = docs + doc_limits::kBlockSize - len;
      if constexpr (Sink::kOrdered) {
        for (uint32_t i = 0; i != len; ++i) {
          sink.Doc(data[i]);
        }
      } else {
        VisitDocs<N>(len,
                     [&](uint32_t i) IRS_FORCE_INLINE { sink.Doc(data[i]); });
      }
    }
    if (has_freq && len == doc_limits::kBlockSize) {
      FormatTraits128::SkipBlock(in);
    }
    return leaf.max;
  };

  auto prev = doc_limits::invalid();
  for (auto blocks = meta.docs_count / doc_limits::kBlockSize; blocks--;) {
    prev = read_leaf.template operator()<doc_limits::kBlockSize>(
      doc_limits::kBlockSize, prev);
  }
  if (const auto tail = meta.docs_count % doc_limits::kBlockSize; tail != 0) {
    read_leaf.template operator()<std::dynamic_extent>(tail, prev);
  }
}

template<typename Input>
class PostingReader {
 public:
  explicit PostingReader(const IndexInput& doc) noexcept : _doc{&doc} {}

  Input& In() {
    if (_in == nullptr) [[unlikely]] {
      _owned = _doc->Reopen();
      if (!_owned) [[unlikely]] {
        throw IoError{"failed to reopen document input"};
      }
      _in = &sdb::basics::downCast<Input>(*_owned);
    }
    return *_in;
  }

  uint32_t* Enc() noexcept { return EncOf<Input>(_enc); }

  doc_id_t* Docs() noexcept { return _buf; }

 private:
  const IndexInput* _doc;
  IndexInput::ptr _owned;
  Input* _in = nullptr;
  ABSL_CACHELINE_ALIGNED doc_id_t _buf[doc_limits::kBlockSize];
#ifdef __AVX2__
  [[maybe_unused]] SlackBuf _slack;
#endif
  [[no_unique_address]] NeedEnc<Input> _enc;
};

template<typename Term, typename Sink, typename Input>
void ReadTerms(std::span<const Term> terms, const TermReader* field,
               PostingReader<Input>& r, Sink& sink) {
  for (size_t i = 0; i != terms.size(); ++i) {
    const auto& meta = CookieOf(terms[i]);
    SDB_ASSERT(meta.docs_count != 0);
    if (meta.docs_count == 1) {
      sink.Doc(doc_limits::min() + meta.doc_delta);
      continue;
    }
    const auto& own = FieldOf(terms[i], field);
    ReadPosting(meta, r.In(), r.Enc(), r.Docs(), BoundsOf(own), FreqOf(own),
                sink);
  }
}

inline void ReadFill(FillNode& node, doc_id_t end,
                     uint64_t* IRS_RESTRICT words) {
  constexpr auto kBits = BitsetStorage::kBits;
  for (doc_id_t min = 0; min < end;) {
    const auto next = node.FillOr(min, min + kWindowDocs, words + min / kBits);
    if (next >= end) {
      break;
    }
    min = std::max(min + kWindowDocs, next - next % kWindowDocs);
  }
}

struct BitsetBuckets {
  std::vector<std::vector<PostingClause>> must;
  std::vector<PostingClause> must_not;
  std::vector<FillNode::ptr> fills;
  std::vector<FillNode::ptr> exclude_fills;

  size_t Seed(doc_id_t docs_count) const noexcept {
    if (must.empty() || must.front().size() > 1) {
      return 0;
    }
    if (!AppliedInPlace(std::span<const PostingClause>{must.front()},
                        docs_count)) {
      return 0;
    }
    for (size_t i = 1; i != must.size(); ++i) {
      if (must[i].size() > 1) {
        return i;
      }
    }
    return 0;
  }

  bool NeedsSet() const noexcept {
    if (!fills.empty() || !exclude_fills.empty()) {
      return true;
    }
    for (const auto& clause : must) {
      if (clause.size() > 1) {
        return true;
      }
    }
    return false;
  }
};

inline BitsetStorage BuildBitset(BitsetBuckets& buckets, const IndexInput& doc,
                                 doc_id_t docs_count) {
  SDB_ASSERT(!buckets.must.empty() || !buckets.fills.empty());
  SDB_ASSERT(buckets.fills.empty() || buckets.must.size() <= 1);
  BitsetStorage bits{docs_count};
  auto* const words = bits.Words();
  const auto seed = buckets.Seed(docs_count);

  ResolveInput(doc, [&]<typename Input> {
    PostingReader<Input> reader{doc};
    if (!buckets.must.empty()) {
      OrBits or_seed{words};
      ReadTerms(std::span<const PostingClause>{buckets.must[seed]}, nullptr,
                reader, or_seed);
    }
    for (auto& node : buckets.fills) {
      SDB_ASSERT(node);
      ReadFill(*node, bits.End(), words);
    }

    std::unique_ptr<uint64_t[]> scratch;
    const auto open_scratch = [&]() -> uint64_t* {
      if (!scratch) {
        scratch = std::make_unique<uint64_t[]>(bits.Alloc());
      } else {
        std::fill_n(scratch.get(), bits.Alloc(), uint64_t{0});
      }
      return scratch.get();
    };

    for (size_t i = 0, n = buckets.must.size(); i != n; ++i) {
      if (i == seed) {
        continue;
      }
      const std::span<const PostingClause> clause{buckets.must[i]};
      SDB_ASSERT(!clause.empty());
      if (AppliedInPlace(clause, docs_count)) {
        RetainBits retain{words};
        ReadTerms(clause, nullptr, reader, retain);
        retain.Finish(bits.WordCount());
        continue;
      }
      auto* const other = open_scratch();
      OrBits or_clause{other};
      ReadTerms(clause, nullptr, reader, or_clause);
      for (uint32_t w = 0, count = bits.WordCount(); w != count; ++w) {
        words[w] &= other[w];
      }
    }

    if (!buckets.must_not.empty()) {
      ClearBits clear{words};
      ReadTerms(std::span<const PostingClause>{buckets.must_not}, nullptr,
                reader, clear);
    }
    if (!buckets.exclude_fills.empty()) {
      auto* const other = open_scratch();
      for (auto& node : buckets.exclude_fills) {
        SDB_ASSERT(node);
        ReadFill(*node, bits.End(), other);
      }
      for (uint32_t w = 0, count = bits.WordCount(); w != count; ++w) {
        words[w] &= ~other[w];
      }
    }
  });

  bits.Trim();
  return bits;
}

}  // namespace irs::search
