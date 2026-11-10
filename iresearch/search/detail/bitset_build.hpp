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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <bit>
#include <optional>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/formats/posting/block_io.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/doc_input.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/search/detail/bitset_storage.hpp"
#include "iresearch/search/detail/enc_buf.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

inline constexpr uint64_t kBitsetDensity = 8;

inline uint64_t SegmentWords(doc_id_t docs_count) noexcept {
  return uint64_t{docs_count} / BitsetStorage::kBits + 1;
}

inline uint64_t SegmentWindows(doc_id_t docs_count) noexcept {
  return uint64_t{docs_count} / kWindowDocs + 1;
}

inline bool DensePosting(uint64_t docs, doc_id_t docs_count) noexcept {
  return docs * kBitsetDensity >= docs_count;
}

inline uint64_t FoldRead(uint64_t docs, doc_id_t docs_count,
                         uint64_t words) noexcept {
  return DensePosting(docs, docs_count) ? words : docs;
}

inline constexpr uint64_t kProbePostings = 20;

inline bool ProbeBeatsFold(doc_id_t rarest, doc_id_t densest,
                           doc_id_t docs_count) noexcept {
  const auto words = SegmentWords(docs_count);
  return uint64_t{rarest} * kProbePostings <=
         words + FoldRead(rarest, docs_count, words) +
           FoldRead(densest, docs_count, words);
}

template<typename Term>
bool AppliedInPlace(std::span<const Term> clause,
                    doc_id_t docs_count) noexcept {
  return clause.size() == 1 &&
         DensePosting(CookieOf(clause.front()).docs_count, docs_count);
}

struct OrBits {
  static constexpr auto kBits = BitsetStorage::kBits;
  static constexpr bool kOrdered = false;

  uint64_t* IRS_RESTRICT words;
  doc_id_t base = BitsetStorage::kMin;

  IRS_FORCE_INLINE void Run(uint64_t prev, uint32_t len) noexcept {
    const auto first = prev + 1 - base;
    SetBitRange(words, first, first + len);
  }

  IRS_FORCE_INLINE void Bitset(uint64_t prev,
                               const UnalignedWord* IRS_RESTRICT src,
                               uint32_t n, uint64_t) noexcept {
    OrBlock(words, prev + 1 - base, src, n);
  }

  IRS_FORCE_INLINE void Doc(size_t doc) noexcept {
    const auto offset = doc - base;
    SetBit(words[offset / kBits], offset % kBits);
  }
};

struct ClearBits {
  static constexpr auto kBits = BitsetStorage::kBits;
  static constexpr bool kOrdered = false;

  uint64_t* IRS_RESTRICT words;
  doc_id_t base = BitsetStorage::kMin;

  IRS_FORCE_INLINE void Run(uint64_t prev, uint32_t len) noexcept {
    const auto first = prev + 1 - base;
    ClearBitRange(words, first, first + len);
  }

  IRS_FORCE_INLINE void Bitset(uint64_t prev,
                               const UnalignedWord* IRS_RESTRICT src,
                               uint32_t n, uint64_t) noexcept {
    ClearBlock(words, prev + 1 - base, src, n);
  }

  IRS_FORCE_INLINE void Doc(size_t doc) noexcept {
    const auto offset = doc - base;
    UnsetBit(words[offset / kBits], offset % kBits);
  }
};

struct RetainBits {
  static constexpr auto kBits = BitsetStorage::kBits;
  static constexpr bool kOrdered = true;

  uint64_t* IRS_RESTRICT words;
  doc_id_t base = BitsetStorage::kMin;
  uint32_t at = 0;
  uint64_t keep = 0;

  static IRS_FORCE_INLINE uint64_t Between(uint64_t first,
                                           uint64_t last) noexcept {
    return ((uint64_t{2} << last) - 1) & (~uint64_t{0} << first);
  }

  static constexpr uint32_t kBulkGap = 16;

  IRS_FORCE_INLINE void Reach(uint64_t offset) noexcept {
    const auto word = static_cast<uint32_t>(offset / kBits);
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
    const uint64_t first = prev + 1 - base;
    const uint64_t last = prev + len - base;
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

  IRS_FORCE_INLINE void Bitset(uint64_t prev,
                               const UnalignedWord* IRS_RESTRICT src,
                               uint32_t n, uint64_t max) noexcept {
    const auto first = prev + 1 - base;
    const auto last = max - base;
    Reach(first);
    words[at] &= keep | (~uint64_t{0} << (first % kBits));
    RetainBlock(words, first, src, n, last);
    at = static_cast<uint32_t>(last / kBits);
    keep = (uint64_t{2} << (last % kBits)) - 1;
  }

  IRS_FORCE_INLINE void Doc(size_t doc) noexcept {
    const auto offset = doc - base;
    Reach(offset);
    keep |= uint64_t{1} << (offset % kBits);
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

inline constexpr uint64_t kFillPrefetch = 512;

class LiveWords {
 public:
  static constexpr auto kBits = BitsetStorage::kBits;

  void Reset(const uint64_t* IRS_RESTRICT words, uint32_t count,
             doc_id_t base) {
    _base = base;
    _count = count;
    _live.assign((count + kBits - 1) / kBits, uint64_t{0});
    for (uint32_t w = 0; w != count; ++w) {
      _live[w / kBits] |= uint64_t{words[w] != 0} << (w % kBits);
    }
  }

  doc_id_t Next(doc_id_t doc) const noexcept {
    SDB_ASSERT(doc >= _base);
    const uint64_t at = (doc - _base) / kBits;
    if (at >= _count) {
      return doc_limits::eof();
    }
    auto i = at / kBits;
    auto rest = _live[i] & (~uint64_t{0} << (at % kBits));
    while (rest == 0) {
      if (++i == _live.size()) {
        return doc_limits::eof();
      }
      rest = _live[i];
    }
    const auto word = i * kBits + static_cast<uint64_t>(std::countr_zero(rest));
    return word == at ? doc : static_cast<doc_id_t>(_base + word * kBits);
  }

 private:
  std::vector<uint64_t> _live;
  doc_id_t _base = 0;
  uint32_t _count = 0;
};

template<typename Input, typename Sink>
doc_id_t ReadPosting(Input& in, uint32_t* IRS_RESTRICT enc,
                     uint64_t* IRS_RESTRICT holes, doc_id_t* IRS_RESTRICT docs,
                     bool has_freq, doc_id_t prev, uint32_t left, doc_id_t min,
                     doc_id_t max, BlockCursor* cursor, const LiveWords* live,
                     Sink& sink) {
  constexpr auto kBlock = doc_limits::kBlockSize;
  SDB_ASSERT(live == nullptr || cursor != nullptr);

  [[maybe_unused]] const byte_type* at = nullptr;
  [[maybe_unused]] const byte_type* fetched = nullptr;
  const auto seat = [&] IRS_FORCE_INLINE {
    if constexpr (Input::kVolatileAlways) {
      at = in.Current();
      fetched = at;
    }
  };
  seat();

  const auto read_leaf = [&](uint32_t len) IRS_FORCE_INLINE {
    if constexpr (Input::kVolatileAlways) {
      const auto* const from = std::max(fetched, at);
      for (uint64_t offset = 0; offset != kFillPrefetch;
           offset += ABSL_CACHELINE_SIZE) {
        __builtin_prefetch(from + offset);
      }
      fetched = at + kFillPrefetch;
      return block_io::FillView(in, at, len, holes, docs, prev,
                                has_freq && len == kBlock);
    } else {
      const auto read =
        block_io::ReadTailForFill(len, in, enc, holes, docs, prev);
      if (has_freq && len == kBlock) {
        block_io::SkipBlock(in);
      }
      return read;
    }
  };

  const auto emit = [&]<size_t N>(const block_io::FillLeaf& leaf,
                                  uint32_t len) IRS_FORCE_INLINE {
    if (leaf.IsRun()) {
      sink.Run(prev, len);
    } else if (leaf.IsBitset()) {
      sink.Bitset(prev, leaf.bitset, leaf.words, leaf.max);
    } else {
      const auto* const data = docs + kBlock - len;
      if constexpr (Sink::kOrdered) {
        for (uint32_t i = 0; i != len; ++i) {
          sink.Doc(data[i]);
        }
      } else {
        VisitDocs<N>(len,
                     [&](uint32_t i) IRS_FORCE_INLINE { sink.Doc(data[i]); });
      }
    }
  };

  while (left != 0) {
    if (live != nullptr) {
      const auto want = live->Next(std::max<doc_id_t>(prev + 1, min));
      if (want >= max) {
        return max;
      }
      if (want > prev + 1) {
        const auto rest = cursor->Seek(want, in);
        if (rest == 0) {
          return doc_limits::eof();
        }
        if (const auto landing = cursor->Landing(); landing.doc > prev) {
          in.Seek(landing.doc_ptr);
          seat();
          prev = landing.doc;
          left = rest;
        }
      }
    }
    const auto len = std::min(left, kBlock);
    const auto leaf = read_leaf(len);
    left -= len;
    if (prev + 1 >= min && leaf.max < max) [[likely]] {
      if (len == kBlock) {
        emit.template operator()<kBlock>(leaf, len);
      } else {
        emit.template operator()<std::dynamic_extent>(leaf, len);
      }
      prev = leaf.max;
      continue;
    }
    if (leaf.max >= min) {
      auto* const first = docs + kBlock - len;
      if (leaf.IsRun()) {
        block_codec::FillProgression(first, len, prev, 1);
      } else if (leaf.IsBitset()) {
        block_io::MaterializeBitsetFrom(prev, leaf.bitset, 0, leaf.bitset[0],
                                        leaf.words, first);
      }
      for (const auto* it = first; it != docs + kBlock; ++it) {
        if (*it >= max) {
          return *it;
        }
        if (*it >= min) {
          sink.Doc(*it);
        }
      }
    }
    prev = leaf.max;
  }
  return doc_limits::eof();
}

template<typename Input>
class PostingReader {
 public:
  explicit PostingReader(const IndexInput& doc) noexcept : _doc{&doc} {}

  Input& In() {
    if (_in == nullptr) [[unlikely]] {
      _owned = _doc->Reopen();
      SDB_ASSERT(_owned);
      _in = &irs::utils::downCast<Input>(*_owned);
    }
    return *_in;
  }

  template<typename Sink>
  void Read(const PostingMeta& meta, bool has_score_bounds, bool has_freq,
            Sink& sink) {
    Read(meta, IndexFeatures::None, has_score_bounds, has_freq,
         doc_limits::min(), doc_limits::eof(), nullptr, sink);
  }

  template<typename Sink>
  doc_id_t Read(const PostingMeta& meta, IndexFeatures layout,
                bool has_score_bounds, bool has_freq, doc_id_t min,
                doc_id_t max, const LiveWords* live, Sink& sink) {
    SDB_ASSERT(meta.docs_count > 1);
    constexpr auto kStart = doc_limits::invalid();
    if (meta.inline_size != 0) {
      BytesViewInput in{meta.Inline()};
      SkipScoreBounds(has_score_bounds, in);
      return ReadPosting(in, Enc(), Holes(), Docs(), has_freq, kStart,
                         meta.docs_count, min, max, nullptr, nullptr, sink);
    }
    auto& in = In();
    const bool first = min == doc_limits::min();
    if (first) {
      PrefetchDocs(in, meta);
    }
    if (meta.docs_count <= doc_limits::kBlockSize ||
        (first && live == nullptr)) {
      in.Seek(meta.doc_start);
      if (meta.docs_count <= doc_limits::kBlockSize) {
        SkipScoreBounds(has_score_bounds, in);
      }
      return ReadPosting(in, Enc(), Holes(), Docs(), has_freq, kStart,
                         meta.docs_count, min, max, nullptr, nullptr, sink);
    }
    _cursor.Arm(meta, BlockIndexShapeOf(layout, has_score_bounds));
    const auto left = _cursor.Seek(min, in);
    if (left == 0) {
      return doc_limits::eof();
    }
    const auto landing = _cursor.Landing();
    in.Seek(landing.doc_ptr);
    return ReadPosting(in, Enc(), Holes(), Docs(), has_freq, landing.doc, left,
                       min, max, &_cursor, live, sink);
  }

 private:
  uint32_t* Enc() noexcept { return EncOf<Input>(_enc); }

  uint64_t* Holes() noexcept { return _holes.data; }

  doc_id_t* Docs() noexcept { return _buf; }

  const IndexInput* _doc;
  IndexInput::ptr _owned;
  Input* _in = nullptr;
  BlockCursor _cursor;
  DocsBuf _buf;
  HoleBuf _holes;
  [[no_unique_address]] NeedEnc<Input> _enc;
};

template<typename Term, typename Input>
void PrefetchTerms(std::span<const Term> terms, PostingReader<Input>& r) {
  const auto metas =
    terms | std::views::transform([](const Term& term) -> const PostingMeta& {
      return CookieOf(term);
    });
  if (absl::c_any_of(
        metas, [](const PostingMeta& meta) { return DocExtent(meta) != 0; })) {
    PrefetchDocExtents(r.In(), metas);
  }
}

template<typename Sink, typename Input>
void FillTerms(std::span<const PostingClause> terms,
               doc_id_t* IRS_RESTRICT next, PostingReader<Input>& r,
               doc_id_t min, doc_id_t max, const LiveWords* live, Sink& sink) {
  if (min == doc_limits::min()) {
    PrefetchTerms(terms, r);
  }
  for (size_t i = 0; i != terms.size(); ++i) {
    if (next[i] >= max) {
      continue;
    }
    const auto& meta = CookieOf(terms[i]);
    SDB_ASSERT(meta.docs_count != 0);
    if (meta.docs_count == 1) {
      const auto doc = doc_limits::min() + meta.doc_delta;
      if (doc >= min && doc < max) {
        sink.Doc(doc);
      }
      next[i] = doc >= max ? doc : doc_limits::eof();
      continue;
    }
    const auto& own = FieldOf(terms[i], nullptr);
    next[i] = r.Read(meta, LayoutOf(own), BoundsOf(own), FreqOf(own), min, max,
                     live, sink);
  }
}

template<typename Term, typename Sink, typename Input>
void ReadTerms(std::span<const Term> terms, const TermReader* field,
               PostingReader<Input>& r, Sink& sink) {
  PrefetchTerms(terms, r);
  for (size_t i = 0; i != terms.size(); ++i) {
    if constexpr (requires { sink.Term(); }) {
      sink.Term();
    }
    const auto& meta = CookieOf(terms[i]);
    SDB_ASSERT(meta.docs_count != 0);
    if (meta.docs_count == 1) {
      sink.Doc(doc_limits::min() + meta.doc_delta);
      continue;
    }
    const auto& own = FieldOf(terms[i], field);
    r.Read(meta, BoundsOf(own), FreqOf(own), sink);
  }
}

inline void ReadFill(FillNode& node, doc_id_t min, doc_id_t max,
                     const LiveWords* live, uint64_t* IRS_RESTRICT words) {
  constexpr auto kBits = BitsetStorage::kBits;
  for (auto at = min; at < max;) {
    if (live != nullptr) {
      const auto want = live->Next(at);
      if (want >= max) {
        return;
      }
      at = want - (want - min) % kWindowDocs;
    }
    const auto stop = max - at > kWindowDocs ? at + kWindowDocs : max;
    const auto next = node.FillOr(at, stop, words + (at - min) / kBits);
    if (next >= max) {
      return;
    }
    at = std::max(stop, next - (next - min) % kWindowDocs);
  }
}

struct BitsetBuckets {
  std::vector<std::vector<PostingClause>> must;
  std::vector<PostingClause> must_not;
  std::vector<FillNode::ptr> fills;
  std::vector<FillNode::ptr> exclude_fills;
  const SubReader* masked = nullptr;
  std::vector<doc_id_t> next;

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

  bool DenseLead(doc_id_t docs_count) const noexcept {
    return must.size() == 1 &&
           AppliedInPlace(std::span<const PostingClause>{must.front()},
                          docs_count);
  }
};

inline void FillBitset(BitsetBuckets& buckets, const IndexInput& doc,
                       doc_id_t docs_count, doc_id_t min, doc_id_t max,
                       uint64_t* IRS_RESTRICT words) {
  SDB_ASSERT(!buckets.must.empty() || !buckets.fills.empty());
  SDB_ASSERT(buckets.fills.empty() || buckets.must.size() <= 1);
  SDB_ASSERT(min < max);
  constexpr auto kBits = BitsetStorage::kBits;
  const auto count =
    static_cast<uint32_t>((uint64_t{max - min} + kBits - 1) / kBits);
  const auto seed = buckets.Seed(docs_count);
  if (buckets.next.empty()) {
    auto terms = buckets.must_not.size();
    for (const auto& clause : buckets.must) {
      terms += clause.size();
    }
    buckets.next.assign(terms, doc_limits::invalid());
  }
  const auto next_of = [&](size_t clause) {
    auto* next = buckets.next.data();
    for (size_t i = 0; i != clause; ++i) {
      next += buckets.must[i].size();
    }
    return next;
  };

  ResolveInput(doc, [&]<typename Input> {
    PostingReader<Input> reader{doc};
    if (!buckets.must.empty()) {
      OrBits or_seed{.words = words, .base = min};
      FillTerms(std::span<const PostingClause>{buckets.must[seed]},
                next_of(seed), reader, min, max, nullptr, or_seed);
    }
    for (auto& node : buckets.fills) {
      SDB_ASSERT(node);
      ReadFill(*node, min, max, nullptr, words);
    }

    std::optional<BitsetStorage> scratch;
    const auto open_scratch = [&]() -> uint64_t* {
      if (!scratch) {
        scratch.emplace(max - min);
      } else {
        std::fill_n(scratch->Words(), count, uint64_t{0});
      }
      return scratch->Words();
    };
    LiveWords live;
    const auto gate = [&] {
      live.Reset(words, count, min);
      return &live;
    };

    for (size_t i = 0, n = buckets.must.size(); i != n; ++i) {
      if (i == seed) {
        continue;
      }
      const std::span<const PostingClause> clause{buckets.must[i]};
      SDB_ASSERT(!clause.empty());
      const auto* const alive = gate();
      if (AppliedInPlace(clause, docs_count)) {
        RetainBits retain{.words = words, .base = min};
        FillTerms(clause, next_of(i), reader, min, max, alive, retain);
        retain.Finish(count);
        continue;
      }
      auto* const other = open_scratch();
      OrBits or_clause{.words = other, .base = min};
      FillTerms(clause, next_of(i), reader, min, max, alive, or_clause);
      for (uint32_t w = 0; w != count; ++w) {
        words[w] &= other[w];
      }
    }

    if (!buckets.must_not.empty()) {
      ClearBits clear{.words = words, .base = min};
      FillTerms(std::span<const PostingClause>{buckets.must_not},
                next_of(buckets.must.size()), reader, min, max, gate(), clear);
    }
    if (!buckets.exclude_fills.empty()) {
      const auto* const alive = gate();
      auto* const other = open_scratch();
      for (auto& node : buckets.exclude_fills) {
        SDB_ASSERT(node);
        ReadFill(*node, min, max, alive, other);
      }
      for (uint32_t w = 0; w != count; ++w) {
        words[w] &= ~other[w];
      }
    }
  });

  if (buckets.masked != nullptr) {
    ResolveDocsMask(*buckets.masked, [&]<DocsMaskType Mask>(Mask mask) {
      mask.Remove(min, max, words);
    });
  }
}

inline BitsetStorage BuildBitset(BitsetBuckets& buckets, const IndexInput& doc,
                                 doc_id_t docs_count) {
  BitsetStorage bits{docs_count};
  FillBitset(buckets, doc, docs_count, BitsetStorage::kMin, bits.End(),
             bits.Words());
  bits.Trim();
  return bits;
}

}  // namespace irs::detail
