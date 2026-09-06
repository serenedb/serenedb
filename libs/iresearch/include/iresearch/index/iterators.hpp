////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/functional/function_ref.h>
#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "basics/system-compiler.h"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/iterator.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class PosAttr;

// One term's documents as a merge hands them to the postings writer: in
// ascending order, one at a time, with what the field stores beside each of
// them. A document stream and nothing else -- no score, no probe, no
// two-phase check -- so it is a `lead::Node`. Flush hands the writer
// `PostingRows` instead.
struct TermPostings : lead::Node {
  using ptr = memory::managed_ptr<TermPostings>;

  // What a consumer is handed back when the attributes below change identity.
  using AttrRefresh = absl::FunctionRef<void(TermPostings&)>;

  [[nodiscard]] static ptr empty() noexcept;

  // The write path reads a list front to back, so no stream here seeks.
  doc_id_t Seek(doc_id_t /*target*/) final {
    SDB_ASSERT(false);
    return _doc = doc_limits::eof();
  }

  // The frequency of the document this stands on; unread for a field that
  // stores none.
  virtual uint32_t GetFreq() const = 0;

  // The positions of that document, null for a field that stores none. Where
  // the field has offsets they are an attribute of what this returns.
  virtual PosAttr* Positions() noexcept { return nullptr; }

  // A stream concatenating several sources answers with a different provider
  // once per source, and says so through this. The call is what arms the
  // consumer, so a stream that answers with its own provider throughout still
  // owes exactly one -- which is the default. Staying silent leaves the
  // consumer holding nothing.
  virtual void Subscribe(AttrRefresh refresh) { refresh(*this); }
};

struct ScoreDoc {
  score_t score = 0.0f;
  doc_id_t doc = doc_limits::eof();
  uint32_t segment_idx = 0;

  bool operator==(const ScoreDoc& other) const = default;
};

// TODO(mbkkt) Try to make it autovectorized,
// otherwise try to use xsimd/neon specific intrinsics
class LoserScoreCollector {
 public:
  LoserScoreCollector(score_t& score_threshold, std::span<ScoreDoc> hits)
    : _score_threshold{&score_threshold},
      _hits{hits.data()},
      _k{hits.size()},
      _tree(hits.size()) {
    SDB_ASSERT(!hits.empty());
  }

  IRS_FORCE_INLINE size_t AcceptedCount() const noexcept { return _size; }

  void SetScoreThreshold(score_t& score_threshold) noexcept {
    SDB_ASSERT(score_threshold <= *_score_threshold);
    score_threshold = *_score_threshold;
    _score_threshold = &score_threshold;
  }

  // The one place the threshold moves, and it only ever rises. Two things
  // raise it and neither knows about the other: this collector's own k-th
  // whenever it has k hits, and the scan seeding a segment with the best k-th
  // any worker has published. The hits are not dropped at that seed, so hits
  // accepted under the older threshold stay in and the next k-th can be below
  // the seeded value -- routing every raise through here is what keeps that
  // lower k-th from writing the threshold back down. SetScoreThreshold
  // asserts on it.
  IRS_FORCE_INLINE void RaiseScoreThreshold(score_t score_threshold) noexcept {
    *_score_threshold = std::max(*_score_threshold, score_threshold);
  }

  void SetSegment(uint32_t idx) noexcept { _current_segment = idx; }

  // The `k`-th score this collector holds, and what a plan free to leave
  // documents out is measured against. It only rises, and it spans every
  // segment of the query, so a plan reads it as it goes rather than once.
  IRS_FORCE_INLINE score_t ScoreThreshold() const noexcept {
    return *_score_threshold;
  }

  IRS_FORCE_INLINE uint64_t TotalMatches() const noexcept { return _count; }

  IRS_FORCE_INLINE void Add(score_t score, doc_id_t doc) noexcept {
    ++_count;
    TryPush(*_score_threshold, score, doc);
  }

  IRS_FORCE_INLINE void ConsumeWindow(score_t* scores, uint64_t* mask,
                                      doc_id_t min,
                                      size_t num_blocks) noexcept {
    score_t threshold = *_score_threshold;
    for (size_t i = 0; i < num_blocks; ++i) {
      auto word = mask[i];
      if (word == 0) [[likely]] {
        continue;
      }
      mask[i] = 0;

      _count += std::popcount(word);
      auto* IRS_RESTRICT const score_base =
        scores + i * BitsRequired<uint64_t>();
#ifdef __AVX2__
      word &= GetScoreMask(score_base, threshold);
#endif
      const doc_id_t doc_base = min + i * BitsRequired<uint64_t>();

      while (word != 0) {
        const doc_id_t bit = std::countr_zero(word);
        word = PopBit(word);
        TryPush(threshold, score_base[bit], doc_base + bit);
      }

      std::memset(score_base, 0, BitsRequired<uint64_t>() * sizeof(score_t));
    }
    *_score_threshold = threshold;
  }

  IRS_FORCE_INLINE void AddDocs(const doc_id_t* docs, size_t count,
                                const score_t* scores) noexcept {
    _count += count;
    score_t threshold = *_score_threshold;
    size_t i = 0;
#ifdef __AVX2__
    for (; i + 8 <= count; i += 8) {
      auto pass = static_cast<unsigned>(_mm256_movemask_ps(_mm256_cmp_ps(
        _mm256_loadu_ps(scores + i), _mm256_set1_ps(threshold), kCmpPred)));
      while (pass != 0) {
        const int bit = std::countr_zero(pass);
        pass = PopBit(pass);
        TryPush(threshold, scores[i + bit], docs[i + bit]);
      }
    }
#endif
    for (; i < count; ++i) {
      TryPush(threshold, scores[i], docs[i]);
    }
    *_score_threshold = threshold;
  }

 private:
  IRS_FORCE_INLINE void TryPush(score_t& threshold, score_t score,
                                doc_id_t doc) noexcept {
    if (score > threshold) {
      Push(threshold, score, doc);
    }
  }

#ifdef __AVX2__
  static constexpr int kCmpPred = _CMP_GT_OQ;

  IRS_FORCE_INLINE static uint64_t GetScoreMask(
    const score_t* IRS_RESTRICT scores, score_t threshold) noexcept {
    const auto v = _mm256_set1_ps(threshold);
    uint64_t mask = 0;
    for (int i = 0; i < 64; i += 8) {
      const uint64_t bits = _mm256_movemask_ps(
        _mm256_cmp_ps(_mm256_loadu_ps(scores + i), v, kCmpPred));
      mask |= bits << i;
    }
    return mask;
  }
#endif

  struct Node {
    score_t score;
    uint32_t leaf;
  };

  static constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();

  IRS_FORCE_INLINE size_t Match(uint32_t leaf) const noexcept {
    return (_k + leaf) >> 1;
  }

  void Build() noexcept {
    Node* IRS_RESTRICT const tree = _tree.data();
    for (size_t i = 1; i < _k; ++i) {
      tree[i].leaf = kNone;
    }
    for (uint32_t leaf = 0; leaf != _k; ++leaf) {
      Node cur{_hits[leaf].score, leaf};
      for (size_t node = Match(leaf); node != 0; node >>= 1) {
        Node& slot = tree[node];
        if (slot.leaf == kNone) {
          slot = cur;
          cur.leaf = kNone;
          break;
        }
        if (slot.score < cur.score) {
          std::swap(slot, cur);
        }
      }
      if (cur.leaf != kNone) {
        _root = cur;
      }
    }
  }

  IRS_FORCE_INLINE void Replace(ScoreDoc hit) noexcept {
    Node* IRS_RESTRICT const tree = _tree.data();
    const uint32_t leaf = _root.leaf;
    _hits[leaf] = hit;
    Node cur{hit.score, leaf};
    for (size_t node = Match(leaf); node != 0; node >>= 1) {
      const Node loser = tree[node];
      const bool win = loser.score < cur.score;
      tree[node] = win ? cur : loser;
      cur = win ? loser : cur;
    }
    _root = cur;
  }

  IRS_FORCE_INLINE void Push(score_t& threshold, score_t score,
                             doc_id_t doc) noexcept {
    if (_size != _k) [[unlikely]] {
      _hits[_size++] = {score, doc, _current_segment};
      if (_size != _k) {
        return;
      }
      Build();
    } else {
      Replace({score, doc, _current_segment});
    }
    threshold = std::max(threshold, _root.score);
  }

  uint64_t _count = 0;
  uint32_t _current_segment = 0;
  score_t* IRS_RESTRICT _score_threshold;
  ScoreDoc* IRS_RESTRICT _hits;
  size_t _k;
  std::vector<Node> _tree;
  size_t _size = 0;
  Node _root{};
};

// One term's postings as block-contiguous columns: one row per (doc,
// position) occurrence, docs non-decreasing, freq(doc) = run length of
// equal doc. `pos`/`offs_*` are null when the field lacks the feature.
struct PostingsSpan {
  const uint32_t* docs;
  const uint32_t* pos;
  const uint32_t* offs_start;
  const uint32_t* offs_end;
  size_t count;
};

// One term's postings rows for a span-capable writer: either one
// contiguous span, or a row range over 2-level blocked lanes (the flush
// scatter shape), read in place -- no virtual dispatch, no copies.
// `span.docs` selects the form.
struct PostingRows {
  PostingsSpan span;
  const uint32_t* const* docs_blocks;
  const uint32_t* const* pos_blocks;
  const uint32_t* const* offs_start_blocks;
  const uint32_t* const* offs_end_blocks;
  uint64_t begin;
  uint64_t end;
  uint32_t block_shift;

  // Walks doc runs in order: `on_doc(doc, freq)` once per doc with its
  // complete freq (the skip protocol needs it up front), then
  // `on_positions(pos, offs_start, offs_end, n)` for each contiguous piece
  // of the doc's rows (one in all but the block-boundary case). The
  // doc-only overload compiles the position emission out.
  template<typename OnDoc>
  void VisitRuns(OnDoc&& on_doc) const {
    Visit<false>(on_doc, nullptr);
  }

  template<typename OnDoc, typename OnPositions>
  void VisitRuns(OnDoc&& on_doc, OnPositions&& on_positions) const {
    Visit<true>(on_doc, on_positions);
  }

 private:
  template<bool WithPositions, typename OnDoc, typename OnPositions>
  void Visit(OnDoc&& on_doc, OnPositions&& on_positions) const {
    if (span.docs != nullptr) {
      size_t i = 0;
      while (i < span.count) {
        const doc_id_t doc = span.docs[i];
        size_t j = i + 1;
        while (j < span.count && span.docs[j] == doc) {
          ++j;
        }
        on_doc(doc, static_cast<uint32_t>(j - i));
        if constexpr (WithPositions) {
          on_positions(span.pos + i,
                       span.offs_start ? span.offs_start + i : nullptr,
                       span.offs_end ? span.offs_end + i : nullptr, j - i);
        }
        i = j;
      }
      return;
    }
    // blocked form, taken only by a block-crossing term: a flat row cursor
    // makes runs cross block boundaries transparently, so the freq is
    // complete before the doc is handed over; only position emission has
    // to split at block edges (the lanes are not contiguous across them)
    const uint64_t mask = (uint64_t{1} << block_shift) - 1;
    const auto doc_at = [&](uint64_t row) {
      return docs_blocks[row >> block_shift][row & mask];
    };
    for (auto row = begin; row != end;) {
      const doc_id_t doc = doc_at(row);
      auto run_end = row + 1;
      while (run_end != end && doc_at(run_end) == doc) {
        ++run_end;
      }
      on_doc(doc, static_cast<uint32_t>(run_end - row));
      if constexpr (WithPositions) {
        for (auto cur = row; cur != run_end;) {
          const auto block = cur >> block_shift;
          const auto off = static_cast<size_t>(cur & mask);
          const auto stop = std::min(run_end, (block + 1) << block_shift);
          on_positions(
            pos_blocks[block] + off,
            offs_start_blocks ? offs_start_blocks[block] + off : nullptr,
            offs_end_blocks ? offs_end_blocks[block] + off : nullptr,
            static_cast<size_t>(stop - cur));
          cur = stop;
        }
      }
      row = run_end;
    }
  }
};

// What a field writer reads: each term hands over its posting list whole,
// through the batched span pull when the reader provides one and through
// `postings()`/`PostingsWriter::Write` otherwise. The write-side walk never
// asks what a term's postings cost, so it owes no record -- a separate
// contract rather than a mode of `TermIterator`.
struct TermOnlyIterator : Iterator<bytes_view, AttributeProvider> {
  using ptr = memory::managed_ptr<TermOnlyIterator>;

  // Return the associated posting list with the requested features.
  [[nodiscard]] virtual TermPostings::ptr postings(
    IndexFeatures features) const = 0;

  // Columnar in-memory readers expose the current term's postings as
  // in-place row views so a postings writer can consume them without
  // per-doc iterator dispatch; file-backed and merge iterators keep
  // `postings`.
  // Batched term pull: fills a prefix of the parallel `terms`/`postings`
  // arrays with consecutive terms. Returns how many terms were filled
  // (0 = exhausted). Term views and row data stay valid until the following
  // NextTermsWithPostings call. A reader without a batched pull is
  // compaction-only and must never reach the flush path, so the default
  // refuses rather than reporting an empty field.
  virtual size_t NextTermsWithPostings(std::span<bytes_view> /*terms*/,
                                       std::span<PostingRows> /*postings*/,
                                       IndexFeatures /*features*/) {
    throw IndexError{"term reader has no batched postings pull"};
  }
};

struct TermIterator : TermOnlyIterator {
  using ptr = memory::managed_ptr<TermIterator>;

  // Where the current term's postings live and how big they are. Decoding
  // happens here, once, so statistics collection and posting-list construction
  // share one parse of the entry. Answers the term the iterator stands on, so
  // it is called per term -- a pointer kept across a move reports the term it
  // was taken on. The reference is the iterator's own storage and dies with the
  // next move; copy to keep it.
  [[nodiscard]] virtual const PostingMeta& cookie() const = 0;
};

// Represents a result of seek operation
enum class SeekResult {
  // Exact value is found
  Found = 0,
  // Exact value is not found, an iterator is positioned at the next
  // greater value.
  NotFound,
  // No value greater than a target found, eof
  End,
};

// An iterator providing random and sequential access to term
// dictionary.
struct SeekTermIterator : TermIterator {
  using ptr = memory::managed_ptr<SeekTermIterator>;

  [[nodiscard]] static SeekTermIterator::ptr empty() noexcept;

  // Position iterator at a value that is not less than the specified
  // one. Returns seek result.
  virtual SeekResult seek_ge(bytes_view value) = 0;

  // Position iterator at `value` exactly. Returns `true` when the dictionary
  // holds it, `false` otherwise -- and on `false` the iterator is left wherever
  // answering took it, so its value must not be read.
  //
  // Not `seek_ge` plus a comparison: a miss is answered as soon as the block
  // that would hold the key says it has no terms, where `seek_ge` still has to
  // read the block after it to have somewhere to stand. Every exact-match
  // filter goes through here for that reason.
  virtual bool seek(bytes_view value) = 0;
};

// Position iterator to the specified min term or to the next term
// after the min term depending on the specified `Include` value.
// Returns true in case if iterator has been successfully positioned,
// false otherwise.
template<bool Include>
bool seek_min(SeekTermIterator& it, bytes_view min) {
  const auto res = it.seek_ge(min);

  return SeekResult::End != res &&
         (Include || SeekResult::Found != res || it.next());
}

// Position iterator `count` items after the current position.
// Returns true if the iterator has been successfully positioned
template<typename Iterator>
bool skip(Iterator& itr, size_t count) {
  while (count--) {
    if (!itr.next()) {
      return false;
    }
  }

  return true;
}

}  // namespace irs
