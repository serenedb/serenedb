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
#include <cstring>
#include <memory>
#include <span>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/formats/posting/block_io.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/doc_input.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/enc_buf.hpp"
#include "iresearch/search/detail/landings.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

enum class LeafReads : uint8_t {
  Grows,
  Whole,
};

struct LeafShape {
  bool scored = false;
  bool defer = false;
  bool freqs = false;
  bool gather = false;
  bool cursor = false;
  bool enc = false;
  bool delta = false;
  bool holes = false;
  bool docs = true;
  LeafReads reads = LeafReads::Grows;
};

inline constexpr LeafShape kFillShape{.cursor = true, .holes = true};

inline constexpr LeafShape kBatchShape{.docs = false};

inline constexpr LeafShape kScoredBatchShape{
  .scored = true,
  .freqs = true,
  .docs = false,
};

inline constexpr LeafShape kPosShape{.freqs = true, .delta = true};

inline constexpr LeafShape kCountShape{
  .delta = true,
  .reads = LeafReads::Whole,
};

inline constexpr LeafShape kWindowScoredShape{
  .scored = true,
  .freqs = true,
  .enc = true,
  .reads = LeafReads::Whole,
};

inline constexpr LeafShape kCursorShape{
  .cursor = true,
  .delta = true,
};

inline constexpr LeafShape kCursorScoredShape{
  .defer = true,
  .freqs = true,
  .gather = true,
  .cursor = true,
  .delta = true,
};

inline constexpr LeafShape kProbeShape{.cursor = true};

inline constexpr LeafShape kProbeScoredShape{
  .defer = true,
  .freqs = true,
  .gather = true,
  .cursor = true,
};

struct FreqLen {
  uint32_t value = 0;
};

struct DocSpan {
  doc_id_t first;
  doc_id_t last;
};

struct BlockRun {
  uint32_t first;
  uint32_t last;
};

inline constexpr size_t kMostSpans = 2 * BlockIndex::kRun;

struct LeafScore {
  ColumnArgsFetcher* fetcher = nullptr;
  ScoreFunction score;
  doc_id_t next_last = doc_limits::invalid();
};

struct LeafCursor {
  doc_id_t base = 0;
};

template<ScoreMergeType MergeType, bool Counts>
IRS_FORCE_INLINE void AddDoc(doc_id_t doc, score_t score, doc_id_t min,
                             uint32_t* IRS_RESTRICT counts,
                             uint64_t* IRS_RESTRICT mask,
                             score_t* IRS_RESTRICT window) noexcept {
  static constexpr auto kBits = BitsRequired<uint64_t>();
  const size_t offset = doc - min;
  if constexpr (Counts) {
    ++counts[offset];
  }
  SetBit(mask[offset / kBits], offset % kBits);
  irs::Merge<MergeType>(window[offset], score);
}

template<ScoreMergeType MergeType, bool Counts>
IRS_FORCE_INLINE void AddWhole(const doc_id_t* begin, const doc_id_t* end,
                               const score_t* IRS_RESTRICT scores, doc_id_t min,
                               uint32_t* IRS_RESTRICT counts,
                               uint64_t* IRS_RESTRICT mask,
                               score_t* IRS_RESTRICT window) noexcept {
  const auto len = static_cast<uint32_t>(end - begin);
  if (len == doc_limits::kBlockSize) [[likely]] {
    VisitDocs<doc_limits::kBlockSize>(
      doc_limits::kBlockSize, [&](uint32_t i) IRS_FORCE_INLINE {
        AddDoc<MergeType, Counts>(begin[i], scores[i], min, counts, mask,
                                  window);
      });
  } else {
    for (uint32_t i = 0; i != len; ++i) {
      AddDoc<MergeType, Counts>(begin[i], scores[i], min, counts, mask, window);
    }
  }
}

template<ScoreMergeType MergeType, bool Counts>
IRS_FORCE_INLINE const doc_id_t* AddUntil(
  const doc_id_t* begin, const doc_id_t* end,
  const score_t* IRS_RESTRICT scores, doc_id_t min, doc_id_t max,
  uint32_t* IRS_RESTRICT counts, uint64_t* IRS_RESTRICT mask,
  score_t* IRS_RESTRICT window) noexcept {
  SDB_ASSERT(begin != end && *(end - 1) >= max);
  for (; *begin < max; ++begin, ++scores) {
    AddDoc<MergeType, Counts>(*begin, *scores, min, counts, mask, window);
  }
  return begin;
}

template<typename InputType, LeafShape Shape>
class PostingLeaf {
 public:
  static constexpr bool kDefaultInit = true;

  PostingLeaf() = default;

  PostingLeaf(const PostingLeaf&) = delete;
  PostingLeaf& operator=(const PostingLeaf&) = delete;
  PostingLeaf(PostingLeaf&&) = delete;
  PostingLeaf& operator=(PostingLeaf&&) = delete;

  IRS_FORCE_INLINE doc_id_t Next() {
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadLeafDelta(_last);
    }
    _doc = *(std::end(_docs) - _left_in_leaf);
    --_left_in_leaf;
    return _doc;
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    static_assert(Shape.cursor && Shape.delta);
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    if (_last < target && !ReadToDelta(target)) [[unlikely]] {
      _left_in_leaf = 0;
      return _doc = doc_limits::eof();
    }

    if (_left_in_list != 0) [[likely]] {
      const auto* it =
        BranchlessLowerBound<doc_limits::kBlockSize>(std::begin(_docs), target);
      _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - it) - 1;
      return _doc = *it;
    }

    return Scan(target);
  }

  IRS_FORCE_INLINE doc_id_t Seek(doc_id_t target) {
    static_assert(Shape.cursor && Shape.delta);
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    if (_last < target && !ReadToDelta(target)) [[unlikely]] {
      _left_in_leaf = 0;
      return _doc = doc_limits::eof();
    }

    return Scan(target);
  }

  const ScoreInputs* Inputs() const noexcept
    requires(Shape.defer && Shape.gather)
  {
    return &_provider;
  }

 protected:
  static constexpr bool kEnc = Shape.enc || !InputType::CONTIGUOUS;
  static constexpr uint32_t kBlock = doc_limits::kBlockSize;
  static constexpr auto kBits = BitsRequired<uint64_t>();
  static constexpr doc_id_t kForeseenDocs = doc_id_t{1} << 16;

  IRS_FORCE_INLINE InputType& In() noexcept { return _in; }

  IRS_FORCE_INLINE uint32_t* Enc() noexcept {
    if constexpr (kEnc) {
      return _enc.data;
    } else {
      return nullptr;
    }
  }

  IRS_FORCE_INLINE uint64_t* Holes() noexcept {
    if constexpr (Shape.holes) {
      return _holes.data;
    } else {
      return nullptr;
    }
  }

  IRS_FORCE_INLINE score_t* Scores() noexcept {
    static_assert(Shape.enc && sizeof(score_t) == sizeof(uint32_t));
    return reinterpret_cast<score_t*>(_enc.data);
  }

  IRS_FORCE_INLINE const score_t* Scores() const noexcept {
    static_assert(Shape.enc && sizeof(score_t) == sizeof(uint32_t));
    return reinterpret_cast<const score_t*>(_enc.data);
  }

  IRS_FORCE_INLINE const score_t* ScoresAt(const doc_id_t* doc) const noexcept {
    return Scores() + (doc - std::cbegin(_docs));
  }

  IRS_FORCE_INLINE const UnalignedWord* StableBitset(
    const block_io::FillLeaf& leaf) noexcept {
    SDB_ASSERT(!leaf.IsBitset() || leaf.words <= _docs.size() *
                                                   sizeof(doc_id_t) /
                                                   sizeof(uint64_t));
    return block_io::StableBitset<InputType>(leaf, _docs.begin());
  }

  void OpenInput(const PostingMeta& meta, const duckdb::InputFile& doc_in,
                 bool bounds, bool scans = false) {
    _in = OpenDocInput<InputType>(meta, doc_in,
                                  scans ? duckdb::ReadAheadSteps::SCANNING
                                  : Shape.reads == LeafReads::Grows
                                    ? duckdb::ReadAheadSteps::SKIPPING
                                    : duckdb::ReadAheadSteps::NONE);
    auto& in = In();
    if constexpr (Shape.reads == LeafReads::Whole) {
      PrefetchDocs(doc_in, meta);
      _foreseen = 0;
    }
    if (meta.docs_count <= kBlock) {
      SkipScoreBounds(bounds, in);
    }
    _left_in_list = meta.docs_count;
  }

  void ArmWalk(const PostingMeta& meta, IndexFeatures layout, bool bounds) {
    if (meta.docs_count > kBlock) {
      _walk.Arm(meta, BlockIndexShapeOf(layout, bounds));
    }
  }

  bool OpenList(const PostingMeta& meta, const duckdb::InputFile& doc_in,
                IndexFeatures layout, bool bounds, bool scans = false) {
    SDB_ASSERT(meta.docs_count != 0);
    if (meta.docs_count == 1) {
      return false;
    }
    OpenInput(meta, doc_in, bounds, scans);
    ArmWalk(meta, layout, bounds);
    return true;
  }

  IRS_FORCE_INLINE bool Armed() const noexcept { return _walk.Armed(); }

  IRS_FORCE_INLINE BlockCursor<InputType>& Walk() noexcept { return _walk; }

  IRS_NO_INLINE void Land(doc_id_t min) {
    auto& walk = Walk();
    const auto left = walk.Seek(min, _in);
    _left_in_leaf = 0;
    _left_in_list = left;
    if (left == 0) [[unlikely]] {
      _doc = doc_limits::eof();
      return;
    }
    const auto landing = walk.Landing();
    In().Seek(landing.doc_ptr);
    _last = landing.doc;
  }

  IRS_FORCE_INLINE bool Start(doc_id_t min, doc_id_t max) {
    if (_doc >= max) {
      return false;
    }
    if (min > _last + doc_limits::kBlockSize && Armed()) [[unlikely]] {
      Land(min);
      if (doc_limits::eof(_doc)) {
        return false;
      }
    }
    if constexpr (Shape.reads == LeafReads::Whole) {
      if (uint64_t{max} + kForeseenDocs / 2 > _foreseen && Armed() &&
          !_walk.Pending()) [[unlikely]] {
        ForeseeFrom(std::max(min, _foreseen), max);
      }
    }
    return true;
  }

  IRS_NO_INLINE void ForeseeFrom(doc_id_t from, doc_id_t max) {
    _foreseen = static_cast<doc_id_t>(
      std::min<uint64_t>(uint64_t{max} + kForeseenDocs, doc_limits::eof()));
    const DocSpan span{.first = from, .last = _foreseen - 1};
    ForeseeSpans({&span, 1});
  }

  IRS_FORCE_INLINE void SetFreqLen(bool has_freq) noexcept {
    static_assert(!Shape.freqs);
    _freq_len.value = has_freq ? kBlock : 0;
  }

  IRS_FORCE_INLINE doc_id_t SetSingle(const PostingMeta& meta) noexcept {
    const auto doc = doc_limits::min() + meta.doc_delta;
    *(std::end(_docs) - 1) = doc;
    if constexpr (Shape.freqs) {
      *(std::end(_freqs.data) - 1) = meta.freq;
    }
    _last = doc;
    _left_in_leaf = 1;
    return doc;
  }

  void MakeScore(const SubReader& segment, const TermReader& field,
                 const ScoreArgs& args) {
    static_assert(Shape.scored && Shape.freqs);
    _score.fetcher = args.fetcher;
    _provider.freq.value = _freqs.data;
    _score.score = PrepareScore(args, segment, field.meta(), _provider);
  }

  IRS_FORCE_INLINE void SetRecipe(const SubReader& segment,
                                  const TermReader& field,
                                  const ScoreArgs& args) noexcept {
    static_assert(Shape.defer);
    _provider.recipe = {.segment = &segment, .field = &field, .args = args};
  }

  ScoreFunction MakeDeferredScore() {
    static_assert(Shape.defer && Shape.gather);
    _provider.freq.value = _gather.data;
    const auto& recipe = _provider.recipe;
    SDB_ASSERT(recipe.segment != nullptr && recipe.field != nullptr);
    return PrepareScore(recipe.args, *recipe.segment, recipe.field->meta(),
                        _provider);
  }

  IRS_FORCE_INLINE void SkipFreqs(uint32_t len) {
    static_assert(!Shape.freqs);
    SDB_ASSERT(len != 0);
    if (len == _freq_len.value) {
      block_io::SkipBlock(In());
    }
  }

  IRS_FORCE_INLINE void TakeFreqs(uint32_t len) {
    if constexpr (Shape.freqs) {
      block_io::ReadTail<block_io::kFreqBias>(len, In(), Enc(), _freqs.data);
    } else {
      SkipFreqs(len);
    }
  }

  void ReadLeafDelta(doc_id_t prev) {
    static_assert(Shape.delta);
    if constexpr (Shape.cursor) {
      _cursor.base = prev;
    }
    auto& in = In();
    if constexpr (Shape.reads == LeafReads::Grows) {
      in.Advance();
    }
    if (_left_in_list >= kBlock) [[likely]] {
      block_io::ReadBlockDelta(in, Enc(), _docs, prev);
      _left_in_leaf = kBlock;
      _left_in_list -= kBlock;
      TakeFreqs(kBlock);
      if constexpr (Shape.scored) {
        _score.next_last = NextLast();
      }
    } else {
      const auto tail = _left_in_list;
      block_io::ReadTailDelta(tail, in, Enc(), _docs, prev);
      _left_in_leaf = tail;
      _left_in_list = 0;
      TakeFreqs(tail);
    }
    _last = *(std::end(_docs) - 1);
  }

  template<bool Score = true>
  bool ReadLeafBelow(uint32_t len, doc_id_t min) {
    static_assert(Shape.scored && Shape.freqs && Shape.enc);
    auto& in = In();
    block_io::ReadTailDelta(len, in, _enc.data, _docs, _last);
    _last = *(std::cend(_docs) - 1);
    if (_last < min) {
      block_io::SkipTail(len, in);
      return false;
    }
    block_io::ReadTail<block_io::kFreqBias>(len, in, _enc.data, _freqs.data);
    if constexpr (Score) {
      _score.next_last = NextLast();
      ScoreLeaf(kBlock - len, len);
    }
    return true;
  }

  IRS_FORCE_INLINE doc_id_t NextLast() const noexcept {
    if (_left_in_list == 0 || !_walk.Armed() || _walk.Pending()) {
      return doc_limits::invalid();
    }
    return _walk.Index().Last(_walk.NextBlock(_left_in_list));
  }

  IRS_FORCE_INLINE void ScoreLeaf(uint32_t offset, uint32_t len) {
    static_assert(Shape.scored && Shape.freqs && Shape.enc);
    const auto* const docs = _docs + offset;
    if (len == kBlock) {
      FetchAndScorePostingBlock(*_score.fetcher, _score.score, docs, Scores(),
                                _score.next_last);
      return;
    }
    _provider.freq.value = _freqs.data + offset;
    FetchAndScore(*_score.fetcher, _score.score, docs, Scores() + offset, len);
    _provider.freq.value = _freqs.data;
  }

  template<ScoreMergeType MergeType>
  const doc_id_t* ScoreMasked(const doc_id_t* begin, const doc_id_t* end,
                              doc_id_t min, doc_id_t max,
                              const uint64_t* IRS_RESTRICT mask,
                              score_t* IRS_RESTRICT window) {
    static_assert(Shape.scored && Shape.freqs);
    doc_id_t docs[kBlock];
    uint32_t freqs[kBlock];
    uint32_t offsets[kBlock];
    uint32_t hits = 0;
    const auto* const first = std::cbegin(_docs);
    for (; begin != end && *begin < max; ++begin) {
      const auto offset = static_cast<uint32_t>(*begin - min);
      docs[hits] = *begin;
      freqs[hits] = _freqs.data[begin - first];
      offsets[hits] = offset;
      hits +=
        static_cast<uint32_t>(CheckBit(mask[offset / kBits], offset % kBits));
    }
    if (hits != 0) {
      score_t scores[kBlock];
      _provider.freq.value = freqs;
      FetchAndScore(*_score.fetcher, _score.score, docs, scores, hits);
      _provider.freq.value = _freqs.data;
      for (uint32_t i = 0; i != hits; ++i) {
        irs::Merge<MergeType>(window[offsets[i]], scores[i]);
      }
    }
    return begin;
  }

  doc_id_t Scan(doc_id_t target) {
    const auto* const end = std::cend(_docs);
    SDB_ASSERT(_left_in_leaf != 0 && _last >= target);
    auto left = _left_in_leaf;
    auto doc = *(end - left);
    if (doc < target) [[unlikely]] {
      do {
        doc = *(end - --left);
      } while (doc < target);
    }
    _left_in_leaf = left - 1;
    return _doc = doc;
  }

  template<typename Read>
  IRS_NO_INLINE bool SeekToLeaf(doc_id_t target, Read&& read) {
    static_assert(Shape.cursor);
    if (_walk.Armed() && !_walk.Pending()) {
      if (_left_in_list == 0) [[unlikely]] {
        return false;
      }
      const auto next = _walk.NextBlock(_left_in_list);
      const auto block = _walk.Index().Find(next, target);
      if (block == next) {
        read(_last);
        return true;
      }
      _left_in_list = _walk.MoveTo(block);
      if (_left_in_list == 0) [[unlikely]] {
        return false;
      }
      const auto landing = _walk.Landing();
      In().Seek(landing.doc_ptr);
      read(landing.doc);
      return true;
    }
    const auto span = _last - _cursor.base;
    const bool avoid_seek = target - _last <= span ||
                            target <= _walk.UpperBound() ||
                            target <= doc_limits::min();

    if (avoid_seek) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return false;
      }
      read(_last);
      if (target <= _last) [[likely]] {
        return true;
      }
      if (_left_in_list == 0) [[unlikely]] {
        return false;
      }
    }

    _left_in_list = _walk.Seek(target, _in);
    if (_left_in_list == 0) [[unlikely]] {
      return false;
    }
    const auto landing = _walk.Landing();
    In().Seek(landing.doc_ptr);
    read(landing.doc);
    return target <= _last;
  }

  IRS_FORCE_INLINE bool ReadToDelta(doc_id_t target) {
    return SeekToLeaf(
      target, [this](doc_id_t prev) IRS_FORCE_INLINE { ReadLeafDelta(prev); });
  }

  uint32_t Landings(const doc_id_t* in, uint32_t n, uint32_t* blocks) {
    static_assert(Shape.cursor);
    if (!_walk.Armed() || _walk.Pending() || n < 2 || in[n - 1] <= _last ||
        (in[n - 1] - in[0]) / (n - 1) <= _last - _cursor.base) {
      return 0;
    }
    const auto block = _walk.NextBlock(_left_in_list);
    const auto landings = PlanLandings(
      _walk.Index(),
      {.block = block, .next = block, .upper = _last, .cap = kBlock}, in, n,
      blocks, [&](uint32_t landed) IRS_FORCE_INLINE {
        if constexpr (InputType::CONTIGUOUS) {
          const auto* const at = In().Data() + _walk.BlockStart(landed);
          __builtin_prefetch(at, 0, 2);
          __builtin_prefetch(at + ABSL_CACHELINE_SIZE, 0, 2);
        }
      });
    if (landings != 0 && !InMemory(blocks[landings - 1])) {
      HintBlocks({blocks, landings});
    }
    return landings;
  }

  bool ForeseeSpans(std::span<const DocSpan> spans) {
    BlockRun runs[kMostSpans];
    const auto count = ForeseeRuns(spans, runs);
    if (count == 0) {
      return false;
    }
    HintRuns({runs, count});
    return true;
  }

  uint32_t ForeseeRuns(std::span<const DocSpan> spans, BlockRun* runs) {
    SDB_ASSERT(spans.size() <= kMostSpans);
    if (spans.empty() || spans.back().last <= _last || _left_in_list == 0 ||
        !_walk.Armed()) {
      return 0;
    }
    return ForeseeArmed(spans, runs);
  }

  IRS_NO_INLINE uint32_t ForeseeArmed(std::span<const DocSpan> spans,
                                      BlockRun* runs) {
    const auto& index = _walk.Loaded(In());
    const auto n = index.Size();
    auto block = _walk.NextBlock(_left_in_list);
    if (InMemory(std::min(index.Find(block, spans.back().last), n - 1))) {
      return 0;
    }
    uint32_t count = 0;
    for (const auto& span : spans) {
      if (span.last <= _last) {
        continue;
      }
      const auto first = index.Find(block, std::max(span.first, _last + 1));
      if (first == n) {
        break;
      }
      block = std::min(index.Find(first, span.last), n - 1);
      runs[count++] = {.first = first, .last = block};
    }
    return count;
  }

  IRS_NO_INLINE void HintRuns(std::span<const BlockRun> runs) {
    duckdb::FileRange ranges[kMostSpans];
    for (size_t i = 0; i != runs.size(); ++i) {
      const auto start = _walk.BlockStart(runs[i].first);
      ranges[i] = {.offset = start,
                   .size = _walk.BlockEnd(runs[i].last) - start};
    }
    In().File()->Hint(std::span{ranges, runs.size()});
  }

  bool InMemory(uint32_t block) {
    const auto start = _walk.BlockStart(block);
    return In().File()->IsResident(start, _walk.BlockEnd(block) - start);
  }

  IRS_NO_INLINE void HintBlocks(std::span<const uint32_t> blocks) {
    duckdb::FileRange ranges[kBlock];
    for (size_t i = 0; i != blocks.size(); ++i) {
      const auto start = _walk.BlockStart(blocks[i]);
      ranges[i] = {.offset = start, .size = _walk.BlockEnd(blocks[i]) - start};
    }
    In().File()->Hint(std::span{ranges, blocks.size()});
  }

  template<typename Read>
  IRS_FORCE_INLINE void LandOn(uint32_t block, Read&& read) {
    static_assert(Shape.cursor);
    if (block == _walk.NextBlock(_left_in_list)) {
      read(_last);
      return;
    }
    _left_in_list = _walk.MoveTo(block);
    const auto landing = _walk.Landing();
    In().Seek(landing.doc_ptr);
    read(landing.doc);
  }

  struct FillRead {
    block_io::FillLeaf leaf;
    const UnalignedWord* bitset;
    uint32_t len;
  };

  IRS_FORCE_INLINE FillRead ReadLeafFill(doc_id_t prev) {
    static_assert(Shape.cursor && !Shape.delta);
    auto& in = In();
    in.Advance();
    const auto len = std::min(_left_in_list, kBlock);
    const auto leaf =
      block_io::ReadTailForFill(len, in, Enc(), Holes(), _docs, prev);
    _left_in_list -= len;
    const auto* const bitset = StableBitset(leaf);
    TakeFreqs(len);
    _cursor.base = prev;
    _last = leaf.max;
    return {.leaf = leaf, .bitset = bitset, .len = len};
  }

  [[no_unique_address]] utils::Need<kEnc, EncBuf> _enc;
  [[no_unique_address]] utils::Need<Shape.holes, HoleBuf> _holes;
  [[no_unique_address]] utils::Need<Shape.freqs, FreqBuf> _freqs;
  [[no_unique_address]] utils::Need<Shape.gather, GatherBuf> _gather;
  [[no_unique_address]] utils::Need<Shape.docs, DocsBuf> _docs;
  InputType _in;
  doc_id_t _doc = doc_limits::invalid();
  doc_id_t _last = doc_limits::invalid();
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
  [[no_unique_address]] utils::Need<!Shape.freqs, FreqLen> _freq_len;
  [[no_unique_address]] utils::Need<Shape.scored, LeafScore> _score;
  [[no_unique_address]] utils::Need<
    Shape.scored || Shape.defer,
    std::conditional_t<Shape.defer, FreqProvider<ScoreInputs>, LeafProvider>>
    _provider;
  [[no_unique_address]] utils::Need<Shape.cursor, LeafCursor> _cursor;
  [[no_unique_address]] utils::Need<Shape.reads == LeafReads::Whole, doc_id_t>
    _foreseen{};
  BlockCursor<InputType> _walk;
};

}  // namespace irs::detail
