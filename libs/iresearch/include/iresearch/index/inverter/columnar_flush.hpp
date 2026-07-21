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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/inverter/fields_inverter.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {

// Flush-time scatter scratch: owned by the writer so its block pool and rank
// arrays persist across segment flushes (pooled writers reach steady state
// with zero flush-time allocation).
struct ScatterScratch {
  struct RankedTerm {
    uint64_t key;
    uint32_t id;
  };

  explicit ScatterScratch(IResourceManager& rm)
    : blocks{ManagedTypedAllocator<duckdb::AllocatedData>{rm}},
      cursors{ManagedTypedAllocator<uint32_t>{rm}},
      bounds{ManagedTypedAllocator<uint64_t>{rm}},
      ranked{ManagedTypedAllocator<RankedTerm>{rm}},
      ranked_alt{ManagedTypedAllocator<RankedTerm>{rm}},
      radix_counts{ManagedTypedAllocator<uint32_t>{rm}},
      docs{ManagedTypedAllocator<uint32_t*>{rm}},
      pos{ManagedTypedAllocator<uint32_t*>{rm}},
      offs_start{ManagedTypedAllocator<uint32_t*>{rm}},
      offs_end{ManagedTypedAllocator<uint32_t*>{rm}} {}

  // Scratch reuse pays within one flush (across fields); across flushes the
  // realloc cost is unmeasurable while retention is 20-76MB per pooled
  // writer, so Flush releases everything at its end.
  void Release() noexcept {
    blocks = ManagedVector<duckdb::AllocatedData>{blocks.get_allocator()};
    cursors = ManagedVector<uint32_t>{cursors.get_allocator()};
    bounds = ManagedVector<uint64_t>{bounds.get_allocator()};
    term_starts = ManagedVector<uint32_t>{term_starts.get_allocator()};
    ranked = ManagedVector<RankedTerm>{ranked.get_allocator()};
    ranked_alt = ManagedVector<RankedTerm>{ranked_alt.get_allocator()};
    radix_counts = ManagedVector<uint32_t>{radix_counts.get_allocator()};
    docs = ManagedVector<uint32_t*>{docs.get_allocator()};
    pos = ManagedVector<uint32_t*>{pos.get_allocator()};
    offs_start = ManagedVector<uint32_t*>{offs_start.get_allocator()};
    offs_end = ManagedVector<uint32_t*>{offs_end.get_allocator()};
  }

  ManagedVector<duckdb::AllocatedData> blocks;
  ManagedVector<uint32_t> cursors;
  ManagedVector<uint64_t> bounds;
  ManagedVector<uint32_t> term_starts;
  ManagedVector<RankedTerm> ranked;
  ManagedVector<RankedTerm> ranked_alt;
  ManagedVector<uint32_t> radix_counts;
  ManagedVector<uint32_t*> docs;
  ManagedVector<uint32_t*> pos;
  ManagedVector<uint32_t*> offs_start;
  ManagedVector<uint32_t*> offs_end;
};

// Two-level view over fixed-size scatter blocks: consumers are doc-at-a-time
// iterators, so term-major output never needs to be contiguous.
class ScatterView {
 public:
  static constexpr size_t kBlockShift = 16;
  static constexpr size_t kBlockValues = size_t{1} << kBlockShift;
  static constexpr size_t kBlockMask = kBlockValues - 1;

  ScatterView() = default;
  explicit ScatterView(uint32_t* const* blocks) noexcept : _blocks{blocks} {}

  uint32_t operator[](uint64_t i) const noexcept {
    return _blocks[i >> kBlockShift][i & kBlockMask];
  }

 private:
  uint32_t* const* _blocks = nullptr;
};

// Flush-time scratch, reused across fields of one segment flush. The scatter
// turns the doc-major occurrence log into term-major {doc, pos, offs} regions
// with one stable counting-sort pass; docs within a term's region are
// non-decreasing by construction, freq(term, doc) = run length of equal doc.
class ScatteredField : util::Noncopyable {
 public:
  ScatteredField(InverterMemory& mem, ScatterScratch& scratch) noexcept
    : _mem{&mem}, _s{&scratch} {}

  void Reset(const FieldInverter& field);

  const FieldInverter& Field() const noexcept { return *_field; }
  TokenLayout Layout() const noexcept { return _layout; }
  size_t TermCount() const noexcept {
    return _s->term_starts.empty() ? _s->ranked.size()
                                   : _s->term_starts.size() - 1;
  }

  bytes_view TermAt(size_t rank) const noexcept {
    return _field->Dictionary()
      .Entries()[_s->ranked[RankAt(rank)].id]
      .TermBytes();
  }

  // Region bounds are materialized per rank: [bounds[rank], bounds[rank+1]).
  // Unique-terms fields may hold the same term in several entries (same PK
  // re-inserted within a segment); their ranks sort adjacent with regions
  // already contiguous, so term_starts folds each group into one term.
  uint64_t TermBegin(size_t rank) const noexcept {
    return _s->bounds[RankAt(rank)];
  }
  uint64_t TermEnd(size_t rank) const noexcept {
    return _s->bounds[RankAt(rank + 1)];
  }

  ScatterView Docs() const noexcept { return ScatterView{_s->docs.data()}; }
  ScatterView Pos() const noexcept { return ScatterView{_s->pos.data()}; }
  ScatterView OffsStart() const noexcept {
    return ScatterView{_s->offs_start.data()};
  }
  ScatterView OffsEnd() const noexcept {
    return ScatterView{_s->offs_end.data()};
  }

 private:
  size_t RankAt(size_t rank) const noexcept {
    return _s->term_starts.empty() ? rank : _s->term_starts[rank];
  }

  void BuildHistogram(const LogColumn& term_ids, size_t vocab);
  void FoldDuplicateTerms(std::span<const TermDictionary::Entry> entries);
  uint64_t RankLiveTerms(std::span<const TermDictionary::Entry> entries);
  uint64_t PrefixSums();
  // Fixed-size blocks from a grow-only pool, reused across fields; a column
  // is a flat array of block pointers, written and read via ScatterView's
  // power-of-2 indexing.
  uint32_t** AssignBlocks(ManagedVector<uint32_t*>& col, size_t nblocks,
                          size_t& next);
  void RadixSortByKey();

  template<typename Log>
  void Scatter(const Log& log, uint64_t nocc);

  InverterMemory* _mem;
  ScatterScratch* _s;
  const FieldInverter* _field = nullptr;
  TokenLayout _layout = TokenLayout::Terms;
};

// Positions of one (term, doc) pair over the scattered arrays; values are
// stored absolute, matching what PosIteratorImpl exposes after its delta
// accumulation.
class ColumnarPosIterator final : public PosAttr {
 public:
  void ResetField(ScatterView pos, ScatterView offs_start, ScatterView offs_end,
                  const FreqAttr& freq, bool offs) {
    _pos_col = pos;
    _offs_start = offs_start;
    _offs_end = offs_end;
    _freq = &freq;
    if (offs) {
      _offs.emplace();
    } else {
      _offs.reset();
    }
  }

  void ResetDoc(uint64_t begin) noexcept {
    _begin = begin;
    _i = 0;
    _value = pos_limits::invalid();
    if (_offs) {
      _offs->clear();
    }
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    if (id == irs::Type<OffsAttr>::id() && _offs) {
      return &*_offs;
    }
    return nullptr;
  }

  bool next() final {
    SDB_ASSERT(_freq);
    if (_i == _freq->value) {
      _value = pos_limits::eof();
      return false;
    }
    const auto c = _begin + _i;
    _value = _pos_col[c];
    if (_offs) {
      _offs->start = _offs_start[c];
      _offs->end = _offs_end[c];
    }
    ++_i;
    return true;
  }

 private:
  ScatterView _pos_col;
  ScatterView _offs_start;
  ScatterView _offs_end;
  const FreqAttr* _freq = nullptr;
  std::optional<OffsAttr> _offs;
  uint64_t _begin = 0;
  uint32_t _i = 0;
};

class ColumnarDocIterator final : public DocIterator {
 public:
  void ResetField(const ScatteredField& scattered) {
    _scattered = &scattered;
    _freq.value = 0;
    auto& freq = std::get<AttributePtr<FreqAttr>>(_attrs);
    auto& pos = std::get<AttributePtr<PosAttr>>(_attrs);
    freq = nullptr;
    pos = nullptr;
    _has_pos = false;

    const auto features = scattered.Field().RequestedFeatures();
    if (IndexFeatures::None != (features & IndexFeatures::Freq)) {
      freq = &_freq;
      if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
        _pos.ResetField(
          scattered.Pos(), scattered.OffsStart(), scattered.OffsEnd(), _freq,
          IndexFeatures::None != (features & IndexFeatures::Offs));
        pos = &_pos;
        _has_pos = true;
      }
    }
  }

  void ResetTerm(size_t rank) noexcept {
    _cur = _scattered->TermBegin(rank);
    _end = _scattered->TermEnd(rank);
    _doc = doc_limits::invalid();
    _freq.value = 0;
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t advance() final {
    if (_cur == _end) {
      return _doc = doc_limits::eof();
    }
    const auto docs = _scattered->Docs();
    const doc_id_t doc = docs[_cur];
    const auto run_begin = _cur;
    uint32_t freq = 0;
    do {
      ++_cur;
      ++freq;
    } while (_cur != _end && docs[_cur] == doc);
    _freq.value = freq;
    if (_has_pos) {
      _pos.ResetDoc(run_begin);
    }
    return _doc = doc;
  }

  doc_id_t seek(doc_id_t) final {
    SDB_ASSERT(false);
    return _doc = doc_limits::eof();
  }

  IRS_DOC_ITERATOR_DEFAULTS

  uint32_t GetFreq() const final { return _freq.value; }

 private:
  using Attributes = std::tuple<AttributePtr<FreqAttr>, AttributePtr<PosAttr>>;

  const ScatteredField* _scattered = nullptr;
  FreqAttr _freq;
  ColumnarPosIterator _pos;
  Attributes _attrs;
  uint64_t _cur = 0;
  uint64_t _end = 0;
  bool _has_pos = false;
};

class ColumnarTermIterator final : public TermIterator {
 public:
  void Reset(const ScatteredField& scattered) {
    _scattered = &scattered;
    _rank = 0;
    _doc_itr.ResetField(scattered);
  }

  bytes_view value() const noexcept final {
    SDB_ASSERT(_rank);
    return _scattered->TermAt(_rank - 1);
  }

  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  void read() noexcept final {}

  DocIterator::ptr postings(IndexFeatures /*features*/) const final {
    SDB_ASSERT(_rank);
    _doc_itr.ResetTerm(_rank - 1);
    return memory::to_managed<DocIterator>(_doc_itr);
  }

  bool next() final {
    if (_rank == _scattered->TermCount()) {
      return false;
    }
    ++_rank;
    return true;
  }

  const FieldMeta& Meta() const noexcept { return _scattered->Field().Meta(); }

 private:
  const ScatteredField* _scattered = nullptr;
  mutable ColumnarDocIterator _doc_itr;
  size_t _rank = 0;
};

class ColumnarTermReader final : public BasicTermReader,
                                 private util::Noncopyable {
 public:
  void Reset(const ScatteredField& scattered) {
    _it.Reset(scattered);
    _min = _max = {};
    if (const auto nterms = scattered.TermCount()) {
      _min = scattered.TermAt(0);
      _max = scattered.TermAt(nterms - 1);
    }
  }

  bytes_view(min)() const noexcept final { return _min; }
  bytes_view(max)() const noexcept final { return _max; }
  const FieldMeta& Meta() const noexcept { return _it.Meta(); }
  field_id id() const noexcept final { return Meta().id; }
  FieldProperties properties() const noexcept final { return Meta(); }

  irs::TermIterator::ptr iterator() const noexcept final {
    return memory::to_managed<irs::TermIterator>(_it);
  }

  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

 private:
  mutable ColumnarTermIterator _it;
  bytes_view _min{};
  bytes_view _max{};
};

}  // namespace irs
