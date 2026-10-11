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

#include "iresearch/index/merge/postings_unit.hpp"

#include <absl/algorithm/container.h>

#include <vector>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

class ProgressTracker {
 public:
  explicit ProgressTracker(const MergeProgress& progress, size_t count) noexcept
    : _progress(&progress), _count(count) {
    SDB_ASSERT(progress);
  }

  bool operator()(size_t hits = 1) {
    _hits += hits;
    if (_hits > _count) {
      _hits = 0;
      _valid = (*_progress)();
    }
    return _valid;
  }

  explicit operator bool() const noexcept { return _valid; }

 private:
  const MergeProgress* _progress;
  const size_t _count;
  size_t _hits{0};
  bool _valid{true};
};

class CompoundPostings : public TermPostings {
 public:
  struct PostingsT {
    TermPostings::ptr it;
    const DocRemap* remap;
    size_t source;
  };
  using IteratorsT = std::vector<PostingsT>;

  static constexpr auto kProgressStepDocs = size_t{1} << size_t{14};

  explicit CompoundPostings(const MergeProgress& progress) noexcept
    : _progress(progress, kProgressStepDocs) {}

  template<typename Func>
  void Reset(Func&& func, IndexFeatures features) {
    func(_iterators);
    _current_itr = 0;
    _at = 0;
    _size = 0;
    _has_freq = IndexFeatures::None != (features & IndexFeatures::Freq);
    _has_pos = IndexFeatures::None != (features & IndexFeatures::Pos);
  }

  void Clear() noexcept { _iterators.clear(); }

  bool Aborted() const noexcept { return !static_cast<bool>(_progress); }

  uint32_t NextDocs(doc_id_t* docs, uint32_t* freqs) final;

  void NextPositions(uint32_t* pos, uint32_t* offs_start, uint32_t* offs_len,
                     uint32_t n) final {
    SDB_ASSERT(_current_itr < _iterators.size());
    _iterators[_current_itr].it->NextPositions(pos, offs_start, offs_len, n);
  }

 private:
  uint32_t NextLive(TermPostings& it, const DocRemap& remap, doc_id_t* docs,
                    uint32_t* freqs);

  std::vector<PostingsT> _iterators;
  size_t _current_itr{0};
  ProgressTracker _progress;
  doc_id_t _docs[doc_limits::kBlockSize];
  uint32_t _freqs[doc_limits::kBlockSize];
  uint32_t _at{0};
  uint32_t _size{0};
  bool _has_freq{false};
  bool _has_pos{false};
};

uint32_t CompoundPostings::NextDocs(doc_id_t* docs, uint32_t* freqs) {
  for (; _current_itr < _iterators.size(); ++_current_itr) {
    auto& [it, remap, source] = _iterators[_current_itr];
    uint32_t n = 0;
    if (remap->id_map.empty()) {
      n = it->NextDocs(docs, freqs);
      const auto shift = remap->base_id - doc_limits::min();
      for (uint32_t i = 0; i != n; ++i) {
        docs[i] += shift;
      }
    } else {
      n = NextLive(*it, *remap, docs, freqs);
    }
    if (n != 0) {
      if (!_progress(n)) {
        _iterators.clear();
        return 0;
      }
      return n;
    }
  }
  return 0;
}

uint32_t CompoundPostings::NextLive(TermPostings& it, const DocRemap& remap,
                                    doc_id_t* docs, uint32_t* freqs) {
  while (true) {
    if (_at == _size) {
      _size = it.NextDocs(_docs, _freqs);
      _at = 0;
      if (_size == 0) {
        return 0;
      }
    }
    uint64_t skip = 0;
    for (; _at != _size && remap.IsMasked(_docs[_at]); ++_at) {
      if (_has_pos) {
        skip += _freqs[_at];
      }
    }
    if (skip != 0) {
      it.SkipPositions(skip);
    }
    uint32_t n = 0;
    for (; _at != _size && !remap.IsMasked(_docs[_at]); ++_at, ++n) {
      docs[n] = remap.Remap(_docs[_at]);
      if (_has_freq) {
        freqs[n] = _freqs[_at];
      }
    }
    if (n != 0) {
      return n;
    }
  }
}

class CompoundTermIterator : public TermOnlyIterator {
 public:
  CompoundTermIterator(const CompoundTermIterator&) = delete;
  CompoundTermIterator& operator=(const CompoundTermIterator&) = delete;

  static constexpr const size_t kProgressStepTerms = size_t{1} << 7;

  explicit CompoundTermIterator(const MergeProgress& progress)
    : _doc_itr{progress}, _progress{progress, kProgressStepTerms} {}

  bool Aborted() const {
    return !static_cast<bool>(_progress) || _doc_itr.Aborted();
  }

  void Reset(const FieldMeta& meta) noexcept {
    _current_term = {};
    _meta = &meta;
    _doc_itr.Clear();
    _term_iterator_mask.clear();
    _term_iterators.clear();
    _min_term.clear();
    _max_term.clear();
    _has_min_term = false;
  }

  const FieldMeta& Meta() const noexcept { return *_meta; }

  void Add(const TermReader& reader, const DocRemap& remap);

  Attribute* GetMutable(TypeInfo::type_id) noexcept final {
    SDB_ASSERT(false);
    return nullptr;
  }

  bool next() final;

  TermPostings::ptr postings(IndexFeatures features) const final;

  bytes_view value() const noexcept final {
    if (!_has_min_term) [[unlikely]] {
      _has_min_term = true;
      _min_term = _current_term;
    }
    SDB_ASSERT(_max_term <= _current_term);
    _max_term = _current_term;
    return _current_term;
  }

  bytes_view MinTerm() const noexcept { return _min_term; }
  bytes_view MaxTerm() const noexcept { return _max_term; }

 private:
  struct TermIteratorImpl {
    SeekTermIterator::ptr it;
    const DocRemap* remap;
    mutable TermPostings::ptr postings;
  };

  bytes_view _current_term;
  const FieldMeta* _meta{};
  std::vector<size_t> _term_iterator_mask;
  std::vector<TermIteratorImpl> _term_iterators;
  mutable bstring _min_term;
  mutable bstring _max_term;
  mutable CompoundPostings _doc_itr;
  mutable bool _has_min_term{false};
  ProgressTracker _progress;
};

void CompoundTermIterator::Add(const TermReader& reader,
                               const DocRemap& remap) {
  auto it = reader.iterator();
  SDB_ASSERT(it);
  _term_iterator_mask.emplace_back(_term_iterators.size());
  _term_iterators.emplace_back(std::move(it), &remap);
}

bool CompoundTermIterator::next() {
  _progress();

  if (Aborted()) {
    _term_iterators.clear();
    _term_iterator_mask.clear();
    return false;
  }

  for (const auto itr_id : _term_iterator_mask) {
    auto& it = _term_iterators[itr_id].it;
    SDB_ASSERT(it);
    if (!it->next()) {
      it.reset();
    }
  }

  _term_iterator_mask.clear();
  _current_term = {};

  for (size_t i = 0, count = _term_iterators.size(); i != count; ++i) {
    auto& it = _term_iterators[i].it;
    if (!it) {
      continue;
    }
    const bytes_view value = it->value();
    SDB_ASSERT(!IsNull(value));
    SDB_ASSERT(_term_iterator_mask.empty() == IsNull(_current_term));
    if (!IsNull(_current_term)) {
      const auto cmp = value.compare(_current_term);
      if (cmp > 0) {
        continue;
      }
      if (cmp < 0) {
        _term_iterator_mask.clear();
      }
    }
    _current_term = value;
    _term_iterator_mask.emplace_back(i);
  }

  return !IsNull(_current_term);
}

TermPostings::ptr CompoundTermIterator::postings(
  IndexFeatures /*features*/) const {
  auto add_iterators = [this](CompoundPostings::IteratorsT& itrs) {
    const auto* const empty = TermPostings::empty().get();
    for (auto& entry : itrs) {
      if (entry.it.get() != empty) {
        _term_iterators[entry.source].postings = std::move(entry.it);
      }
    }
    itrs.clear();
    itrs.reserve(_term_iterator_mask.size());
    for (auto& itr_id : _term_iterator_mask) {
      auto& term_itr = _term_iterators[itr_id];
      SDB_ASSERT(term_itr.it);
      auto it = term_itr.it->ReusePostings(Meta().index_features,
                                           std::move(term_itr.postings));
      SDB_ASSERT(it);
      itrs.emplace_back(std::move(it), term_itr.remap, itr_id);
    }
  };

  _doc_itr.Reset(add_iterators, Meta().index_features);
  return memory::to_managed<TermPostings>(_doc_itr);
}

class CompoundFieldIterator final : public BasicTermReader {
 public:
  static constexpr const size_t kProgressStepFields = size_t{1};

  explicit CompoundFieldIterator(size_t size, const MergeProgress& progress)
    : _term_itr(progress), _progress(progress, kProgressStepFields) {
    _field_iterators.reserve(size);
    _field_iterator_mask.reserve(size);
  }

  void Add(const SubReader& reader, const DocRemap& remap);
  bool Next();

  const FieldMeta& Meta() const noexcept {
    SDB_ASSERT(_current_meta);
    return *_current_meta;
  }

  field_id id() const noexcept final { return Meta().id; }
  FieldProperties properties() const noexcept final { return _props; }
  bytes_view min() const noexcept final { return _term_itr.MinTerm(); }
  bytes_view max() const noexcept final { return _term_itr.MaxTerm(); }
  TermOnlyIterator::ptr iterator() const final;

  bool Aborted() const {
    return !static_cast<bool>(_progress) || _term_itr.Aborted();
  }

  void SetProperties(FieldProperties props) noexcept { _props = props; }

 private:
  struct FieldIteratorImpl {
    std::span<const field_id> ids;
    size_t pos;
    const SubReader* reader;
    const DocRemap* remap;
  };

  struct TermIteratorImpl {
    size_t itr_id;
    const TermReader* reader;
  };

  FieldProperties _props;
  field_id _current_id{field_limits::invalid()};
  FieldMeta _synthesized_meta;
  const FieldMeta* _current_meta{&FieldMeta::kEmpty};
  std::vector<TermIteratorImpl> _field_iterator_mask;
  std::vector<FieldIteratorImpl> _field_iterators;
  mutable CompoundTermIterator _term_itr;
  ProgressTracker _progress;
};

void CompoundFieldIterator::Add(const SubReader& reader,
                                const DocRemap& remap) {
  _field_iterators.emplace_back(reader.field_ids(), size_t{0}, &reader, &remap);
}

bool CompoundFieldIterator::Next() {
  _progress();

  if (Aborted()) {
    _field_iterator_mask.clear();
    _field_iterators.clear();
    _current_id = field_limits::invalid();
    return false;
  }

  for (auto& entry : _field_iterator_mask) {
    ++_field_iterators[entry.itr_id].pos;
  }

  _field_iterator_mask.clear();
  _current_id = field_limits::invalid();

  for (size_t i = 0, count = _field_iterators.size(); i != count; ++i) {
    auto& field_itr = _field_iterators[i];
    if (field_itr.pos >= field_itr.ids.size()) {
      continue;
    }
    const auto id = field_itr.ids[field_itr.pos];
    if (field_limits::valid(_current_id) && id > _current_id) {
      continue;
    }
    const auto* field_terms = field_itr.reader->field(id);
    SDB_ASSERT(field_terms);
    if (field_limits::valid(_current_id) && id < _current_id) {
      _field_iterator_mask.clear();
    }
    const auto features = field_terms->meta().index_features;
    _synthesized_meta.index_features =
      _field_iterator_mask.empty()
        ? features
        : _synthesized_meta.index_features & features;
    _current_id = id;
    _field_iterator_mask.emplace_back(TermIteratorImpl{i, field_terms});
  }

  if (!field_limits::valid(_current_id)) {
    _current_meta = &FieldMeta::kEmpty;
    return false;
  }
  _synthesized_meta.id = _current_id;
  _current_meta = &_synthesized_meta;
  return true;
}

TermOnlyIterator::ptr CompoundFieldIterator::iterator() const {
  _term_itr.Reset(Meta());
  for (const auto& segment : _field_iterator_mask) {
    _term_itr.Add(*(segment.reader), *(_field_iterators[segment.itr_id].remap));
  }
  return memory::to_managed<TermOnlyIterator>(_term_itr);
}

bool WriteFields(const irs::FlushState& flush_state,
                 CompoundFieldIterator& field_itr,
                 const MergedNormIdMap& merged_norm_ids, IResourceManager& rm,
                 IdxWriter& idx,
                 std::span<const BasicTermReader* const> extra) {
  auto field_writer =
    std::make_unique<burst_trie::FieldWriter>(/*compaction=*/true, rm);
  field_writer->SetIdxWriter(idx);
  field_writer->prepare(flush_state);

  std::vector<const BasicTermReader*> sorted_extra(extra.begin(), extra.end());
  absl::c_sort(sorted_extra,
               [](const BasicTermReader* lhs, const BasicTermReader* rhs) {
                 return lhs->id() < rhs->id();
               });
  size_t ei = 0;
  const size_t en = sorted_extra.size();

  while (field_itr.Next()) {
    const auto fid = field_itr.Meta().id;
    while (ei < en && sorted_extra[ei]->id() < fid) {
      field_writer->write(*sorted_extra[ei]);
      ++ei;
    }

    if (ei < en && sorted_extra[ei]->id() == fid) {
      field_writer->write(*sorted_extra[ei]);
      ++ei;
      continue;
    }

    if (IsSubsetOf(IndexFeatures::Vec, field_itr.Meta().index_features)) {
      continue;
    }

    FieldProperties props;
    props.index_features = field_itr.Meta().index_features;

    if (IsSubsetOf(IndexFeatures::Norm, props.index_features)) {
      const auto it = merged_norm_ids.find(field_itr.Meta().id);
      if (it != merged_norm_ids.end()) {
        props.norm = it->second;
      }
    }

    field_itr.SetProperties(props);
    field_writer->write(field_itr);
  }

  while (ei < en) {
    field_writer->write(*sorted_extra[ei]);
    ++ei;
  }

  field_writer->end();
  field_writer.reset();

  return !field_itr.Aborted();
}

}  // namespace

struct PostingsMerger::Impl {
  Impl(size_t inputs, const MergeProgress& progress)
    : fields{inputs, progress} {}

  CompoundFieldIterator fields;
};

PostingsMerger::PostingsMerger(size_t inputs, const MergeProgress& progress)
  : _impl{std::make_unique<Impl>(inputs, progress)} {}

PostingsMerger::~PostingsMerger() = default;

void PostingsMerger::Add(const SubReader& reader, const DocRemap& remap) {
  _impl->fields.Add(reader, remap);
}

bool PostingsMerger::Write(const FlushState& flush_state,
                           const MergedNormIdMap& merged_norm_ids,
                           IResourceManager& rm, IdxWriter& idx,
                           std::span<const BasicTermReader* const> extra) {
  return WriteFields(flush_state, _impl->fields, merged_norm_ids, rm, idx,
                     extra);
}

}  // namespace irs
