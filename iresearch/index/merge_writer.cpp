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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "merge_writer.hpp"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/internal/resize_uninitialized.h>

#include <bit>
#include <duckdb/storage/buffer_manager.hpp>
#include <optional>
#include <span>
#include <vector>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/hot_column_reader.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/field_writer.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/piece_fields.hpp"
#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/storage.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

using DocIdMapT = ManagedVector<doc_id_t>;

struct MergedMember {
  IndexFeatures features = IndexFeatures::None;
  bytes_view single;
  bool present = false;
  bool keyed = false;
};

struct MergedField {
  std::optional<DictLayout> layout;
  std::array<MergedMember, kDictMembers> members;
};

using FieldMetaMapT = absl::flat_hash_map<field_id, MergedField>;

struct DictKey {
  field_id id;
  DictMember member;

  auto operator<=>(const DictKey&) const = default;
};

class ProgressTracker {
 public:
  explicit ProgressTracker(const MergeWriter::FlushProgress& progress,
                           size_t count) noexcept
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
  const MergeWriter::FlushProgress* _progress;
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

  explicit CompoundPostings(const MergeWriter::FlushProgress& progress) noexcept
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

  size_t Size() const noexcept { return _iterators.size(); }

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

  explicit CompoundTermIterator(const MergeWriter::FlushProgress& progress)
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
  }

  const FieldMeta& Meta() const noexcept { return *_meta; }

  void Add(const TermReader& reader, const DocRemap& remap);

  Attribute* GetMutable(TypeInfo::type_id) noexcept final {
    SDB_ASSERT(false);
    return nullptr;
  }

  bool next() final;

  TermPostings::ptr postings(IndexFeatures features) const final;

  bytes_view value() const noexcept final { return _current_term; }

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
  mutable CompoundPostings _doc_itr;
  ProgressTracker _progress;
};

void CompoundTermIterator::Add(const TermReader& reader,
                               const DocRemap& remap) {
  auto it = reader.iterator();
  SDB_ASSERT(it);
  if (it) [[likely]] {
    _term_iterator_mask.emplace_back(_term_iterators.size());
    _term_iterators.emplace_back(std::move(it), &remap);
  }
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
      if (it) [[likely]] {
        itrs.emplace_back(std::move(it), term_itr.remap, itr_id);
      }
    }
  };

  _doc_itr.Reset(add_iterators, Meta().index_features);
  return memory::to_managed<TermPostings>(_doc_itr);
}

class CompoundFieldIterator final : public BasicTermReader {
 public:
  static constexpr const size_t kProgressStepFields = size_t{1};

  CompoundFieldIterator(size_t size, const MergeWriter::FlushProgress& progress,
                        const IndexFieldOptions* field_options)
    : _term_itr(progress),
      _progress(progress, kProgressStepFields),
      _field_options{field_options} {
    _field_iterators.reserve(size);
    _field_iterator_mask.reserve(size);
  }

  void Add(const SubReader& reader, const DocRemap& remap);
  bool Next();
  size_t Size() const noexcept { return _field_iterators.size(); }

  const FieldMeta& Meta() const noexcept {
    SDB_ASSERT(_current_meta);
    return *_current_meta;
  }

  DictRef Dict() const noexcept {
    return {.layout = _layout, .member = _current.member};
  }

  field_id id() const noexcept final { return Meta().id; }
  FieldProperties properties() const noexcept final { return _props; }
  void SampleTerms(
    uint64_t bytes,
    absl::FunctionRef<void(std::span<const bytes_view>)> visit) const final;
  TermOnlyIterator::ptr iterator() const final;

  bool Aborted() const {
    return !static_cast<bool>(_progress) || _term_itr.Aborted();
  }

  void SetProperties(FieldProperties props) noexcept { _props = props; }

 private:
  struct FieldIteratorImpl {
    std::span<const field_id> ids;
    size_t pos;
    size_t at;
    const SubReader* reader;
    const DocRemap* remap;
  };

  struct TermIteratorImpl {
    size_t itr_id;
    const TermReader* reader;
  };

  const TermReader* Current(FieldIteratorImpl& it) const;

  FieldProperties _props;
  DictKey _current{.id = field_limits::invalid(), .member = {}};
  DictLayout _layout = DictLayout::String;
  FieldMeta _synthesized_meta;
  const FieldMeta* _current_meta{&FieldMeta::kEmpty};
  std::vector<TermIteratorImpl> _field_iterator_mask;
  std::vector<FieldIteratorImpl> _field_iterators;
  mutable CompoundTermIterator _term_itr;
  ProgressTracker _progress;
  const IndexFieldOptions* _field_options;
};

void CompoundFieldIterator::Add(const SubReader& reader,
                                const DocRemap& remap) {
  _field_iterators.emplace_back(reader.field_ids(), size_t{0}, size_t{0},
                                &reader, &remap);
}

const TermReader* CompoundFieldIterator::Current(FieldIteratorImpl& it) const {
  for (; it.pos < it.ids.size(); ++it.pos, it.at = 0) {
    const auto id = it.ids[it.pos];
    if (_field_options &&
        _field_options->GetFieldKind(id) != FieldKind::Index) {
      continue;
    }
    if (const auto fields = it.reader->fields(id); it.at < fields.size()) {
      return fields[it.at];
    }
  }
  return nullptr;
}

bool CompoundFieldIterator::Next() {
  _progress();

  const DictKey none{.id = field_limits::invalid(), .member = {}};
  if (Aborted()) {
    _field_iterator_mask.clear();
    _field_iterators.clear();
    _current = none;
    return false;
  }

  for (auto& entry : _field_iterator_mask) {
    ++_field_iterators[entry.itr_id].at;
  }

  _field_iterator_mask.clear();
  _current = none;

  for (size_t i = 0, count = _field_iterators.size(); i != count; ++i) {
    auto& field_itr = _field_iterators[i];
    const auto* field_terms = Current(field_itr);
    if (!field_terms) {
      continue;
    }
    const auto dict = field_terms->meta().dict;
    const DictKey key{.id = field_itr.ids[field_itr.pos],
                      .member = dict.member};
    const bool first = !field_limits::valid(_current.id) || key < _current;
    if (!first && key != _current) {
      continue;
    }
    if (first) {
      _field_iterator_mask.clear();
      _layout = dict.layout;
    }
    SDB_ASSERT(dict.layout == _layout);
    const auto features = field_terms->meta().index_features;
    _synthesized_meta.index_features =
      _field_iterator_mask.empty()
        ? features
        : _synthesized_meta.index_features & features;
    key_width =
      _field_iterator_mask.empty() || key_width == field_terms->key_width
        ? field_terms->key_width
        : 0;
    _current = key;
    _field_iterator_mask.emplace_back(TermIteratorImpl{i, field_terms});
  }

  if (!field_limits::valid(_current.id)) {
    _current_meta = &FieldMeta::kEmpty;
    return false;
  }
  _synthesized_meta.id = _current.id;
  _current_meta = &_synthesized_meta;
  return true;
}

void CompoundFieldIterator::SampleTerms(
  uint64_t bytes,
  absl::FunctionRef<void(std::span<const bytes_view>)> visit) const {
  if (_field_iterator_mask.empty()) {
    return;
  }
  const uint64_t share = bytes / _field_iterator_mask.size();
  for (const auto& segment : _field_iterator_mask) {
    segment.reader->SampleTerms(share, visit);
  }
}

TermOnlyIterator::ptr CompoundFieldIterator::iterator() const {
  _term_itr.Reset(Meta());
  for (const auto& segment : _field_iterator_mask) {
    _term_itr.Add(*(segment.reader), *(_field_iterators[segment.itr_id].remap));
  }
  return memory::to_managed<TermOnlyIterator>(_term_itr);
}

bool ComputeFieldMeta(FieldMetaMapT& field_meta_map,
                      IndexFeatures& index_features, const SubReader& reader,
                      const IndexFieldOptions* field_options) {
  for (auto id : reader.field_ids()) {
    if (field_options && field_options->GetFieldKind(id) != FieldKind::Index) {
      continue;
    }
    const auto fields = reader.fields(id);
    if (fields.empty()) {
      continue;
    }
    auto& field = field_meta_map[id];
    const auto layout = fields.front()->meta().dict.layout;
    if (!field.layout) {
      field.layout = layout;
    } else if (*field.layout != layout) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                      ERR_MSG("merge: field ", id,
                              " has different dictionary layouts in its "
                              "segments"));
    }
    for (const auto* term_reader : fields) {
      auto& merged =
        field.members[static_cast<size_t>(term_reader->meta().dict.member)];
      const auto features = term_reader->meta().index_features;
      index_features |= features;
      merged.features = merged.present ? merged.features & features : features;
      merged.keyed = merged.keyed || term_reader->size() != 1 ||
                     (merged.present && merged.single != term_reader->min());
      if (!merged.present) {
        merged.single = term_reader->min();
        merged.present = true;
      }
    }
  }
  return true;
}

uint64_t KeyedDicts(const FieldMetaMapT& field_meta_map) noexcept {
  uint64_t count = 0;
  for (const auto& [id, field] : field_meta_map) {
    count += static_cast<uint64_t>(absl::c_count_if(
      field.members, [](const MergedMember& member) { return member.keyed; }));
  }
  return count;
}

doc_id_t ComputeDocIds(DocIdMapT& doc_id_map, const SubReader& reader,
                       doc_id_t next_id) {
  doc_id_map.resize(reader.docs_count() + doc_limits::min(), doc_limits::eof());
  auto mask = MakeGenericDocsMask(reader);
  uint32_t live[detail::kWindowDocs + doc_limits::kDocsSlack];
  const auto end = LiveEnd(reader);
  for (auto window = doc_limits::min(); window < end;) {
    const auto count = std::min(end - window, detail::kWindowDocs);
    const auto alive = mask.FillLive(window, count, live);
    auto* IRS_RESTRICT out = doc_id_map.data() + window;
    for (uint32_t i = 0; i != alive; ++i) {
      out[live[i]] = next_id + i;
    }
    next_id += alive;
    window += count;
  }
  return next_id;
}

const MergeWriter::FlushProgress kProgressNoop = [] { return true; };

struct NormPiece {
  const MergeSource* source;
  const NormColumnReader* reader;
  size_t region;
  NormStats planned;
};

constexpr size_t kNormMergeChunk = 64 * 1024;

const NormColumnReader* SourceNormColumn(const MergeSource& src, field_id id,
                                         DictRef dict) {
  const auto* terms = src.reader->field(id, dict);
  return terms ? src.reader->NormColumn(terms->meta().norm) : nullptr;
}

void ForLiveNorms(const MergeSource& src, const NormColumnReader& column,
                  size_t region, std::vector<uint32_t>& values, auto&& sink) {
  const auto& r = column.Region(region);
  const bool has_mask = HasRemovals(src.reader->Meta());
  auto mask = MakeGenericDocsMask(*src.reader);
  uint32_t live[detail::kWindowDocs + doc_limits::kDocsSlack];
  for (auto doc = r.first_doc; doc != r.end_doc;) {
    const auto n = std::min<size_t>(r.end_doc - doc, kNormMergeChunk);
    values.resize(n);
    column.Decode(doc, n, values.data());
    if (has_mask) {
      size_t kept = 0;
      for (size_t at = 0; at < n;) {
        const auto count =
          static_cast<doc_id_t>(std::min<size_t>(n - at, detail::kWindowDocs));
        const auto alive =
          mask.FillLive(static_cast<doc_id_t>(doc + at), count, live);
        const auto* from = values.data() + at;
        for (uint32_t i = 0; i != alive; ++i) {
          values[kept++] = from[live[i]];
        }
        at += count;
      }
      values.resize(kept);
    }
    sink(std::span<const uint32_t>{values});
    doc += static_cast<doc_id_t>(n);
  }
}

void MergeNormPiece(const NormPiece& piece, NormColumnWriter& writer,
                    std::vector<uint32_t>& values) {
  if (piece.reader == nullptr) {
    for (auto left = piece.planned.rows; left != 0;) {
      const auto n = std::min<uint64_t>(left, kNormMergeChunk);
      values.assign(n, 0);
      writer.Write(values);
      left -= n;
    }
    return;
  }
  ForLiveNorms(*piece.source, *piece.reader, piece.region, values,
               [&](std::span<const uint32_t> live) { writer.Write(live); });
}

std::unique_ptr<NormColumnReader> ViewMergedNorm(
  field_id out_id, field_id id, DictRef dict,
  std::span<const MergeSource> sources) {
  NormColumnConcat concat;
  std::vector<uint32_t> values;
  for (const auto& src : sources) {
    const auto* column = SourceNormColumn(src, id, dict);
    if (!column) {
      concat.AppendZeros(src.alive_count);
      continue;
    }
    if (!HasRemovals(src.reader->Meta())) {
      for (size_t r = 0; r != column->RegionCount(); ++r) {
        concat.Append(*column, r);
      }
      continue;
    }
    for (size_t r = 0; r != column->RegionCount(); ++r) {
      ForLiveNorms(
        src, *column, r, values,
        [&](std::span<const uint32_t> live) { concat.Append(live); });
    }
  }
  return std::move(concat).Finish(out_id);
}

field_id MergeNormColumnFromSources(NormsWriter& norms, field_id id,
                                    DictRef dict,
                                    std::span<const MergeSource> sources,
                                    const IndexFieldOptions* field_options) {
  std::vector<NormPiece> pieces;
  bool any_source_has_norm = false;
  for (const auto& src : sources) {
    const auto* reader = SourceNormColumn(src, id, dict);
    if (!reader) {
      if (src.alive_count != 0) {
        pieces.push_back(
          {&src, nullptr, 0, NormStats{.rows = src.alive_count, .min = 0}});
      }
      continue;
    }
    SDB_ASSERT(reader->RowCount() == src.reader->docs_count());
    any_source_has_norm = true;
    for (size_t r = 0; r != reader->RegionCount(); ++r) {
      pieces.push_back({&src, reader, r, reader->Stats(r)});
    }
  }
  if (!any_source_has_norm || !field_options) {
    return field_limits::invalid();
  }
  const auto out_id = field_options->GetNormColumnId(id);
  SDB_ENSURE(field_limits::valid(out_id),
             "MergeNormColumnFromSources: GetNormColumnId did not "
             "mint a valid id for field ",
             id);
  auto& writer = norms.Stream(out_id);
  std::vector<uint32_t> values;
  for (size_t i = 0; i != pieces.size();) {
    auto planned = pieces[i].planned;
    const auto layout = PickNormLayout(planned);
    size_t j = i + 1;
    for (; j != pieces.size(); ++j) {
      const auto next = PickNormLayout(pieces[j].planned);
      if (next.bits != layout.bits || next.value != layout.value) {
        break;
      }
      planned.Merge(pieces[j].planned);
    }
    writer.OpenRegion(planned);
    for (; i != j; ++i) {
      MergeNormPiece(pieces[i], writer, values);
    }
    writer.CloseRegion();
  }
  writer.Finalize();
  return out_id;
}

using MergedNormIdMap = absl::flat_hash_map<field_id, field_id>;

MergedNormIdMap MergeNorms(NormsWriter& norms,
                           std::span<const MergeSource> sources,
                           const FieldMetaMapT& field_meta_map,
                           const IndexFieldOptions* field_options) {
  MergedNormIdMap out;
  for (const auto& [id, field] : field_meta_map) {
    const auto& strings =
      field.members[static_cast<size_t>(DictMember::Strings)];
    if (!strings.present ||
        !IsSubsetOf(IndexFeatures::Norm, strings.features)) {
      continue;
    }
    const DictRef dict{.layout = *field.layout, .member = DictMember::Strings};
    const auto new_norm_id =
      MergeNormColumnFromSources(norms, id, dict, sources, field_options);
    if (field_limits::valid(new_norm_id)) {
      out.emplace(id, new_norm_id);
    }
  }
  return out;
}

class MergedNormViews final : public NormProvider {
 public:
  MergedNormViews(std::span<const MergeSource> sources,
                  const FieldMetaMapT& field_meta_map,
                  const MergedNormIdMap& merged_norm_ids)
    : _sources{sources} {
    _fields.reserve(merged_norm_ids.size());
    for (const auto& [id, out_id] : merged_norm_ids) {
      _fields.try_emplace(out_id, id,
                          DictRef{.layout = *field_meta_map.at(id).layout,
                                  .member = DictMember::Strings});
    }
  }

  NormReader::ptr norms(field_id out_id) const final {
    const auto it = _fields.find(out_id);
    if (it == _fields.end()) {
      return {};
    }
    const auto& [id, dict] = it->second;
    return MakeNormReader(ViewMergedNorm(out_id, id, dict, _sources));
  }

 private:
  struct Field {
    field_id id;
    DictRef dict;
  };

  std::span<const MergeSource> _sources;
  containers::FlatHashMap<field_id, Field> _fields;
};

bool WriteFields(FieldSink& field_writer, const irs::FlushState& flush_state,
                 CompoundFieldIterator& field_itr,
                 const MergedNormIdMap& merged_norm_ids,
                 std::span<const BasicTermReader* const> extra,
                 uint64_t keyed_dicts) {
  field_writer.prepare(flush_state, keyed_dicts + extra.size());

  std::vector<const BasicTermReader*> sorted_extra(extra.begin(), extra.end());
  absl::c_sort(sorted_extra,
               [](const BasicTermReader* lhs, const BasicTermReader* rhs) {
                 return lhs->id() < rhs->id();
               });
  const auto extra_key = [](const BasicTermReader* reader) {
    return DictKey{
      .id = reader->id(),
      .member = StringLayout::kStrings.member,
    };
  };
  const auto write_extra = [&](const BasicTermReader& reader) {
    field_writer.write(StringLayout::kStrings, reader);
  };
  size_t ei = 0;
  const size_t en = sorted_extra.size();

  while (field_itr.Next()) {
    const auto dict = field_itr.Dict();
    const DictKey key{.id = field_itr.Meta().id, .member = dict.member};
    while (ei < en && extra_key(sorted_extra[ei]) < key) {
      write_extra(*sorted_extra[ei]);
      ++ei;
    }

    if (ei < en && extra_key(sorted_extra[ei]) == key) {
      SDB_ASSERT(dict.layout == DictLayout::String);
      write_extra(*sorted_extra[ei]);
      ++ei;
      continue;
    }

    if (!IsTermMember(dict.member)) {
      if (auto terms = field_itr.iterator(); terms->next()) {
        SDB_ASSERT(terms->value() == kSlotTerm);
        field_writer.WriteSlot(key.id, dict,
                               *terms->postings(IndexFeatures::None));
      }
      continue;
    }

    FieldProperties props;
    props.index_features = field_itr.Meta().index_features;

    if (IsSubsetOf(IndexFeatures::Norm, props.index_features)) {
      const auto it = merged_norm_ids.find(key.id);
      if (it != merged_norm_ids.end()) {
        props.norm = it->second;
      }
    }

    field_itr.SetProperties(props);
    field_writer.write(dict, field_itr);
  }

  while (ei < en) {
    write_extra(*sorted_extra[ei]);
    ++ei;
  }

  field_writer.end();

  return !field_itr.Aborted();
}

bool ComputeDocMappingsAndFieldMeta(
  ManagedVector<MergeWriter::ReaderCtx>& readers, SegmentMeta& segment,
  FieldMetaMapT& field_meta_map, CompoundFieldIterator& fields_itr,
  IndexFeatures& index_features, const IndexFieldOptions* field_options) {
  doc_id_t base_id = doc_limits::min();
  for (auto& reader_ctx : readers) {
    SDB_ASSERT(reader_ctx.reader);
    auto& reader = *reader_ctx.reader;
    const auto docs_count = reader.docs_count();
    SDB_ENSURE(base_id - doc_limits::min() + reader.live_docs_count() <=
                 doc_limits::kMaxDocs,
               "a merge into one segment of more than ", doc_limits::kMaxDocs,
               " documents");
    reader_ctx.remap.base_id = base_id;
    if (reader.live_docs_count() == docs_count) {
      base_id += static_cast<doc_id_t>(docs_count);
    } else {
      base_id = ComputeDocIds(reader_ctx.remap.id_map, reader, base_id);
    }
    if (!doc_limits::valid(base_id)) {
      return false;
    }
    if (!ComputeFieldMeta(field_meta_map, index_features, reader,
                          field_options)) {
      return false;
    }
    fields_itr.Add(reader, reader_ctx.remap);
  }
  segment.docs_count = base_id - doc_limits::min();
  segment.live_docs_count = segment.docs_count;
  return true;
}

std::vector<MergeSource> MakeSources(
  std::span<const MergeWriter::ReaderCtx> readers) {
  std::vector<MergeSource> sources;
  sources.reserve(readers.size());
  for (const auto& ctx : readers) {
    sources.push_back(MergeSource{
      .reader = ctx.reader,
      .col_reader = ctx.reader->GetColReader(),
      .alive_count = static_cast<uint64_t>(ctx.reader->live_docs_count()),
    });
  }
  return sources;
}

}  // namespace

MergeWriter::ReaderCtx::ReaderCtx(const SubReader* reader,
                                  IResourceManager& rm) noexcept
  : reader{reader}, remap{rm} {
  SDB_ASSERT(this->reader);
}

auto MergeWriter::Flush(SegmentMeta& segment,
                        const FlushProgress& progress /*= {}*/,
                        const AnnBuildEnv* env /*= nullptr*/)
  -> yaclib::Future<std::shared_ptr<const FileTracker>> {
  bool result = false;
  auto tracker =
    std::make_shared<FileTracker>(_fs, _root, segment.name, 0, _released);
  Finally segment_invalidator = [&]() noexcept {
    if (!result) [[unlikely]] {
      tracker->Remove();
      segment.kinds = 0;
      static_cast<SegmentInfo&>(segment) = SegmentInfo{};
    }
  };

  const auto& progress_callback = progress ? progress : kProgressNoop;

  FieldMetaMapT field_meta_map;
  CompoundFieldIterator fields_itr{_readers.size(), progress_callback,
                                   _field_options};
  IndexFeatures index_features{IndexFeatures::None};
  if (!ComputeDocMappingsAndFieldMeta(_readers, segment, field_meta_map,
                                      fields_itr, index_features,
                                      _field_options)) {
    co_return nullptr;
  }

  if (!progress_callback()) {
    co_return nullptr;
  }

  const auto sources = MakeSources(_readers);
  const auto col_writer = std::make_unique<ColWriter>(_db, *tracker);
  col_writer->SetFieldOptions(_field_options);
  col_writer->SetHnswBases(_hnsw_bases);

  IdxWriter idx{*tracker};
  NormsWriter norms_writer{
    [&idx] -> duckdb::WriteStream& { return idx.NormsOut(); }};
  const auto merged_norm_ids =
    MergeNorms(norms_writer, sources, field_meta_map, _field_options);
  idx.SetNorms(std::move(norms_writer).Seal(segment.docs_count));

  if (!progress_callback()) {
    co_return nullptr;
  }

  if (!sources.empty() &&
      !MergeInto(sources, *col_writer, _field_options, progress_callback)) {
    co_return nullptr;
  }

  if (!progress_callback()) {
    co_return nullptr;
  }

  col_writer->SetIdxWriter(idx);
  if (!col_writer->Commit(segment.docs_count, progress_callback)) {
    co_return nullptr;
  }
  if (!co_await col_writer->ComputeAnn(env, progress_callback)) {
    co_return nullptr;
  }
  auto ann_writers = col_writer->TakeAnnWriters();
  auto col_reader = col_writer->TakeReader();
  std::optional<ReadContext> ivf_ctx;
  const auto cluster_readers =
    PrepareIvfClusterReaders(ann_writers, col_reader.get(), ivf_ctx);
  for (const auto* reader : cluster_readers) {
    index_features |= reader->properties().index_features;
  }

  if (!progress_callback()) {
    co_return nullptr;
  }

  const MergedNormViews norms{sources, field_meta_map, merged_norm_ids};
  const FlushState state{
    .tracker = *tracker,
    .norms = &norms,
    .scorer = _scorer,
    .doc_count = segment.docs_count,
    .index_features = index_features,
  };
  PostingsInputs postings;
  if (segment.docs_count != 0) {
    auto field_writer =
      std::make_unique<term_dict::FieldWriter>(/*compaction=*/true);
    field_writer->SetIdxWriter(idx);
    if (!WriteFields(*field_writer, state, fields_itr, merged_norm_ids,
                     cluster_readers, KeyedDicts(field_meta_map))) {
      co_return nullptr;
    }
    postings = field_writer->TakePostings();
  }

  for (const auto& w : ann_writers) {
    if (w) {
      w->Flush();
    }
  }

  _inputs = {
    .col = std::move(col_reader),
    .idx = idx.Commit(_hnsw_bases),
    .postings = std::move(postings),
  };

  if (!progress_callback()) {
    co_return nullptr;
  }

  if (segment.live_docs_count == 0) {
    co_return nullptr;
  }
  segment.byte_size = tracker->Bytes();
  segment.kinds = tracker->Kinds();
  SDB_ASSERT(segment.kinds != 0);
  result = true;
  co_return tracker;
}

std::shared_ptr<const SegmentReaderImpl> MergeWriter::FlushPiece(
  SegmentMeta& segment, uint64_t id) {
  FileTracker tracker{_fs, _root, segment.name};
  const absl::Cleanup remove = [&] { tracker.Remove(); };
  FieldMetaMapT field_meta_map;
  CompoundFieldIterator fields_itr{_readers.size(), kProgressNoop,
                                   _field_options};
  IndexFeatures index_features{IndexFeatures::None};
  if (!ComputeDocMappingsAndFieldMeta(_readers, segment, field_meta_map,
                                      fields_itr, index_features,
                                      _field_options) ||
      segment.docs_count == 0) {
    return nullptr;
  }

  const auto sources = MakeSources(_readers);
  BufferedNorms buffered_norms{
    duckdb::BufferManager::GetBufferManager(_db).GetBufferAllocator()};
  const auto merged_norm_ids = MergeNorms(buffered_norms.Writer(), sources,
                                          field_meta_map, _field_options);
  auto norms = std::move(buffered_norms).Seal(segment.docs_count);
  auto columns = ConcatInMemory(_db, sources, _field_options);

  auto fields = std::make_shared<PieceFields>();
  PieceFieldWriter field_writer{*fields, /*compaction=*/true};
  const FlushState state{
    .tracker = tracker,
    .doc_count = segment.docs_count,
    .index_features = index_features,
  };
  if (!WriteFields(field_writer, state, fields_itr, merged_norm_ids, {},
                   KeyedDicts(field_meta_map))) {
    return nullptr;
  }

  segment.byte_size =
    tracker.Bytes() + (norms.file ? norms.file->GetSize() : 0);
  for (const auto& column : columns) {
    segment.byte_size += utils::downCast<HotColumnReader>(*column).Bytes();
  }
  segment.byte_size += fields->Bytes();
  auto postings = field_writer.TakePostings();
  fields->Open(
    ReaderState{
      .db = _db,
      .fs = _fs,
      .root = _root,
      .meta = segment,
      .postings = postings.doc ? &postings : nullptr,
    },
    field_writer.Features());
  return SegmentReaderImpl::OpenPiece(
    segment, id, std::make_unique<ColReader>(_db, std::move(columns)),
    std::make_unique<IdxReader>(std::move(norms)), std::move(fields));
}

}  // namespace irs
