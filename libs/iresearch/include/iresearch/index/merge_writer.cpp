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

#include <absl/container/flat_hash_map.h>
#include <absl/strings/internal/resize_uninitialized.h>

#include <vector>

#include "basics/assert.h"
#include "basics/log.h"
#include "basics/memory.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/norm_reader_impl.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

using DocIdMapT = ManagedVector<doc_id_t>;
using FieldMetaMapT = absl::flat_hash_map<field_id, IndexFeatures>;

class ProgressTracker {
 public:
  explicit ProgressTracker(const MergeWriter::FlushProgress& progress,
                           size_t count) noexcept
    : _progress(&progress), _count(count) {
    SDB_ASSERT(progress);
  }

  bool operator()() {
    if (_hits++ >= _count) {
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

class CompoundDocIterator : public DocIterator {
 public:
  struct DocIteratorT {
    DocIterator::ptr it;
    const DocRemap* remap;
  };
  using IteratorsT = std::vector<DocIteratorT>;

  static constexpr auto kProgressStepDocs = size_t{1} << size_t{14};

  explicit CompoundDocIterator(
    const MergeWriter::FlushProgress& progress) noexcept
    : _progress(progress, kProgressStepDocs) {}

  template<typename Func>
  bool Reset(Func&& func) {
    if (!func(_iterators)) {
      return false;
    }
    _doc = doc_limits::invalid();
    _current_itr = 0;
    return true;
  }

  size_t Size() const noexcept { return _iterators.size(); }

  bool Aborted() const noexcept { return !static_cast<bool>(_progress); }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::Type<AttrProviderChangeAttr>::id() == type ? &_attribute_change
                                                           : nullptr;
  }

  doc_id_t advance() final;

  IRS_DOC_ITERATOR_DEFAULTS

  doc_id_t seek(doc_id_t /*target*/) final {
    SDB_ASSERT(false);
    return _doc = doc_limits::eof();
  }

  uint32_t GetFreq() const final {
    SDB_ASSERT(_current_itr < _iterators.size());
    SDB_ASSERT(_iterators[_current_itr].it);
    return _iterators[_current_itr].it->GetFreq();
  }

 private:
  AttrProviderChangeAttr _attribute_change;
  std::vector<DocIteratorT> _iterators;
  size_t _current_itr{0};
  ProgressTracker _progress;
};

doc_id_t CompoundDocIterator::advance() {
  _progress();

  if (Aborted()) {
    _iterators.clear();
    return _doc = doc_limits::eof();
  }

  for (bool notify = !doc_limits::valid(_doc); _current_itr < _iterators.size();
       notify = true, ++_current_itr) {
    auto& it_entry = _iterators[_current_itr];
    auto& it = it_entry.it;
    const auto& remap = *it_entry.remap;

    if (!it) {
      continue;
    }

    if (notify) {
      _attribute_change(*it);
    }

    while (true) {
      auto it_value = it->advance();
      if (doc_limits::eof(it_value)) {
        break;
      }
      if (remap.IsMasked(it_value)) {
        continue;
      }
      return _doc = remap.Remap(it_value);
    }
    it.reset();
  }

  return _doc = doc_limits::eof();
}

class CompoundTermIterator : public TermIterator {
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

  DocIterator::ptr postings(IndexFeatures features) const final;

  void read() final {
    for (const auto itr_id : _term_iterator_mask) {
      auto& it = _term_iterators[itr_id].it;
      SDB_ASSERT(it);
      it->read();
    }
  }

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
  };

  bytes_view _current_term;
  const FieldMeta* _meta{};
  std::vector<size_t> _term_iterator_mask;
  std::vector<TermIteratorImpl> _term_iterators;
  mutable bstring _min_term;
  mutable bstring _max_term;
  mutable CompoundDocIterator _doc_itr;
  mutable bool _has_min_term{false};
  ProgressTracker _progress;
};

void CompoundTermIterator::Add(const TermReader& reader,
                               const DocRemap& remap) {
  auto it = reader.iterator(SeekMode::NORMAL);
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

DocIterator::ptr CompoundTermIterator::postings(
  IndexFeatures /*features*/) const {
  auto add_iterators = [this](CompoundDocIterator::IteratorsT& itrs) {
    itrs.clear();
    itrs.reserve(_term_iterator_mask.size());
    for (auto& itr_id : _term_iterator_mask) {
      auto& term_itr = _term_iterators[itr_id];
      SDB_ASSERT(term_itr.it);
      auto it = term_itr.it->postings(Meta().index_features);
      SDB_ASSERT(it);
      if (it) [[likely]] {
        itrs.emplace_back(std::move(it), term_itr.remap);
      }
    }
    return true;
  };

  _doc_itr.Reset(add_iterators);
  return memory::to_managed<DocIterator>(_doc_itr);
}

class CompoundFieldIterator final : public BasicTermReader {
 public:
  static constexpr const size_t kProgressStepFields = size_t{1};

  explicit CompoundFieldIterator(size_t size,
                                 const MergeWriter::FlushProgress& progress)
    : _term_itr(progress), _progress(progress, kProgressStepFields) {
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

  field_id id() const noexcept final { return Meta().id; }
  FieldProperties properties() const noexcept final { return _props; }
  bytes_view(min)() const noexcept final { return _term_itr.MinTerm(); }
  bytes_view(max)() const noexcept final { return _term_itr.MaxTerm(); }
  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }
  TermIterator::ptr iterator() const final;

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
    if (!field_terms) {
      ++field_itr.pos;
      continue;
    }
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

TermIterator::ptr CompoundFieldIterator::iterator() const {
  _term_itr.Reset(Meta());
  for (const auto& segment : _field_iterator_mask) {
    _term_itr.Add(*(segment.reader), *(_field_iterators[segment.itr_id].remap));
  }
  return memory::to_managed<TermIterator>(_term_itr);
}

bool ComputeFieldMeta(FieldMetaMapT& field_meta_map,
                      IndexFeatures& index_features, const SubReader& reader) {
  for (auto id : reader.field_ids()) {
    const auto* term_reader = reader.field(id);
    if (!term_reader) {
      continue;
    }
    const auto& field_meta = term_reader->meta();
    const auto [field_meta_it, is_new] =
      field_meta_map.emplace(id, field_meta.index_features);
    if (!is_new) {
      field_meta_it->second &= field_meta.index_features;
    }
    index_features |= field_meta.index_features;
  }
  return true;
}

doc_id_t ComputeDocIds(DocIdMapT& doc_id_map, const SubReader& reader,
                       doc_id_t next_id) noexcept {
  try {
    doc_id_map.resize(reader.docs_count() + doc_limits::min(),
                      doc_limits::eof());
  } catch (...) {
    SDB_ERROR(
      IRESEARCH,
      "Failed to resize merge_writer::doc_id_map to accommodate element: ",
      reader.docs_count() + doc_limits::min());
    return doc_limits::invalid();
  }
  for (auto docs_itr = reader.docs_iterator();
       !doc_limits::eof(docs_itr->advance()); ++next_id) {
    auto src_doc_id = docs_itr->value();
    SDB_ASSERT(src_doc_id >= doc_limits::min());
    SDB_ASSERT(src_doc_id < reader.docs_count() + doc_limits::min());
    doc_id_map[src_doc_id] = next_id;
  }
  return next_id;
}

const MergeWriter::FlushProgress kProgressNoop = [] { return true; };

field_id MergeNormColumnFromSources(ColWriter& col_writer, field_id id,
                                    std::span<const MergeSource> sources,
                                    const IndexFieldOptions* field_options) {
  bool any_source_has_norm = false;
  for (const auto& src : sources) {
    if (!src.col_reader) {
      continue;
    }
    const auto* source_terms = src.reader->field(id);
    if (source_terms && field_limits::valid(source_terms->meta().norm) &&
        src.col_reader->NormColumn(source_terms->meta().norm) != nullptr) {
      any_source_has_norm = true;
      break;
    }
  }
  NormColumnOptions opts{};
  if (any_source_has_norm && field_options) {
    opts = field_options->GetNormColumnOptions(id);
  }
  field_id out_id = field_limits::invalid();
  NormColumnWriter* norm_writer = nullptr;
  uint64_t merged_row = 0;
  for (const auto& src : sources) {
    const NormColumnReader* norm_reader = nullptr;
    if (src.col_reader) {
      if (const auto* source_terms = src.reader->field(id);
          source_terms && field_limits::valid(source_terms->meta().norm)) {
        norm_reader = src.col_reader->NormColumn(source_terms->meta().norm);
      }
    }

    if (!norm_reader) {
      merged_row += src.alive_count;
      if (norm_writer) {
        norm_writer->PadTo(merged_row);
      }
      continue;
    }

    if (!norm_writer) {
      SDB_ENSURE(field_limits::valid(opts.id), sdb::ERROR_INTERNAL,
                 "MergeNormColumnFromSources: GetNormColumnOptions did not "
                 "mint a valid id for field ",
                 id);
      out_id = opts.id;
      norm_writer = &col_writer.OpenNormColumn(out_id, opts.row_group_size);
      norm_writer->PadTo(merged_row);
    }

    SDB_ASSERT(norm_reader->RowCount() == src.reader->docs_count());
    const bool has_mask = src.mask && !src.mask->empty();
    for (size_t rg = 0, rg_count = norm_reader->RowGroupCount(); rg < rg_count;
         ++rg) {
      const auto bytes = norm_reader->RowGroupBytes(rg);
      const auto byte_size = norm_reader->ByteSize(rg);
      const auto rg_first_row = norm_reader->RowGroupFirstRow(rg);
      const auto n = norm_reader->RowGroupRowCount(rg);
      if (!has_mask) {
        norm_writer->AppendBytes(merged_row, bytes.data(), n, byte_size);
        merged_row += n;
        continue;
      }
      size_t run_start = 0;
      auto flush_run = [&](size_t run_end) {
        if (run_end > run_start) {
          const auto run = run_end - run_start;
          norm_writer->AppendBytes(
            merged_row, bytes.data() + run_start * byte_size, run, byte_size);
          merged_row += run;
        }
      };
      for (size_t i = 0; i < n; ++i) {
        const auto src_doc =
          static_cast<doc_id_t>(rg_first_row + i + doc_limits::min());
        if (src.mask->contains(src_doc)) {
          flush_run(i);
          run_start = i + 1;
        }
      }
      flush_run(n);
    }
  }
  return out_id;
}

using MergedNormIdMap = absl::flat_hash_map<field_id, field_id>;

MergedNormIdMap MergeNorms(ColWriter& col_writer,
                           std::span<const MergeSource> sources,
                           const FieldMetaMapT& field_meta_map,
                           const IndexFieldOptions* field_options) {
  MergedNormIdMap out;
  for (const auto& [id, features] : field_meta_map) {
    if (!IsSubsetOf(IndexFeatures::Norm, features)) {
      continue;
    }
    const auto new_norm_id =
      MergeNormColumnFromSources(col_writer, id, sources, field_options);
    if (field_limits::valid(new_norm_id)) {
      out.emplace(id, new_norm_id);
    }
  }
  return out;
}

struct MergedNormProvider final : public NormProvider {
  const ColReader* reader = nullptr;

  NormReader::ptr norms(field_id id) const final {
    if (reader == nullptr) {
      return {};
    }
    const auto* col = reader->NormColumn(id);
    if (col == nullptr) {
      return {};
    }
    return MakePersistedNormReader(*col);
  }
};

bool WriteFields(const irs::FlushState& flush_state, const SegmentMeta& meta,
                 CompoundFieldIterator& field_itr,
                 const MergedNormIdMap& merged_norm_ids,
                 const MergeWriter::FlushProgress& progress,
                 IResourceManager& rm, IdxWriter& idx) {
  auto field_writer = std::make_unique<burst_trie::FieldWriter>(
    meta.codec->get_postings_writer(/*compaction=*/true, rm),
    /*compaction=*/true, rm);
  field_writer->SetIdxWriter(idx);
  field_writer->prepare(flush_state);

  while (field_itr.Next()) {
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

  field_writer->end();
  field_writer.reset();

  return !field_itr.Aborted();
}

bool ComputeDocMappingsAndFieldMeta(
  ManagedVector<MergeWriter::ReaderCtx>& readers, SegmentMeta& segment,
  FieldMetaMapT& field_meta_map, CompoundFieldIterator& fields_itr,
  IndexFeatures& index_features) {
  doc_id_t base_id = doc_limits::min();
  for (auto& reader_ctx : readers) {
    SDB_ASSERT(reader_ctx.reader);
    auto& reader = *reader_ctx.reader;
    const auto docs_count = reader.docs_count();
    if (reader.live_docs_count() == docs_count) {
      SDB_ASSERT(static_cast<uint64_t>(base_id) + docs_count <
                 std::numeric_limits<doc_id_t>::max());
      reader_ctx.remap.base_id = base_id;
      base_id += static_cast<doc_id_t>(docs_count);
    } else {
      reader_ctx.remap.mask = reader.docs_mask();
      base_id = ComputeDocIds(reader_ctx.remap.id_map, reader, base_id);
    }
    if (!doc_limits::valid(base_id)) {
      return false;
    }
    if (!ComputeFieldMeta(field_meta_map, index_features, reader)) {
      return false;
    }
    fields_itr.Add(reader, reader_ctx.remap);
  }
  segment.docs_count = base_id - doc_limits::min();
  segment.live_docs_count = segment.docs_count;
  return true;
}

void OpenColWriter(duckdb::DatabaseInstance& db, TrackingDirectory& dir,
                   std::string_view segment_name,
                   ManagedVector<MergeWriter::ReaderCtx>& readers,
                   std::vector<MergeSource>& sources,
                   std::unique_ptr<ColWriter>& col_writer,
                   const IndexFieldOptions* field_options) {
  sources.reserve(readers.size());
  for (auto& ctx : readers) {
    sources.push_back(MergeSource{
      .reader = ctx.reader,
      .col_reader = ctx.reader->GetColReader(),
      .mask = ctx.reader->docs_mask(),
      .alive_count = static_cast<uint64_t>(ctx.reader->live_docs_count()),
    });
  }
  col_writer = std::make_unique<ColWriter>(dir, segment_name, db);
  col_writer->SetFieldOptions(field_options);
}

}  // namespace

MergeWriter::ReaderCtx::ReaderCtx(const SubReader* reader,
                                  IResourceManager& rm) noexcept
  : reader{reader}, remap{rm} {
  SDB_ASSERT(this->reader);
}

bool MergeWriter::Flush(SegmentMeta& segment,
                        const FlushProgress& progress /*= {}*/) {
  SDB_ASSERT(segment.codec);

  bool result = false;
  Finally segment_invalidator = [&result, &segment]() noexcept {
    if (!result) [[unlikely]] {
      segment.files.clear();
      static_cast<SegmentInfo&>(segment) = SegmentInfo{};
    }
  };

  const auto& progress_callback = progress ? progress : kProgressNoop;
  TrackingDirectory track_dir{_dir};

  FieldMetaMapT field_meta_map;
  CompoundFieldIterator fields_itr{_readers.size(), progress_callback};
  IndexFeatures index_features{IndexFeatures::None};
  if (!ComputeDocMappingsAndFieldMeta(_readers, segment, field_meta_map,
                                      fields_itr, index_features)) {
    return false;
  }

  if (!progress_callback()) {
    return false;
  }

  std::vector<MergeSource> sources;
  std::unique_ptr<ColWriter> col_writer;
  OpenColWriter(_db, track_dir, segment.name, _readers, sources, col_writer,
                _field_options);
  SDB_ASSERT(col_writer);

  const auto merged_norm_ids =
    MergeNorms(*col_writer, sources, field_meta_map, _field_options);

  if (!progress_callback()) {
    return false;
  }

  if (!sources.empty() &&
      !MergeInto(sources, *col_writer, _field_options, progress_callback)) {
    return false;
  }

  if (!progress_callback()) {
    return false;
  }

  std::unique_ptr<ColReader> col_reader;
  MergedNormProvider norm_provider;
  IdxWriter idx{track_dir, segment.name, _db};
  col_writer->Commit(segment.docs_count);
  auto built = col_writer->TakeBuiltHnsw();
  if (!built.empty()) {
    _built_hnsw_graphs.reserve(built.size());
    for (auto& b : built) {
      _built_hnsw_graphs.emplace(b.column_id, b.graph);
      idx.AddHNSW(b.column_id, b.info, std::move(b.graph));
    }
  }
  if (segment.docs_count != 0) {
    col_reader = std::make_unique<ColReader>(track_dir, segment.name, _db);
    norm_provider.reader = col_reader.get();
  }

  if (!progress_callback()) {
    return false;
  }

  const FlushState state{
    .dir = &track_dir,
    .norms = norm_provider.reader != nullptr ? &norm_provider : nullptr,
    .name = segment.name,
    .scorer = _scorer,
    .doc_count = segment.docs_count,
    .index_features = index_features,
  };

  if (segment.docs_count != 0 &&
      !WriteFields(state, segment, fields_itr, merged_norm_ids,
                   progress_callback, _readers.get_allocator().Manager(),
                   idx)) {
    return false;
  }
  idx.Commit();

  if (!progress_callback()) {
    return false;
  }

  segment.files = track_dir.FlushTracked(segment.byte_size);
  if (segment.live_docs_count == 0) {
    return false;
  }
  SDB_ASSERT(!segment.files.empty());
  result = true;
  return true;
}

}  // namespace irs
