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
#include "basics/logger/logger.h"
#include "basics/memory.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/merge.hpp"
#include "iresearch/columnstore/norm_reader.hpp"
#include "iresearch/columnstore/norm_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/norm_column_reader.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

using DocIdMapT = ManagedVector<doc_id_t>;
using FieldMetaMapT = absl::flat_hash_map<std::string_view, const FieldMeta*>;

class NoopDirectory : public Directory {
 public:
  static NoopDirectory& Instance() {
    static NoopDirectory gInstance;
    return gInstance;
  }

  DirectoryAttributes& attributes() noexcept final { return _attrs; }
  IndexOutput::ptr create(std::string_view) noexcept final { return nullptr; }
  bool exists(bool&, std::string_view) const noexcept final { return false; }
  bool length(uint64_t&, std::string_view) const noexcept final {
    return false;
  }
  IndexLock::ptr make_lock(std::string_view) noexcept final { return nullptr; }
  bool mtime(std::time_t&, std::string_view) const noexcept final {
    return false;
  }
  IndexInput::ptr open(std::string_view, IOAdvice) const noexcept final {
    return nullptr;
  }
  bool remove(std::string_view) noexcept final { return false; }
  bool rename(std::string_view, std::string_view) noexcept final {
    return false;
  }
  bool sync(std::span<const std::string_view>) noexcept final { return false; }
  bool visit(const Directory::visitor_f&) const final { return false; }

 private:
  NoopDirectory() : Directory{ResourceManagementOptions::gDefault} {}

  DirectoryAttributes _attrs;
};

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

  template<typename Visitor>
  bool Visit(const Visitor& visitor) const {
    for (auto& entry : _field_iterator_mask) {
      auto& itr = _field_iterators[entry.itr_id];
      if (!visitor(*itr.reader, *itr.remap, *entry.meta)) {
        return false;
      }
    }
    return true;
  }

  const FieldMeta& Meta() const noexcept {
    SDB_ASSERT(_current_meta);
    return *_current_meta;
  }

  std::string_view name() const noexcept final { return Meta().name; }
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
    FieldIterator::ptr itr;
    const SubReader* reader;
    const DocRemap* remap;
  };

  struct TermIteratorImpl {
    size_t itr_id;
    const FieldMeta* meta;
    const TermReader* reader;
  };

  FieldProperties _props;
  std::string_view _current_field;
  const FieldMeta* _current_meta{&FieldMeta::kEmpty};
  std::vector<TermIteratorImpl> _field_iterator_mask;
  std::vector<FieldIteratorImpl> _field_iterators;
  mutable CompoundTermIterator _term_itr;
  ProgressTracker _progress;
};

void CompoundFieldIterator::Add(const SubReader& reader,
                                const DocRemap& remap) {
  auto it = reader.fields();
  SDB_ASSERT(it);
  if (it) [[likely]] {
    _field_iterator_mask.emplace_back(_field_iterators.size(), nullptr,
                                      nullptr);
    _field_iterators.emplace_back(std::move(it), &reader, &remap);
  }
}

bool CompoundFieldIterator::Next() {
  _current_field = {};
  _progress();

  if (Aborted()) {
    _field_iterator_mask.clear();
    _field_iterators.clear();
    return false;
  }

  for (auto& entry : _field_iterator_mask) {
    auto& it = _field_iterators[entry.itr_id].itr;
    SDB_ASSERT(it);
    if (!it->next()) {
      it.reset();
    }
  }

  _field_iterator_mask.clear();

  for (size_t i = 0, count = _field_iterators.size(); i != count; ++i) {
    auto& field_itr = _field_iterators[i];
    if (!field_itr.itr) {
      continue;
    }
    const auto& field_meta = field_itr.itr->value().meta();
    const auto* field_terms = field_itr.reader->field(field_meta.name);
    if (!field_terms) {
      continue;
    }
    const std::string_view field_id = field_meta.name;
    SDB_ASSERT(!IsNull(field_id));
    SDB_ASSERT(_field_iterator_mask.empty() == IsNull(_current_field));
    if (!IsNull(_current_field)) {
      const auto cmp = field_id.compare(_current_field);
      if (cmp > 0) {
        continue;
      }
      if (cmp < 0) {
        _field_iterator_mask.clear();
      }
    }
    _current_field = field_id;
    _current_meta = &field_meta;
    SDB_ASSERT(field_meta.index_features <= Meta().index_features);
    _field_iterator_mask.emplace_back(
      TermIteratorImpl{i, &field_meta, field_terms});
  }

  return !IsNull(_current_field);
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
  for (auto it = reader.fields(); it->next();) {
    const auto& field_meta = it->value().meta();
    const auto [field_meta_it, is_new] =
      field_meta_map.emplace(field_meta.name, &field_meta);
    if (!is_new && (!IsSubsetOf(field_meta.index_features,
                                field_meta_it->second->index_features))) {
      return false;
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
    SDB_ERROR(IRESEARCH,
      "Failed to resize merge_writer::doc_id_map to accommodate element: ",
      reader.docs_count() + doc_limits::min());
    return doc_limits::invalid();
  }
  for (auto docs_itr = reader.docs_iterator(); docs_itr->next(); ++next_id) {
    auto src_doc_id = docs_itr->value();
    SDB_ASSERT(src_doc_id >= doc_limits::min());
    SDB_ASSERT(src_doc_id < reader.docs_count() + doc_limits::min());
    doc_id_map[src_doc_id] = next_id;
  }
  return next_id;
}

const MergeWriter::FlushProgress kProgressNoop = [] { return true; };

field_id MergeNormColumnFromSources(
  columnstore::Writer& cs_writer, std::string_view field_name,
  std::span<const columnstore::MergeSource> sources,
  const NormColumnOptionsProvider* norm_column_options) {
  NormColumnOptions opts{};
  if (norm_column_options && *norm_column_options) {
    opts = (*norm_column_options)(field_name);
  }
  field_id out_id = field_limits::invalid();
  columnstore::NormColumnWriter* norm_writer = nullptr;
  uint64_t merged_row = 0;
  for (const auto& src : sources) {
    const columnstore::NormColumnReader* nc = nullptr;
    if (src.cs_reader != nullptr) {
      if (const auto* source_terms = src.reader->field(field_name);
          source_terms != nullptr &&
          field_limits::valid(source_terms->meta().norm)) {
        nc = src.cs_reader->NormColumn(source_terms->meta().norm);
      }
    }

    if (nc == nullptr) {
      merged_row += src.alive_count;
      if (norm_writer) {
        norm_writer->PadTo(merged_row);
      }
      continue;
    }

    if (!norm_writer) {
      SDB_ASSERT(field_limits::valid(opts.id),
                 "norm_column_options must return a valid id for field ",
                 field_name);
      out_id = opts.id;
      norm_writer = &cs_writer.OpenNormColumn(out_id, opts.row_group_size);
      norm_writer->PadTo(merged_row);
    }

    SDB_ASSERT(nc->RowCount() == src.reader->docs_count());
    const bool has_mask = src.mask && !src.mask->empty();
    for (size_t rg = 0, rg_count = nc->RowGroupCount(); rg < rg_count; ++rg) {
      const auto bytes = nc->RowGroupBytes(rg);
      const auto byte_size = nc->ByteSize(rg);
      const auto rg_first_row = nc->RowGroupFirstRow(rg);
      const auto n = nc->RowGroupRowCount(rg);
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

using MergedNormIdMap = absl::flat_hash_map<std::string_view, field_id>;

MergedNormIdMap MergeNorms(
  columnstore::Writer* cs_writer,
  std::span<const columnstore::MergeSource> sources,
  const FieldMetaMapT& field_meta_map,
  const NormColumnOptionsProvider* norm_column_options) {
  MergedNormIdMap out;
  if (cs_writer == nullptr) {
    return out;
  }
  for (const auto& [name, meta] : field_meta_map) {
    if (!IsSubsetOf(IndexFeatures::Norm, meta->index_features)) {
      continue;
    }
    const auto new_norm_id = MergeNormColumnFromSources(
      *cs_writer, name, sources, norm_column_options);
    if (field_limits::valid(new_norm_id)) {
      out.emplace(name, new_norm_id);
    }
  }
  return out;
}

struct MergedNormProvider final : public NormProvider {
  const columnstore::Reader* reader = nullptr;

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
                 IResourceManager& rm) {
  auto field_writer = meta.codec->get_field_writer(true, rm);
  field_writer->prepare(flush_state);

  while (field_itr.Next()) {
    FieldProperties props;
    props.index_features = field_itr.Meta().index_features;

    if (IsSubsetOf(IndexFeatures::Norm, props.index_features)) {
      const auto it = merged_norm_ids.find(field_itr.Meta().name);
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

// Compute the merged segment's doc_id mapping (identity for fully-live
// sources, compacted for sources with deletes), populate the field-meta
// map, register each source with the field iterator, and set the merged
// segment's doc counts. Returns false if a base_id overflow happens or
// per-source ComputeFieldMeta fails.
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

// Borrow each source's cached columnstore::Reader from its SubReader and
// allocate the output cs Writer. Skipped (all outputs left empty / null)
// when the segment isn't backed by a DatabaseInstance.
void OpenColumnstoreContexts(
  duckdb::DatabaseInstance* db, TrackingDirectory& dir,
  std::string_view segment_name, ManagedVector<MergeWriter::ReaderCtx>& readers,
  std::vector<columnstore::MergeSource>& sources,
  std::unique_ptr<columnstore::Writer>& cs_writer,
  const ColumnOptionsProvider* column_options,
  const NormColumnOptionsProvider* norm_column_options) {
  if (db == nullptr) {
    return;
  }
  sources.reserve(readers.size());
  for (auto& ctx : readers) {
    sources.push_back(columnstore::MergeSource{
      .reader = ctx.reader,
      .cs_reader = ctx.reader->CsReader(),
      .mask = ctx.reader->docs_mask(),
      .alive_count = static_cast<uint64_t>(ctx.reader->live_docs_count()),
    });
  }
  cs_writer = std::make_unique<columnstore::Writer>(
    dir, segment_name, *db, column_options, norm_column_options);
}

}  // namespace

MergeWriter::ReaderCtx::ReaderCtx(const SubReader* reader,
                                  IResourceManager& rm) noexcept
  : reader{reader}, remap{rm} {
  SDB_ASSERT(this->reader);
}

MergeWriter::MergeWriter(IResourceManager& resource_manager) noexcept
  : _dir{NoopDirectory::Instance()}, _readers{{resource_manager}} {}

MergeWriter::operator bool() const noexcept {
  return &_dir != &NoopDirectory::Instance();
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

  std::vector<columnstore::MergeSource> sources;
  std::unique_ptr<columnstore::Writer> cs_writer;
  OpenColumnstoreContexts(_db, track_dir, segment.name, _readers, sources,
                          cs_writer, _column_options, _norm_column_options);

  const auto merged_norm_ids =
    MergeNorms(cs_writer.get(), sources, field_meta_map, _norm_column_options);

  if (!progress_callback()) {
    return false;
  }

  if (cs_writer && !sources.empty()) {
    // TODO(mbkkt) Use progress_callback?
    columnstore::MergeInto(sources, *cs_writer, _column_options);
  }

  if (!progress_callback()) {
    return false;
  }

  std::unique_ptr<columnstore::Reader> cs_reader;
  MergedNormProvider norm_provider;
  if (cs_writer) {
    // TODO(mbkkt) Use progress_callback?
    cs_writer->Commit(segment.docs_count);
    _built_hnsw_graphs = cs_writer->TakeBuiltHnswGraphs();
    if (_db && segment.docs_count != 0) {
      cs_reader =
        std::make_unique<columnstore::Reader>(track_dir, segment.name, *_db);
      norm_provider.reader = cs_reader.get();
    }
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

  if (!WriteFields(state, segment, fields_itr, merged_norm_ids,
                   progress_callback, _readers.get_allocator().Manager())) {
    return false;
  }

  if (!progress_callback()) {
    return false;
  }

  segment.files = track_dir.FlushTracked(segment.byte_size);
  result = true;
  return true;
}

}  // namespace irs
