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
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

using DocIdMapT = ManagedVector<doc_id_t>;
using DocMapF = std::function<doc_id_t(doc_id_t)>;
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

class RemappingDocIterator : public DocIterator {
 public:
  RemappingDocIterator(DocIterator::ptr&& it, const DocMapF& mapper) noexcept
    : _it{std::move(it)}, _mapper{&mapper} {
    SDB_ASSERT(_it);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _it->GetMutable(type);
  }

  doc_id_t advance() final {
    while (true) {
      const auto it_value = _it->advance();
      if (doc_limits::eof(it_value)) {
        return _doc = doc_limits::eof();
      }
      _doc = (*_mapper)(it_value);
      if (doc_limits::eof(_doc)) {
        continue;
      }
      return _doc;
    }
  }

  doc_id_t seek(doc_id_t /*target*/) final {
    SDB_ASSERT(false);
    return _doc = doc_limits::eof();
  }

  uint32_t GetFreq() const final {
    SDB_ASSERT(_it);
    return _it->GetFreq();
  }

 private:
  DocIterator::ptr _it;
  const DocMapF* _mapper;
};

class CompoundDocIterator : public DocIterator {
 public:
  using DocIteratorT =
    std::pair<DocIterator::ptr, std::reference_wrapper<const DocMapF>>;
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
    SDB_ASSERT(_iterators[_current_itr].first);
    return _iterators[_current_itr].first->GetFreq();
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
    auto& it = it_entry.first;
    auto& id_map = it_entry.second.get();

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
      _doc = id_map(it_value);
      if (doc_limits::eof(_doc)) {
        continue;
      }
      return _doc;
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

  void Add(const TermReader& reader, const DocMapF& doc_map);

  Attribute* GetMutable(TypeInfo::type_id) noexcept final {
    SDB_ASSERT(false);
    return nullptr;
  }

  bool next() final;

  DocIterator::ptr postings(IndexFeatures features) const final;

  void read() final {
    for (const auto itr_id : _term_iterator_mask) {
      auto& it = _term_iterators[itr_id].first;
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
    SeekTermIterator::ptr first;
    const DocMapF* second;
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
                               const DocMapF& doc_id_map) {
  auto it = reader.iterator(SeekMode::NORMAL);
  SDB_ASSERT(it);
  if (it) [[likely]] {
    _term_iterator_mask.emplace_back(_term_iterators.size());
    _term_iterators.emplace_back(std::move(it), &doc_id_map);
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
    auto& it = _term_iterators[itr_id].first;
    SDB_ASSERT(it);
    if (!it->next()) {
      it.reset();
    }
  }

  _term_iterator_mask.clear();
  _current_term = {};

  for (size_t i = 0, count = _term_iterators.size(); i != count; ++i) {
    auto& it = _term_iterators[i].first;
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
      SDB_ASSERT(term_itr.first);
      auto it = term_itr.first->postings(Meta().index_features);
      SDB_ASSERT(it);
      if (it) [[likely]] {
        itrs.emplace_back(std::move(it), *term_itr.second);
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

  void Add(const SubReader& reader, const DocMapF& doc_id_map);
  bool Next();
  size_t Size() const noexcept { return _field_iterators.size(); }

  template<typename Visitor>
  bool Visit(const Visitor& visitor) const {
    for (auto& entry : _field_iterator_mask) {
      auto& itr = _field_iterators[entry.itr_id];
      if (!visitor(*itr.reader, *itr.doc_map, *entry.meta)) {
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
    const DocMapF* doc_map;
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
                                const DocMapF& doc_id_map) {
  auto it = reader.fields();
  SDB_ASSERT(it);
  if (it) [[likely]] {
    _field_iterator_mask.emplace_back(_field_iterators.size(), nullptr,
                                      nullptr);
    _field_iterators.emplace_back(std::move(it), &reader, &doc_id_map);
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
    _term_itr.Add(*(segment.reader),
                  *(_field_iterators[segment.itr_id].doc_map));
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
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat(
        "Failed to resize merge_writer::doc_id_map to accommodate element: ",
        reader.docs_count() + doc_limits::min()));
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

// Per-source norm reader handle: which source segment it came from and the
// columnstore::Reader that exposes its NormColumns. The cs_reader is
// non-owning -- it's the cached Reader on SubReader (SegmentReaderImpl), so
// merge doesn't trigger an extra footer parse per source.
struct SourceContext {
  const SubReader* reader;
  const columnstore::Reader* cs_reader;
  const DocMapF* doc_map;
};

// Drives the merged segment's field meta + posting-list write. For fields
// with the Norm feature, allocates a fresh norm column id on `cs_writer`,
// opens its NormColumnWriter, and walks each source's NormColumnReader
// (looked up by the source's per-segment norm id) appending live values.
bool WriteFields(const irs::FlushState& flush_state, const SegmentMeta& meta,
                 CompoundFieldIterator& field_itr,
                 std::span<const SourceContext> sources,
                 columnstore::Writer* cs_writer,
                 const MergeWriter::FlushProgress& progress,
                 IResourceManager& rm) {
  auto field_writer = meta.codec->get_field_writer(true, rm);
  field_writer->prepare(flush_state);

  // Index sources by SubReader* once so the per-field norm merge below does
  // O(1) lookups instead of an N(sources) linear scan per field. Empty when
  // cs_writer is null (db-less test path) -- no entries are inserted.
  absl::flat_hash_map<const SubReader*, const columnstore::Reader*>
    cs_by_reader;
  if (cs_writer != nullptr) {
    cs_by_reader.reserve(sources.size());
    for (const auto& src : sources) {
      cs_by_reader.emplace(src.reader, src.cs_reader);
    }
  }

  while (field_itr.Next()) {
    FieldProperties props;
    props.index_features = field_itr.Meta().index_features;

    if (cs_writer != nullptr &&
        IsSubsetOf(IndexFeatures::Norm, props.index_features)) {
      // Allocate a fresh per-segment norm column id and open the output
      // norm column. Iteration order: source-by-source, then row-by-row in
      // each source -- matches FlushUnsorted's doc_id mapping (each source's
      // live docs occupy a contiguous range in the merged segment), so
      // sequential Append() lands the norm at the right merged row index.
      const field_id new_norm_id = cs_writer->AllocateColumnId();
      auto& nw = cs_writer->OpenNormColumn(new_norm_id,
                                           std::string{field_itr.Meta().name});

      bool any_data = false;
      field_itr.Visit([&](const SubReader& seg, const DocMapF& doc_map,
                          const FieldMeta& field) {
        if (!field_limits::valid(field.norm)) {
          return true;
        }
        auto it = cs_by_reader.find(&seg);
        if (it == cs_by_reader.end()) {
          return true;
        }
        const auto* cs_reader = it->second;
        const auto* nc = cs_reader->NormColumn(field.norm);
        if (nc == nullptr) {
          return true;
        }
        any_data = true;
        for (size_t rg = 0, rg_count = nc->RowGroupCount(); rg < rg_count;
             ++rg) {
          const auto bytes = nc->RowGroupBytes(rg);
          const auto byte_size = nc->ByteSize(rg);
          const auto rg_first_row = nc->RowGroupFirstRow(rg);
          for (uint64_t i = 0, n = nc->RowGroupRowCount(rg); i < n; ++i) {
            const auto src_doc =
              static_cast<doc_id_t>(rg_first_row + i + doc_limits::min());
            if (doc_limits::eof(doc_map(src_doc))) {
              continue;  // deleted
            }
            const auto value = columnstore::ReadNormValue(
              bytes.data() + i * byte_size, byte_size);
            nw.Append(value);
          }
        }
        return true;
      });

      if (any_data) {
        props.norm = new_norm_id;
      }
    }

    field_itr.SetProperties(props);
    field_writer->write(field_itr);
  }

  field_writer->end();
  field_writer.reset();

  return !field_itr.Aborted();
}

}  // namespace

MergeWriter::ReaderCtx::ReaderCtx(const SubReader* reader,
                                  IResourceManager& rm) noexcept
  : reader{reader},
    doc_id_map{{rm}},
    doc_map{[](doc_id_t) noexcept { return doc_limits::eof(); }} {
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

  const size_t size = _readers.size();
  FieldMetaMapT field_meta_map;
  CompoundFieldIterator fields_itr{size, progress_callback};
  IndexFeatures index_features{IndexFeatures::None};

  doc_id_t base_id = doc_limits::min();
  for (auto& reader_ctx : _readers) {
    SDB_ASSERT(reader_ctx.reader);
    auto& reader = *reader_ctx.reader;
    const auto docs_count = reader.docs_count();

    if (reader.live_docs_count() == docs_count) {
      const auto reader_base = base_id - doc_limits::min();
      SDB_ASSERT(static_cast<uint64_t>(base_id) + docs_count <
                 std::numeric_limits<doc_id_t>::max());
      base_id += static_cast<doc_id_t>(docs_count);
      reader_ctx.doc_map = [reader_base](doc_id_t doc) noexcept {
        return reader_base + doc;
      };
    } else {
      auto& doc_id_map = reader_ctx.doc_id_map;
      base_id = ComputeDocIds(doc_id_map, reader, base_id);
      reader_ctx.doc_map = [&doc_id_map](doc_id_t doc) noexcept {
        return doc >= doc_id_map.size() ? doc_limits::eof() : doc_id_map[doc];
      };
    }

    if (!doc_limits::valid(base_id)) {
      return false;
    }
    if (!ComputeFieldMeta(field_meta_map, index_features, reader)) {
      return false;
    }

    fields_itr.Add(reader, reader_ctx.doc_map);
  }

  segment.docs_count = base_id - doc_limits::min();
  segment.live_docs_count = segment.docs_count;

  if (!progress_callback()) {
    return false;
  }

  // Borrow each source's cached columnstore::Reader from its SubReader so
  // merge doesn't trigger an extra footer parse per source. Skipped when
  // the segment isn't backed by a DatabaseInstance (no `<seg>.cs` to read
  // or write).
  std::vector<SourceContext> sources;
  std::vector<const irs::columnstore::Reader*> source_reader_ptrs;
  std::vector<const DocumentMask*> source_masks;
  std::unique_ptr<columnstore::Writer> cs_writer;
  if (_db != nullptr) {
    sources.reserve(_readers.size());
    source_reader_ptrs.reserve(_readers.size());
    source_masks.reserve(_readers.size());
    for (auto& ctx : _readers) {
      const auto* cs_reader = ctx.reader->CsReader();
      source_reader_ptrs.push_back(cs_reader);
      source_masks.push_back(ctx.reader->docs_mask());
      sources.push_back(SourceContext{ctx.reader, cs_reader, &ctx.doc_map});
    }
    cs_writer =
      std::make_unique<columnstore::Writer>(track_dir, segment.name, *_db);
  }

  const FlushState state{.dir = &track_dir,
                         .name = segment.name,
                         .scorer = _scorer,
                         .doc_count = segment.docs_count,
                         .index_features = index_features};

  if (!progress_callback()) {
    return false;
  }

  if (!WriteFields(state, segment, fields_itr, sources, cs_writer.get(),
                   progress_callback, _readers.get_allocator().Manager())) {
    return false;
  }

  if (!progress_callback()) {
    return false;
  }

  // Typed columnstore (PK / geo / wildcard / HNSW / ...): merge column by
  // column via the columnstore merge driver. Norms are handled inline above.
  if (cs_writer && !source_reader_ptrs.empty()) {
    irs::columnstore::MergeInto(source_reader_ptrs, source_masks, *cs_writer);
  }

  if (cs_writer) {
    cs_writer->Commit();
  }

  segment.files = track_dir.FlushTracked(segment.byte_size);
  result = true;
  return true;
}

}  // namespace irs
