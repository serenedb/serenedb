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

#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "iresearch/index/index_features.hpp"
#include "iresearch/utils/string.hpp"

#if defined(SDB_DEV) && !defined(__clang__)
#include <ranges>
#endif

#include <absl/container/flat_hash_map.h>
#include <absl/strings/internal/resize_uninitialized.h>

#include "basics/logger/logger.h"
#include "basics/memory.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/buffered_column.hpp"
#include "iresearch/index/buffered_column_iterator.hpp"
#include "iresearch/index/comparer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/heap_iterator.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// mapping of old doc_id to new doc_id (reader doc_ids are sequential 0 based)
// masked doc_ids have value of MASKED_DOC_ID
using DocIdMapT = ManagedVector<doc_id_t>;

// document mapping function
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
  const size_t _count;  // call progress callback each `count_` hits
  size_t _hits{0};      // current number of hits
  bool _valid{true};
};

class RemappingDocIterator : public DocIterator {
 public:
  RemappingDocIterator(DocIterator::ptr&& it, const DocMapF& mapper) noexcept
    : _it{std::move(it)}, _mapper{&mapper} {
    SDB_ASSERT(_it);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::Type<irs::DocAttr>::id() == type ? &_doc
                                                 : _it->GetMutable(type);
  }

  doc_id_t value() const noexcept final { return _doc.value; }

  doc_id_t advance() final;

  doc_id_t seek(doc_id_t target) final {
    irs::seek(*this, target);
    return value();
  }

 private:
  DocIterator::ptr _it;
  const DocMapF* _mapper;
  irs::DocAttr _doc;
};

doc_id_t RemappingDocIterator::advance() {
  while (true) {
    const auto it_value = _it->advance();
    if (doc_limits::eof(it_value)) {
      return _doc.value = doc_limits::eof();
    }

    _doc.value = (*_mapper)(it_value);
    if (doc_limits::eof(_doc.value)) {
      continue;  // masked doc_id
    }
    return _doc.value;
  }
}

// Iterator over doc_ids for a term over all readers
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

    _doc.value = doc_limits::invalid();
    _current_itr = 0;

    return true;
  }

  size_t Size() const noexcept { return _iterators.size(); }

  bool Aborted() const noexcept { return !static_cast<bool>(_progress); }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (irs::Type<irs::DocAttr>::id() == type) {
      return &_doc;
    }

    return irs::Type<AttrProviderChangeAttr>::id() == type ? &_attribute_change
                                                           : nullptr;
  }

  doc_id_t value() const noexcept final { return _doc.value; }

  doc_id_t advance() final;

  doc_id_t seek(doc_id_t target) final {
    irs::seek(*this, target);
    return value();
  }

 private:
  friend class SortingCompoundDocIterator;

  AttrProviderChangeAttr _attribute_change;
  std::vector<DocIteratorT> _iterators;
  size_t _current_itr{0};
  ProgressTracker _progress;
  DocAttr _doc;
};

doc_id_t CompoundDocIterator::advance() {
  _progress();

  if (Aborted()) {
    _iterators.clear();
    return _doc.value = doc_limits::eof();
  }

  for (bool notify = !doc_limits::valid(_doc.value);
       _current_itr < _iterators.size(); notify = true, ++_current_itr) {
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

      _doc.value = id_map(it_value);
      if (doc_limits::eof(_doc.value)) {
        continue;  // masked doc_id
      }
      return _doc.value;
    }

    it.reset();
  }

  return _doc.value = doc_limits::eof();
}

// Iterator over sorted doc_ids for a term over all readers
class SortingCompoundDocIterator : public DocIterator {
 public:
  explicit SortingCompoundDocIterator(CompoundDocIterator& doc_it) noexcept
    : _doc_it{&doc_it} {}

  template<typename Func>
  bool Reset(Func&& func) {
    if (!_doc_it->Reset(std::forward<Func>(func))) {
      return false;
    }

    _merge_it.Reset(_doc_it->_iterators);
    _lead = nullptr;

    return true;
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _doc_it->GetMutable(type);
  }

  doc_id_t value() const noexcept final { return _doc_it->value(); }

  doc_id_t advance() final;

  doc_id_t seek(doc_id_t target) final {
    irs::seek(*this, target);
    return value();
  }

 private:
  class Context {
   public:
    using Value = CompoundDocIterator::DocIteratorT;

    // advance
    bool operator()(const Value& value) const {
      const auto& map = value.second.get();
      while (value.first->next()) {
        if (!doc_limits::eof(map(value.first->value()))) {
          return true;
        }
      }
      return false;
    }

    // compare
    bool operator()(const Value& lhs, const Value& rhs) const {
      return lhs.second.get()(lhs.first->value()) <
             rhs.second.get()(rhs.first->value());
    }
  };

  CompoundDocIterator* _doc_it;
  ExternalMergeIterator<Context> _merge_it;
  CompoundDocIterator::DocIteratorT* _lead{};
};

doc_id_t SortingCompoundDocIterator::advance() {
  _doc_it->_progress();

  auto& current_id = _doc_it->_doc.value;
  if (_doc_it->Aborted()) {
    _doc_it->_iterators.clear();
    return current_id = doc_limits::eof();
  }

  while (_merge_it.Next()) {
    auto& new_lead = _merge_it.Lead();
    auto& it = new_lead.first;
    auto& doc_map = new_lead.second.get();

    if (&new_lead != _lead) {
      // update attributes
      _doc_it->_attribute_change(*it);
      _lead = &new_lead;
    }

    current_id = doc_map(it->value());
    if (!doc_limits::eof(current_id)) {
      return current_id;
    }
  }

  return current_id = doc_limits::eof();
}

class DocIteratorContainer {
 public:
  explicit DocIteratorContainer(size_t size) { _itrs.reserve(size); }

  auto begin() { return std::begin(_itrs); }
  auto end() { return std::end(_itrs); }

  template<typename Func>
  bool Reset(Func&& func) {
    return func(_itrs);
  }

 private:
  std::vector<RemappingDocIterator> _itrs;
};

class CompoundColumnIterator final {
 public:
  explicit CompoundColumnIterator(size_t size) {
    _iterators.reserve(size);
    _iterator_mask.reserve(size);
  }

  void Add(const SubReader& reader, const DocMapF& doc_map) {
    auto it = reader.columns();
    SDB_ASSERT(it);

    if (it) [[likely]] {
      _iterator_mask.emplace_back(_iterators.size());
      _iterators.emplace_back(std::move(it), &reader, &doc_map);
    }
  }

  // visit matched iterators
  template<typename Visitor>
  bool Visit(const Visitor& visitor) const {
    for (auto id : _iterator_mask) {
      auto& it = _iterators[id];
      if (!visitor(*it.reader, *it.doc_map, it.it->value())) {
        return false;
      }
    }
    return true;
  }

  const ColumnReader& Value() const {
    if (_current_value) [[likely]] {
      return *_current_value;
    }

    return ColumnIterator::empty()->value();
  }

  bool Next() {
    // advance all used iterators
    for (auto id : _iterator_mask) {
      auto& it = _iterators[id].it;

      if (it) {
        // Skip annonymous columns
        bool exhausted;
        do {
          exhausted = !it->next();
        } while (!exhausted && IsNull(it->value().name()));

        if (exhausted) {
          it = nullptr;
        }
      }
    }

    _iterator_mask.clear();  // reset for next pass

    for (size_t i = 0, size = _iterators.size(); i < size; ++i) {
      auto& it = _iterators[i].it;
      if (!it) {
        continue;  // empty iterator
      }

      const auto& value = it->value();
      const std::string_view key = value.name();
      SDB_ASSERT(!IsNull(key));

      if (!_iterator_mask.empty() && _current_key < key) {
        continue;  // empty field or value too large
      }

      // found a smaller field
      if (_iterator_mask.empty() || key < _current_key) {
        _iterator_mask.clear();
        _current_key = key;
        _current_value = &value;
      }

      SDB_ASSERT(value.name() ==
                 _current_value->name());  // validated by caller
      _iterator_mask.push_back(i);
    }

    if (!_iterator_mask.empty()) {
      return true;
    }

    _current_key = {};

    return false;
  }

 private:
  struct Iterator {
    ColumnIterator::ptr it;
    const SubReader* reader;
    const DocMapF* doc_map;
  };

  static_assert(std::is_nothrow_move_constructible_v<Iterator>);

  const ColumnReader* _current_value{};
  std::string_view _current_key;
  std::vector<size_t> _iterator_mask;  // valid iterators for current step
  std::vector<Iterator> _iterators;    // all segment iterators
};

// Iterator over documents for a term over all readers
class CompoundTermIterator : public TermIterator {
 public:
  CompoundTermIterator(const CompoundTermIterator&) = delete;
  CompoundTermIterator& operator=(const CompoundTermIterator&) = delete;

  static constexpr const size_t kProgressStepTerms = size_t{1} << 7;

  explicit CompoundTermIterator(const MergeWriter::FlushProgress& progress,
                                const Comparer* comparator)
    : _doc_itr{progress},
      _has_comparer{nullptr != comparator},
      _progress{progress, kProgressStepTerms} {}

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
    // no way to merge attributes for the same term spread over multiple
    // iterators would require API change for attributes
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

  bytes_view value() const final {
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
  std::vector<size_t> _term_iterator_mask;  // valid iterators for current term
  std::vector<TermIteratorImpl> _term_iterators;  // all term iterators
  mutable bstring _min_term;
  mutable bstring _max_term;
  mutable CompoundDocIterator _doc_itr;
  mutable SortingCompoundDocIterator _sorting_doc_itr{_doc_itr};
  bool _has_comparer;
  mutable bool _has_min_term{false};
  ProgressTracker _progress;
};

void CompoundTermIterator::Add(const TermReader& reader,
                               const DocMapF& doc_id_map) {
  auto it = reader.iterator(SeekMode::NORMAL);
  SDB_ASSERT(it);

  if (it) [[likely]] {
    // mark as used to trigger next()
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

  // advance all used iterators
  for (const auto itr_id : _term_iterator_mask) {
    auto& it = _term_iterators[itr_id].first;
    SDB_ASSERT(it);
    if (!it->next()) {
      it.reset();
    }
  }

  _term_iterator_mask.clear();
  _current_term = {};

  // TODO(mbkkt) use k way merge
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

  if (_has_comparer) {
    _sorting_doc_itr.Reset(add_iterators);
    // we need to use sorting_doc_itr_ wrapper only
    if (_doc_itr.Size() > 1) {
      return memory::to_managed<DocIterator>(_sorting_doc_itr);
    }
  } else {
    _doc_itr.Reset(add_iterators);
  }
  return memory::to_managed<DocIterator>(_doc_itr);
}

// Iterator over field_ids over all readers
class CompoundFieldIterator final : public BasicTermReader {
 public:
  static constexpr const size_t kProgressStepFields = size_t{1};

  explicit CompoundFieldIterator(size_t size,
                                 const MergeWriter::FlushProgress& progress,
                                 const Comparer* comparator = nullptr)
    : _term_itr(progress, comparator),
      _progress(progress, kProgressStepFields) {
    _field_iterators.reserve(size);
    _field_iterator_mask.reserve(size);
  }

  void Add(const SubReader& reader, const DocMapF& doc_id_map);
  bool Next();
  size_t Size() const noexcept { return _field_iterators.size(); }

  // visit matched iterators
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
  // valid iterators for current field
  std::vector<TermIteratorImpl> _field_iterator_mask;
  std::vector<FieldIteratorImpl> _field_iterators;  // all segment iterators
  mutable CompoundTermIterator _term_itr;
  ProgressTracker _progress;
};

void CompoundFieldIterator::Add(const SubReader& reader,
                                const DocMapF& doc_id_map) {
  auto it = reader.fields();
  SDB_ASSERT(it);

  if (it) [[likely]] {
    // mark as used to trigger next()
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

  // advance all used iterators
  for (auto& entry : _field_iterator_mask) {
    auto& it = _field_iterators[entry.itr_id].itr;
    SDB_ASSERT(it);
    if (!it->next()) {
      it.reset();
    }
  }

  // reset for next pass
  _field_iterator_mask.clear();

  // TODO(mbkkt) use k way merge
  for (size_t i = 0, count = _field_iterators.size(); i != count; ++i) {
    auto& field_itr = _field_iterators[i];

    if (!field_itr.itr) {
      continue;  // empty iterator
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

    // validated by caller
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

// Computes fields_type
bool ComputeFieldMeta(FieldMetaMapT& field_meta_map,
                      IndexFeatures& index_features, const SubReader& reader) {
  for (auto it = reader.fields(); it->next();) {
    const auto& field_meta = it->value().meta();
    const auto [field_meta_it, is_new] =
      field_meta_map.emplace(field_meta.name, &field_meta);

    // validate field_meta equivalence
    if (!is_new && (!IsSubsetOf(field_meta.index_features,
                                field_meta_it->second->index_features))) {
      return false;  // field_meta is not equal, so cannot merge segments
    }

    index_features |= field_meta.index_features;
  }

  return true;
}

// Helper class responsible for writing a data from different sources
// into single columnstore.
class Columnstore {
 public:
  static constexpr size_t kProgressStepColumn = size_t{1} << 13;

  Columnstore(ColumnstoreWriter::ptr&& writer,
              const MergeWriter::FlushProgress& progress)
    : _progress{progress, kProgressStepColumn}, _writer{std::move(writer)} {}

  Columnstore(Directory& dir, const SegmentMeta& meta,
              const MergeWriter::FlushProgress& progress,
              IResourceManager& resource_manager)
    : _progress{progress, kProgressStepColumn} {
    auto writer = meta.codec->get_columnstore_writer(true, resource_manager);
    writer->prepare(dir, meta);

    _writer = std::move(writer);
  }

  // Inserts live values from the specified iterators into a column.
  // Returns column id of the inserted column on success,
  //  field_limits::invalid() in case if no data were inserted,
  //  empty value is case if operation was interrupted.
  template<typename Writer>
  std::optional<field_id> insert(DocIteratorContainer& itrs,
                                 const ColumnInfo& info,
                                 ColumnFinalizer&& finalizer, Writer&& writer);

  // Inserts live values from the specified 'iterator' into a column.
  // Returns column id of the inserted column on success,
  //  field_limits::invalid() in case if no data were inserted,
  //  empty value is case if operation was interrupted.
  template<typename Writer>
  std::optional<field_id> insert(SortingCompoundDocIterator& it,
                                 const ColumnInfo& info,
                                 ColumnFinalizer&& finalizer, Writer&& writer);

  // Returns `true` if anything was actually flushed
  bool Flush(const FlushState& state) { return _writer->commit(state); }

  bool Valid() const noexcept { return static_cast<bool>(_writer); }

 private:
  ProgressTracker _progress;
  ColumnstoreWriter::ptr _writer;
};

template<typename Writer>
std::optional<field_id> Columnstore::insert(DocIteratorContainer& itrs,
                                            const ColumnInfo& info,
                                            ColumnFinalizer&& finalizer,
                                            Writer&& writer) {
  auto next_iterator = [end = std::end(itrs)](auto begin) {
    return std::find_if(begin, end, [](auto& it) { return it.next(); });
  };

  auto begin = next_iterator(std::begin(itrs));

  if (begin == std::end(itrs)) {
    // Empty column
    return std::make_optional(field_limits::invalid());
  }

  auto column = _writer->push_column(info, std::move(finalizer));

  auto write_column = [&column, &writer, this](auto& it) {
    auto* payload = irs::get<irs::PayAttr>(it);

    do {
      if (!_progress()) {
        // Stop was requested
        return false;
      }

      const auto doc = it.value();
      if (payload) {
        writer(column.out, doc, payload->value);
      } else {
        column.out.Prepare(doc);
      }
    } while (it.next());

    return true;
  };

  do {
    if (!write_column(*begin)) {
      // Stop was requested
      return std::nullopt;
    }

    begin = next_iterator(++begin);
  } while (begin != std::end(itrs));

  return std::make_optional(column.id);
}

template<typename Writer>
std::optional<field_id> Columnstore::insert(SortingCompoundDocIterator& it,
                                            const ColumnInfo& info,
                                            ColumnFinalizer&& finalizer,
                                            Writer&& writer) {
  const PayAttr* payload = nullptr;

  auto* callback = irs::get<AttrProviderChangeAttr>(it);

  if (callback) {
    callback->Subscribe([&payload](const AttributeProvider& attrs) {
      payload = irs::get<irs::PayAttr>(attrs);
    });
  } else {
    payload = irs::get<irs::PayAttr>(it);
  }

  if (it.next()) {
    auto column = _writer->push_column(info, std::move(finalizer));

    do {
      if (!_progress()) {
        // Stop was requested
        return std::nullopt;
      }

      const auto doc = it.value();

      if (payload) {
        writer(column.out, doc, payload->value);
      } else {
        column.out.Prepare(doc);
      }
    } while (it.next());

    return std::make_optional(column.id);
  } else {
    // Empty column
    return std::make_optional(field_limits::invalid());
  }
}

struct PrimarySortIteratorAdapter {
  explicit PrimarySortIteratorAdapter(DocIterator::ptr it,
                                      DocIterator::ptr live_docs) noexcept
    : it{std::move(it)},
      doc{irs::get<DocAttr>(*this->it)},
      payload{irs::get<irs::PayAttr>(*this->it)},
      live_docs{std::move(live_docs)},
      live_doc{this->live_docs ? irs::get<DocAttr>(*this->live_docs)
                               : nullptr} {
    SDB_ASSERT(Valid());
  }

  [[nodiscard]] bool Valid() const noexcept {
    return it && doc && payload && (!live_docs || live_doc);
  }

  DocIterator::ptr it;
  const DocAttr* doc;
  const irs::PayAttr* payload;
  DocIterator::ptr live_docs;
  const DocAttr* live_doc;
  doc_id_t min{};
};

class MergeContext {
 public:
  using Value = PrimarySortIteratorAdapter;

  MergeContext(const Comparer& compare) noexcept : _compare{&compare} {}

  // advance
  bool operator()(Value& value) const {
    value.min = value.doc->value + 1;
    return value.it->next();
  }

  // compare
  bool operator()(const Value& lhs, const Value& rhs) const {
    SDB_ASSERT(&lhs != &rhs);
    const bytes_view lhs_value = lhs.payload->value;
    const bytes_view rhs_value = rhs.payload->value;
    const auto r = _compare->Compare(lhs_value, rhs_value);
    return r < 0;
  }

 private:
  const Comparer* _compare;
};

class BufferedValues final : public ColumnReader, DataOutput {
 public:
  BufferedValues(IResourceManager& rm) : _index{{rm}}, _data{{rm}} {}

  void Clear() noexcept {
    _index.clear();
    _data.clear();
    _header.reset();
    _out = nullptr;
    _feature_writer = nullptr;
  }

  void Reserve(size_t count, size_t value_size) {
    _index.reserve(count);
    _data.reserve(count * value_size);
  }

  void PushBack(doc_id_t doc, bytes_view value) {
    _index.emplace_back(doc, _data.size(), value.size());
    _data.append(value);
  }

  void SetID(field_id id) noexcept { _id = id; }

  void UpdateHeader() {
    if (_feature_writer) {
      SDB_ASSERT(!_hnsw_index);
      BytesOutput writer{_header.emplace()};
      _feature_writer->finish(writer);
    }
    if (!_out) {
      return;
    }
    auto* data = _data.data();
    for (auto v : _index) {
      _out->Prepare(v.key);
      _out->WriteBytes(data + v.begin, v.size);
    }
  }

  void WithHNSW(HNSWIndexWriter& hnsw_index) {
    SDB_ASSERT(!_hnsw_index);
    _hnsw_index = &hnsw_index;
  }

  void SetFeatureWriter(FeatureWriter& writer) noexcept {
    SDB_ASSERT(!_feature_writer);
    _feature_writer = &writer;
    SDB_ASSERT(_out == nullptr);
  }

  void operator()(ColumnOutput& out, doc_id_t doc, bytes_view payload) {
    SDB_ASSERT(_out == nullptr || _out == &out);
    _out = &out;
    const auto begin = _data.size();
    // Feature writer or HNSW index must be set, but not both
    if (_feature_writer) {
      SDB_ASSERT(!_hnsw_index);
      _feature_writer->write(*this, payload);
      _index.emplace_back(doc, begin, _data.size() - begin);
    } else if (_hnsw_index) {
      SDB_ASSERT(!payload.empty());
      WriteBytes(payload.data(), payload.size());
      _index.emplace_back(doc, begin, _data.size() - begin);
      _hnsw_index->Add(reinterpret_cast<const float*>(payload.data()), doc);
      // Write vector for HNSW index to read later
    }
  }

  void WriteByte(byte_type b) final { _data += b; }

  void WriteBytes(const byte_type* b, size_t len) final {
    _data.append(b, len);
  }

  field_id id() const noexcept final { return _id; }

  std::string_view name() const noexcept final { return {}; }

  bytes_view payload() const noexcept final {
    return _header.has_value() ? bytes_view{_header->data() + sizeof(uint32_t),
                                            _header->size() - sizeof(uint32_t)}
                               : bytes_view{};
  }

  doc_id_t size() const noexcept final {
    return static_cast<doc_id_t>(_index.size());
  }

  ResettableDocIterator::ptr iterator(ColumnHint hint) const noexcept final {
    // kPrevDoc isn't supported atm
    SDB_ASSERT(ColumnHint::Normal == (hint & ColumnHint::PrevDoc));

    // FIXME(gnusi): can avoid allocation with the help of managed_ptr
    return memory::make_managed<BufferedColumnIterator>(_index, _data);
  }

  const auto& Index() const { return _index; }
  const auto& Data() const { return _data; }

 private:
  BufferedColumn::BufferedValues _index;
  BufferedColumn::Buffer _data;
  field_id _id{field_limits::invalid()};
  std::optional<bstring> _header;
  ColumnOutput* _out{};
  FeatureWriter* _feature_writer{};
  HNSWIndexWriter* _hnsw_index = nullptr;
};

class BufferedColumns final : public ColumnProvider {
 public:
  explicit BufferedColumns(IResourceManager& rm) : _rm{rm} {}

  const irs::ColumnReader* column(field_id field) const noexcept final {
    if (!field_limits::valid(field)) [[unlikely]] {
      return nullptr;
    }

    return Find(field);
  }

  BufferedValues& PushColumn() {
    if (auto* column = Find(field_limits::invalid()); column) {
      column->Clear();
      return *column;
    }

    return _columns.emplace_back(_rm);
  }

  void Clear() noexcept {
    for (auto& column : _columns) {
      column.SetID(field_limits::invalid());
    }
  }

 private:
  BufferedValues* Find(field_id id) const noexcept {
    for (const auto& column : _columns) {
      if (column.id() == id) {
        return const_cast<BufferedValues*>(&column);
      }
    }
    return nullptr;
  }

  // SmallVector seems to be incompatible with
  // our ManagedTypedAllocator
  sdb::containers::SmallVector<BufferedValues, 1> _columns;
  IResourceManager& _rm;
};

template<typename Iterator>
bool WriteColumns(Columnstore& cs, Iterator& columns,
                  const irs::FlushState& state,
                  const ColumnInfoProvider& column_info,
                  CompoundColumnIterator& column_itr,
                  const MergeWriter::FlushProgress& progress) {
  SDB_ASSERT(cs.Valid());
  SDB_ASSERT(progress);
  doc_id_t count{};

  auto add_iterators = [&column_itr, &count](auto& itrs) {
    auto add_iterators = [&itrs, &count](const SubReader& /*segment*/,
                                         const DocMapF& doc_map,
                                         const irs::ColumnReader& column) {
      auto it = column.iterator(ColumnHint::Consolidation);

      if (it && irs::get<DocAttr>(*it)) [[likely]] {
        count += column.size();
        itrs.emplace_back(std::move(it), doc_map);
      } else {
        SDB_ASSERT(false);
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          "Got an invalid iterator during consolidationg of the columnstore, "
          "skipping it");
      }
      return true;
    };

    count = 0;
    itrs.clear();
    return column_itr.Visit(add_iterators);
  };
  auto& buffered_columns = const_cast<BufferedColumns&>(
    sdb::basics::downCast<BufferedColumns>(*state.columns));

  while (column_itr.Next()) {
    buffered_columns.Clear();
    // visit matched columns from merging segments and
    // write all survived values to the new segment
    if (!progress() || !columns.Reset(add_iterators)) {
      return false;  // failed to visit all values
    }

    const std::string_view column_name = column_itr.Value().name();
    auto info = column_info(column_name);
    if (!info.hnsw_info) {
      const auto res =
        cs.insert(columns, info,
                  ColumnFinalizer{
                    [](DataOutput&) {},
                    [column_name] { return column_name; },
                  },
                  [](ColumnOutput& out, doc_id_t doc, bytes_view payload) {
                    out.Prepare(doc);
                    if (!payload.empty()) {
                      out.WriteBytes(payload.data(), payload.size());
                    }
                  });
      if (!res.has_value()) {
        return false;  // failed to insert all values
      }
    } else {
      info.hnsw_info->max_doc = count;
      auto* buffered_column = &buffered_columns.PushColumn();
      buffered_column->Reserve(count, info.hnsw_info->d * sizeof(float));
      auto hnsw_index = std::make_unique<HNSWIndexWriter>(
        *info.hnsw_info,
        [buffered_column] {
          return buffered_column->iterator(ColumnHint::Normal);
        },
        [&](ResettableDocIterator::ptr& it) {
          sdb::basics::downCast<BufferedColumnIterator>(*it).Reset(
            buffered_column->Index(), buffered_column->Data());
        });
      buffered_column->WithHNSW(*hnsw_index.get());
      const auto res =
        cs.insert(columns, info,
                  ColumnFinalizer{
                    [hnsw_index = std::move(hnsw_index)](
                      DataOutput& out) mutable { hnsw_index->Serialize(out); },
                    [column_name] { return column_name; }},
                  *buffered_column);
      if (!res.has_value()) {
        return false;
      }
      buffered_column->SetID(*res);
      buffered_column->UpdateHeader();
    }
  }

  return true;
}

// Write field term data
template<typename Iterator>
bool WriteFields(Columnstore& cs, Iterator& feature_itr,
                 const irs::FlushState& flush_state, const SegmentMeta& meta,
                 const FeatureInfoProvider& column_info,
                 CompoundFieldIterator& field_itr,
                 IndexFeatures scorers_features,
                 const MergeWriter::FlushProgress& progress,
                 IResourceManager& rm) {
  SDB_ASSERT(cs.Valid());

  std::vector<bytes_view> hdrs;
  hdrs.reserve(field_itr.Size());
  doc_id_t count{};  // Total count of documents in consolidating columns

  auto add_iterators = [&](auto& itrs) {
    auto add_iterators = [&](const SubReader& segment, const DocMapF& doc_map,
                             const FieldMeta& field) {
      if (!field_limits::valid(field.norm)) {
        // Field has no feature
        return true;
      }

      auto* reader = segment.column(field.norm);

      // Tail columns can be removed if empty.
      if (reader) {
        auto it = reader->iterator(ColumnHint::Consolidation);
        SDB_ASSERT(it);

        if (it) [[likely]] {
          hdrs.emplace_back(reader->payload());

          if (irs::get<DocAttr>(*it)) [[likely]] {
            count += reader->size();
            itrs.emplace_back(std::move(it), doc_map);
          } else {
            SDB_ASSERT(false);
            SDB_ERROR(
              "xxxxx", sdb::Logger::IRESEARCH,
              "Failed to get document attribute from the iterator, skipping "
              "it");
          }
        }
      }

      return true;
    };

    count = 0;
    hdrs.clear();
    itrs.clear();
    field_itr.Visit(add_iterators);
    return !itrs.empty();
  };

  auto field_writer = meta.codec->get_field_writer(true, rm);
  field_writer->prepare(flush_state);

  // Ensured by the caller
  SDB_ASSERT(flush_state.columns);
  auto& buffered_columns = const_cast<BufferedColumns&>(
    sdb::basics::downCast<BufferedColumns>(*flush_state.columns));

  auto write_feature = [&](FieldProperties& props) {
    // Currently we might have only norms
    static constexpr auto kFeature = IndexFeatures::Norm;

    if (!IsSubsetOf(kFeature, props.index_features) ||
        !feature_itr.Reset(add_iterators)) {
      // No feature or data
      return true;
    }

    if (!progress()) {
      return false;
    }

    const auto [info, factory] = column_info(kFeature);

    std::optional<field_id> res;
    auto feature_writer =
      factory ? (*factory)({hdrs.data(), hdrs.size()}) : FeatureWriter::ptr{};

    auto* buffered_column = IsSubsetOf(kFeature, scorers_features)
                              ? &buffered_columns.PushColumn()
                              : nullptr;

    if (buffered_column) {
      // FIXME(gnusi): We can get better estimation from column
      // headers/stats.
      static constexpr size_t kValueSize = sizeof(uint32_t);
      buffered_column->Reserve(count, kValueSize);
    }

    if (feature_writer) {
      auto write_values = [&, &info = info]<typename T>(T&& value_writer) {
        return cs.insert(feature_itr, info,
                         ColumnFinalizer{
                           [feature_writer = std::move(feature_writer)](
                             DataOutput& out) { feature_writer->finish(out); },
                           [] { return std::string_view{}; },
                         },
                         std::forward<T>(value_writer));
      };

      if (buffered_column) {
        buffered_column->SetFeatureWriter(*feature_writer);
        res = write_values(*buffered_column);
      } else {
        res =
          write_values([writer = feature_writer.get()](
                         ColumnOutput& out, doc_id_t doc, bytes_view payload) {
            out.Prepare(doc);
            writer->write(out, payload);
          });
      }

    } else if (!factory) {
      // Factory has failed to instantiate a writer
      res = cs.insert(
        feature_itr, info,
        ColumnFinalizer{
          [](DataOutput&) {},
          [] { return std::string_view{}; },
        },
        [buffered_column](ColumnOutput& out, doc_id_t doc, bytes_view payload) {
          out.Prepare(doc);
          if (!payload.empty()) {
            out.WriteBytes(payload.data(), payload.size());
            if (buffered_column) {
              buffered_column->PushBack(doc, payload);
            }
          }
        });
    }

    if (!res.has_value()) {
      return false;  // Failed to insert all values
    }

    props.norm = *res;
    if (buffered_column) {
      buffered_column->SetID(*res);
      buffered_column->UpdateHeader();
    }

    SDB_ASSERT(!field_limits::valid(props.norm) ||
               IsSubsetOf(IndexFeatures::Norm, props.index_features));

    return true;
  };

  while (field_itr.Next()) {
    buffered_columns.Clear();

    FieldProperties props;
    props.index_features = field_itr.Meta().index_features;
    if (!write_feature(props)) {
      return false;
    }

    field_itr.SetProperties(props);
    field_writer->write(field_itr);
  }

  field_writer->end();
  field_writer.reset();

  return !field_itr.Aborted();
}

// Computes doc_id_map and docs_count
doc_id_t ComputeDocIds(DocIdMapT& doc_id_map, const SubReader& reader,
                       doc_id_t next_id) noexcept {
  // assume not a lot of space wasted if doc_limits::min() > 0
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
    doc_id_map[src_doc_id] = next_id;  // set to next valid doc_id
  }

  return next_id;
}

#if defined(SDB_DEV) && !defined(__clang__)
void EnsureSorted(const auto& readers) {
  for (const auto& reader : readers) {
    const auto& doc_map = reader.doc_id_map;
    SDB_ASSERT(doc_map.size() >= doc_limits::min());

    auto view = doc_map | std::views::filter([](doc_id_t doc) noexcept {
                  return !doc_limits::eof(doc);
                });

    SDB_ASSERT(std::ranges::is_sorted(view));
  }
}
#endif

const MergeWriter::FlushProgress kProgressNoop = [] { return true; };

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

bool MergeWriter::FlushUnsorted(TrackingDirectory& dir, SegmentMeta& segment,
                                const FlushProgress& progress) {
  SDB_ASSERT(progress);
  SDB_ASSERT(!_comparator);
  SDB_ASSERT(_column_info && *_column_info);
  SDB_ASSERT(_feature_info && *_feature_info);

  const size_t size = _readers.size();

  FieldMetaMapT field_meta_map;
  CompoundFieldIterator fields_itr{size, progress};
  CompoundColumnIterator columns_itr{size};
  IndexFeatures index_features{IndexFeatures::None};

  DocIteratorContainer remapping_itrs{size};

  doc_id_t base_id = doc_limits::min();  // next valid doc_id

  // collect field meta and field term data
  for (auto& reader_ctx : _readers) {
    // ensured by merge_writer::add(...)
    SDB_ASSERT(reader_ctx.reader);

    auto& reader = *reader_ctx.reader;
    const auto docs_count = reader.docs_count();

    if (reader.live_docs_count() == docs_count) {  // segment has no deletes
      const auto reader_base = base_id - doc_limits::min();
      SDB_ASSERT(static_cast<uint64_t>(base_id) + docs_count <
                 std::numeric_limits<doc_id_t>::max());
      base_id += static_cast<doc_id_t>(docs_count);

      reader_ctx.doc_map = [reader_base](doc_id_t doc) noexcept {
        return reader_base + doc;
      };
    } else {  // segment has some deleted docs
      auto& doc_id_map = reader_ctx.doc_id_map;
      base_id = ComputeDocIds(doc_id_map, reader, base_id);

      reader_ctx.doc_map = [&doc_id_map](doc_id_t doc) noexcept {
        return doc >= doc_id_map.size() ? doc_limits::eof() : doc_id_map[doc];
      };
    }

    if (!doc_limits::valid(base_id)) {
      return false;  // failed to compute next doc_id
    }

    if (!ComputeFieldMeta(field_meta_map, index_features, reader)) {
      return false;
    }

    fields_itr.Add(reader, reader_ctx.doc_map);
    columns_itr.Add(reader, reader_ctx.doc_map);
  }

  // total number of doc_ids
  segment.docs_count = base_id - doc_limits::min();
  // all merged documents are live
  segment.live_docs_count = segment.docs_count;

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  // write merged segment data
  Columnstore cs(dir, segment, progress, _readers.get_allocator().Manager());

  if (!cs.Valid()) {
    return false;  // flush failure
  }

  BufferedColumns buffered_columns{_readers.get_allocator().Manager()};

  const FlushState state{.dir = &dir,
                         .columns = &buffered_columns,
                         .name = segment.name,
                         .scorers = _scorers,
                         .doc_count = segment.docs_count,
                         .index_features = index_features};

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  if (!WriteColumns(cs, remapping_itrs, state, *_column_info, columns_itr,
                    progress)) {
    return false;  // flush failure
  }

  if (!progress()) {
    return false;  // progress callback requested termination
  }
  // Write field meta and field term data
  if (!WriteFields(cs, remapping_itrs, state, segment, *_feature_info,
                   fields_itr, _scorers_features, progress,
                   _readers.get_allocator().Manager())) {
    return false;  // Flush failure
  }

  if (!progress()) {
    return false;  // Progress callback requested termination
  }

  segment.column_store = cs.Flush(state);

  return true;
}

bool MergeWriter::FlushSorted(TrackingDirectory& dir, SegmentMeta& segment,
                              const FlushProgress& progress) {
  SDB_ASSERT(progress);
  SDB_ASSERT(_comparator);
  SDB_ASSERT(_column_info && *_column_info);
  SDB_ASSERT(_feature_info && *_feature_info);

  const size_t size = _readers.size();

  FieldMetaMapT field_meta_map;
  CompoundColumnIterator columns_itr{size};
  CompoundFieldIterator fields_itr{size, progress, _comparator};
  IndexFeatures index_features{IndexFeatures::None};

  std::vector<PrimarySortIteratorAdapter> itrs;
  itrs.reserve(size);

  auto emplace_iterator = [&itrs](const SubReader& segment) {
    if (!segment.sort()) {
      // Sort column is not present, give up
      return false;
    }

    auto sort =
      segment.mask(segment.sort()->iterator(irs::ColumnHint::Consolidation));

    if (!sort) [[unlikely]] {
      return false;
    }

    DocIterator::ptr live_docs;
    if (segment.docs_count() != segment.live_docs_count()) {
      live_docs = segment.docs_iterator();
    }

    auto& it = itrs.emplace_back(std::move(sort), std::move(live_docs));

    if (!it.Valid()) [[unlikely]] {
      return false;
    }

    return true;
  };

  segment.docs_count = 0;

  // Init doc map for each reader
  for (auto& reader_ctx : _readers) {
    // Ensured by merge_writer::add(...)
    SDB_ASSERT(reader_ctx.reader);

    auto& reader = *reader_ctx.reader;

    if (reader.docs_count() > doc_limits::eof() - doc_limits::min()) {
      // Can't merge segment holding more than 'doc_limits::eof()-1' docs
      return false;
    }

    if (!emplace_iterator(reader)) {
      // Sort column is not present, give up
      return false;
    }

    if (!ComputeFieldMeta(field_meta_map, index_features, reader)) {
      return false;
    }

    fields_itr.Add(reader, reader_ctx.doc_map);
    columns_itr.Add(reader, reader_ctx.doc_map);

    // Count total number of documents in consolidated segment
    if (!math::SumCheckOverflow(segment.docs_count,
                                static_cast<uint32_t>(reader.live_docs_count()),
                                segment.docs_count)) {
      return false;
    }

    // Prepare doc maps
    auto& doc_id_map = reader_ctx.doc_id_map;

    try {
      doc_id_map.resize(reader.docs_count() + doc_limits::min(),
                        doc_limits::eof());
    } catch (...) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat(
          "Failed to resize merge_writer::doc_id_map to accommodate element: ",
          reader.docs_count() + doc_limits::min()));

      return false;
    }

    reader_ctx.doc_map = [doc_id_map =
                            std::span{doc_id_map}](size_t doc) noexcept {
      SDB_ASSERT(doc_id_map[0] == doc_limits::eof());
      return doc_id_map[doc * static_cast<size_t>(doc < doc_id_map.size())];
    };
  }

  if (segment.docs_count >= doc_limits::eof()) {
    // Can't merge segments holding more than 'doc_limits::eof()-1' docs
    return false;
  }

  if (!progress()) {
    return false;  // Progress callback requested termination
  }

  // Write new sorted column and fill doc maps for each reader
  auto writer = segment.codec->get_columnstore_writer(
    true, _readers.get_allocator().Manager());
  writer->prepare(dir, segment);

  // Get column info for sorted column
  const auto info = (*_column_info)({});
  auto [column_id, column_writer] = writer->push_column(info, {});

  doc_id_t next_id = doc_limits::min();

  auto fill_doc_map = [&](DocIdMapT& doc_id_map, PrimarySortIteratorAdapter& it,
                          doc_id_t max) {
    if (auto min = it.min; min < max) {
      if (it.live_docs) {
        auto& live_docs = *it.live_docs;
        const auto* live_doc = it.live_doc;
        for (live_docs.seek(min); live_doc->value < max; live_docs.next()) {
          doc_id_map[live_doc->value] = next_id++;
          if (!progress()) {
            return false;
          }
        }
      } else {
        do {
          doc_id_map[min] = next_id++;
          ++min;
          if (!progress()) {
            return false;
          }
        } while (min < max);
      }
    }
    return true;
  };

  ExternalMergeIterator<MergeContext> columns_it{*_comparator};

  for (columns_it.Reset(itrs); columns_it.Next();) {
    auto& it = columns_it.Lead();
    SDB_ASSERT(it.Valid());

    const auto max = it.doc->value;
    const auto index = &it - itrs.data();
    auto& doc_id_map = _readers[index].doc_id_map;

    // Fill doc id map
    if (!fill_doc_map(doc_id_map, it, max)) {
      return false;  // Progress callback requested termination
    }
    doc_id_map[max] = next_id;

    // write value into new column if present
    column_writer.Prepare(next_id);
    const auto payload = it.payload->value;
    column_writer.WriteBytes(payload.data(), payload.size());

    ++next_id;

    if (!progress()) {
      return false;  // Progress callback requested termination
    }
  }

  // Handle empty values greater than the last document in sort column
  for (auto it = itrs.begin(); auto& reader : _readers) {
    if (!fill_doc_map(reader.doc_id_map, *it,
                      doc_limits::min() +
                        static_cast<doc_id_t>(reader.reader->docs_count()))) {
      return false;  // progress callback requested termination
    }
    ++it;
  }

#if defined(SDB_DEV) && !defined(__clang__)
  EnsureSorted(readers_);
#endif

  Columnstore cs(std::move(writer), progress);
  CompoundDocIterator doc_it(progress);
  SortingCompoundDocIterator sorting_doc_it(doc_it);

  if (!cs.Valid()) {
    return false;  // Flush failure
  }

  BufferedColumns buffered_columns{_readers.get_allocator().Manager()};

  const FlushState state{.dir = &dir,
                         .columns = &buffered_columns,
                         .name = segment.name,
                         .scorers = _scorers,
                         .doc_count = segment.docs_count,
                         .index_features = index_features};

  if (!progress()) {
    return false;  // Progress callback requested termination
  }

  if (!WriteColumns(cs, sorting_doc_it, state, *_column_info, columns_itr,
                    progress)) {
    return false;  // Flush failure
  }

  if (!progress()) {
    return false;  // Progress callback requested termination
  }

  // Write field meta and field term data
  if (!WriteFields(cs, sorting_doc_it, state, segment, *_feature_info,
                   fields_itr, _scorers_features, progress,
                   _readers.get_allocator().Manager())) {
    return false;  // flush failure
  }

  if (!progress()) {
    return false;  // Progress callback requested termination
  }

  segment.column_store = cs.Flush(state);  // Flush columnstore
  segment.sort = column_id;                // Set sort column identifier
  // All merged documents are live
  segment.live_docs_count = segment.docs_count;

  return true;
}

bool MergeWriter::Flush(SegmentMeta& segment,
                        const FlushProgress& progress /*= {}*/) {
  SDB_ASSERT(segment.codec);  // Must be set outside

  bool result = false;  // Flush result

  Finally segment_invalidator = [&result, &segment]() noexcept {
    if (!result) [[unlikely]] {
      // Invalidate segment
      segment.files.clear();
      segment.column_store = false;
      static_cast<SegmentInfo&>(segment) = SegmentInfo{};
    }
  };

  const auto& progress_callback = progress ? progress : kProgressNoop;

  TrackingDirectory track_dir{_dir};  // Track writer created files

  result = _comparator ? FlushSorted(track_dir, segment, progress_callback)
                       : FlushUnsorted(track_dir, segment, progress_callback);

  segment.files = track_dir.FlushTracked(segment.byte_size);

  return result;
}

}  // namespace irs
