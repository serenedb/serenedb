////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "segment_reader_impl.hpp"

#include <absl/strings/str_cat.h>

#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

class AllIterator : public DocIterator {
 public:
  explicit AllIterator(doc_id_t docs_count) noexcept
    : _max_doc{doc_limits::min() + docs_count - 1} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::Type<DocAttr>::id() == type ? &_doc : nullptr;
  }

  doc_id_t value() const noexcept final { return _doc.value; }

  bool next() noexcept final {
    if (_doc.value < _max_doc) {
      ++_doc.value;
      return true;
    } else {
      _doc.value = doc_limits::eof();
      return false;
    }
  }

  doc_id_t seek(doc_id_t target) noexcept final {
    _doc.value = target <= _max_doc ? target : doc_limits::eof();

    return _doc.value;
  }

 private:
  DocAttr _doc;
  doc_id_t _max_doc;  // largest valid doc_id
};

class MaskDocIterator : public DocIterator {
 public:
  MaskDocIterator(DocIterator::ptr&& it, const DocumentMask& mask) noexcept
    : _mask{mask}, _it{std::move(it)} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _it->GetMutable(type);
  }

  doc_id_t value() const final { return _it->value(); }

  bool next() final {
    while (_it->next()) {
      if (!_mask.contains(value())) {
        return true;
      }
    }
    return false;
  }

  doc_id_t seek(doc_id_t target) final {
    const auto doc = _it->seek(target);

    if (!_mask.contains(doc)) {
      return doc;
    }

    next();
    return value();
  }

 private:
  const DocumentMask& _mask;  // excluded document ids
  DocIterator::ptr _it;
};

class MaskedDocIterator : public DocIterator {
 public:
  MaskedDocIterator(doc_id_t begin, doc_id_t end,
                    const DocumentMask& docs_mask) noexcept
    : _docs_mask{docs_mask}, _end{end}, _next{begin} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::Type<DocAttr>::id() == type ? &_current : nullptr;
  }

  doc_id_t value() const final { return _current.value; }

  bool next() final {
    while (_next < _end) {
      _current.value = _next++;

      if (!_docs_mask.contains(_current.value)) {
        return true;
      }
    }

    _current.value = doc_limits::eof();
    return false;
  }

  doc_id_t seek(doc_id_t target) final {
    _next = target;
    next();
    return value();
  }

 private:
  const DocumentMask& _docs_mask;
  DocAttr _current;
  const doc_id_t _end;  // past last valid doc_id
  doc_id_t _next;
};

FileRefs GetRefs(const Directory& dir, const SegmentMeta& meta) {
  FileRefs file_refs;
  file_refs.reserve(meta.files.size());

  auto& refs = dir.attributes().refs();
  for (auto& file : meta.files) {
    file_refs.emplace_back(refs.add(file));
  }

  return file_refs;
}

}  // namespace

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  const Directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& options) {
  SDB_ASSERT(meta.codec);
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = GetRefs(dir, meta);
  // always instantiate to avoid unnecessary checks
  reader->_field_reader =
    meta.codec->get_field_reader(*dir.ResourceManager().readers);
  if (options.index) {
    reader->_field_reader->prepare(
      ReaderState{.dir = &dir, .meta = &meta, .scorers = options.scorers});
  }
  // open column store
  reader->_data = std::make_shared<ColumnData>();
  reader->_sort =
    reader->_data->Open(dir, meta, options, *reader->_field_reader);
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::ReopenColumnStore(
  const Directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& options) const {
  SDB_ASSERT(meta == _info);
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = _refs;
  reader->_field_reader = _field_reader;
  reader->_data = std::make_shared<ColumnData>();
  reader->_sort = reader->_data->Open(dir, meta, options, *_field_reader);
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::UpdateMeta(
  const Directory& dir, const SegmentMeta& meta) const {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  SDB_ASSERT(_refs == GetRefs(dir, meta));
  reader->_refs = _refs;
  reader->_field_reader = _field_reader;
  reader->_data = _data;
  reader->_sort = _sort;
  return reader;
}

uint64_t SegmentReaderImpl::CountMappedMemory() const {
  uint64_t bytes = 0;
  if (_field_reader != nullptr) {
    bytes += _field_reader->CountMappedMemory();
  }
  if (_data != nullptr && _data->columnstore_reader != nullptr) {
    bytes += _data->columnstore_reader->CountMappedMemory();
  }
  return bytes;
}

const irs::ColumnReader* SegmentReaderImpl::column(
  std::string_view name) const {
  const auto& named_columns = _data->named_columns;
  const auto it = named_columns.find(name);
  return it == named_columns.end() ? nullptr : it->second;
}

const irs::ColumnReader* SegmentReaderImpl::column(field_id field) const {
  SDB_ASSERT(_data->columnstore_reader);
  return _data->columnstore_reader->column(field);
}

ColumnIterator::ptr SegmentReaderImpl::columns() const {
  struct Less {
    bool operator()(const irs::ColumnReader& lhs,
                    std::string_view rhs) const noexcept {
      return lhs.name() < rhs;
    }
  };

  using IteratorT =
    IteratorAdaptor<std::string_view, irs::ColumnReader,
                    decltype(_data->sorted_named_columns.begin()),
                    ColumnIterator, Less>;

  return memory::make_managed<IteratorT>(
    std::begin(_data->sorted_named_columns),
    std::end(_data->sorted_named_columns));
}

DocIterator::ptr SegmentReaderImpl::docs_iterator() const {
  if (!_docs_mask) {
    return memory::make_managed<AllIterator>(_info.docs_count);
  }
  SDB_ASSERT(!_docs_mask->empty());

  // the implementation generates doc_ids sequentially
  return memory::make_managed<MaskedDocIterator>(
    doc_limits::min(), doc_limits::min() + _info.docs_count, *_docs_mask);
}

DocIterator::ptr SegmentReaderImpl::mask(DocIterator::ptr&& it) const {
  SDB_ASSERT(it);
  if (!_docs_mask) {
    return std::move(it);
  }
  SDB_ASSERT(!_docs_mask->empty());

  return memory::make_managed<MaskDocIterator>(std::move(it), *_docs_mask);
}

const irs::ColumnReader* SegmentReaderImpl::ColumnData::Open(
  const Directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& options, const FieldReader& field_reader) {
  SDB_ASSERT(meta.codec != nullptr);
  auto& codec = *meta.codec;
  // always instantiate to avoid unnecessary checks
  columnstore_reader = codec.get_columnstore_reader();

  if (!options.columnstore || !meta.column_store) {
    return {};
  }

  // initialize optional columnstore
  ColumnstoreReader::Options columnstore_opts;
  if (options.warmup_columns) {
    columnstore_opts.warmup_column = [warmup = options.warmup_columns,
                                      &field_reader,
                                      &meta](const ColumnReader& column) {
      return warmup(meta, field_reader, column);
    };
  }

  if (!columnstore_reader->prepare(dir, meta, columnstore_opts)) {
    throw IndexError{
      absl::StrCat("Failed to find existing (according to meta) "
                   "columnstore in segment '",
                   meta.name, "'")};
  }

  const irs::ColumnReader* sort{};
  if (field_limits::valid(meta.sort)) {
    sort = columnstore_reader->column(meta.sort);

    if (!sort) {
      throw IndexError{absl::StrCat(
        "Failed to find sort column '", meta.sort,
        "' (according to meta) in columnstore in segment '", meta.name, "'")};
    }
  }

  // FIXME(gnusi): too rough, we must exclude unnamed columns
  const auto num_columns = columnstore_reader->size();
  named_columns.reserve(num_columns);
  sorted_named_columns.reserve(num_columns);

  columnstore_reader->visit([this, &meta](const irs::ColumnReader& column) {
    const auto name = column.name();

    if (!IsNull(name)) {
      const auto [it, is_new] = named_columns.emplace(name, &column);
      IRS_IGNORE(it);

      if (!is_new) [[unlikely]] {
        throw IndexError{absl::StrCat("Duplicate named column '", name,
                                      "' in segment '", meta.name, "'")};
      }

      if (!sorted_named_columns.empty() &&
          sorted_named_columns.back().get().name() >= name) {
        throw IndexError{absl::StrCat(
          "Named columns are out of order in segment '", meta.name, "'")};
      }

      sorted_named_columns.emplace_back(column);
    }

    return true;
  });
  return sort;
}

}  // namespace irs
