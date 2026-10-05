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

#include <duckdb/common/types.hpp>
#include <utility>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/norm_reader_impl.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

class SegmentAllDocs : public lead::Node {
 public:
  explicit SegmentAllDocs(doc_id_t docs_count) noexcept
    : _max_doc{doc_limits::min() + docs_count - 1} {}

  doc_id_t Next() noexcept final {
    _doc = _doc < _max_doc ? _doc + 1 : doc_limits::eof();
    return _doc;
  }

  doc_id_t Seek(doc_id_t target) noexcept final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    _doc = target <= _max_doc ? target : doc_limits::eof();
    return _doc;
  }

 private:
  const doc_id_t _max_doc;
  doc_id_t _doc = doc_limits::invalid();
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
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = GetRefs(dir, meta);
  reader->_data = std::make_shared<ColumnData>();
  reader->_data->Open(dir, meta, options);
  reader->_field_reader =
    std::make_shared<burst_trie::FieldReader>(*dir.ResourceManager().readers);
  if (options.index) {
    reader->_field_reader->prepare(ReaderState{
      .dir = &dir,
      .meta = &meta,
      .idx = reader->_data->idx_reader.get(),
    });
  }
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::ReopenReader(
  const Directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& options) const {
  SDB_ASSERT(meta == _info);
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = _refs;
  reader->_field_reader = _field_reader;
  reader->_data = std::make_shared<ColumnData>();
  reader->_data->Open(dir, meta, options);
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::UpdateMeta(
  const Directory& dir, const SegmentMeta& meta) const {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = GetRefs(dir, meta);
  reader->_field_reader = _field_reader;
  reader->_data = _data;
  return reader;
}

uint64_t SegmentReaderImpl::CountMappedMemory() const {
  uint64_t bytes = 0;
  if (_field_reader != nullptr) {
    bytes += _field_reader->CountMappedMemory();
  }
  return bytes;
}

NormReader::ptr SegmentReaderImpl::norms(field_id field) const {
  if (!_data) {
    return {};
  }
  const auto* nc = _data->col_reader->NormColumn(field);
  if (!nc) {
    return {};
  }
  return MakePersistedNormReader(*nc);
}

const ColumnReader* SegmentReaderImpl::Column(field_id field) const {
  return _data->col_reader->Column(field);
}

const AnnIndex* SegmentReaderImpl::Ann(field_id field) const {
  if (!_data || !_data->idx_reader) {
    return nullptr;
  }
  return _data->idx_reader->Ann(field);
}

IndexInput::ptr SegmentReaderImpl::ReopenAnn() const {
  if (!_data || !_data->idx_reader) {
    return nullptr;
  }
  return _data->idx_reader->ReopenIn();
}

lead::Node::ptr SegmentReaderImpl::docs_iterator() const {
  if (!_docs_mask) {
    return memory::make_managed<SegmentAllDocs>(VisibleCount(_info));
  }
  SDB_ASSERT(!_docs_mask->Empty());

  const auto live_end =
    static_cast<doc_id_t>(doc_limits::min() + VisibleCount(_info));
  return ResolveDocsMask(
    _docs_mask.get(), doc_limits::eof(),
    [&]<DocsMaskType Mask>(Mask docs_mask) -> lead::Node::ptr {
      return memory::make_managed<lead::Impl<detail::LiveDocs<Mask>>>(
        std::move(docs_mask), live_end);
    });
}

void SegmentReaderImpl::ColumnData::Open(const Directory& dir,
                                         const SegmentMeta& meta,
                                         const IndexReaderOptions& options) {
  SDB_ASSERT(options.db);
  col_reader =
    std::make_unique<ColReader>(dir, meta.name, *options.db, IOAdvice::RANDOM);
  idx_reader = std::make_unique<IdxReader>(dir, meta.name);
}

}  // namespace irs
