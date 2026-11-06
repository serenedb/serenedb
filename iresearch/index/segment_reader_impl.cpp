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

#include <absl/algorithm/container.h>

#include <duckdb/common/types.hpp>
#include <duckdb/main/database.hpp>
#include <utility>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/norm_reader_impl.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/index/doc_removal.hpp"
#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/index/file_names.hpp"
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

FileRefs GetRefs(IndexFileRefs& refs, const SegmentMeta& meta) {
  FileRefs file_refs;
  file_refs.reserve(meta.files.size());
  for (auto& file : meta.files) {
    file_refs.emplace_back(refs.add(file));
  }
  return file_refs;
}

bool HasFile(const SegmentMeta& meta, std::string_view ext) {
  return absl::c_linear_search(meta.files, irs::FileName(meta.name, ext));
}

}  // namespace

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
  const SegmentMeta& meta, const IndexReaderOptions& options) {
  SDB_ASSERT(options.refs);
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = GetRefs(*options.refs, meta);
  ParseSegmentId(meta.name, reader->_id);
  reader->_doc_maps = std::make_shared<DocMapsSlot>();
  reader->_data =
    std::make_shared<const ColumnData>(db, fs, root, meta, options.hnsw_bases);
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::UpdateMeta(
  IndexFileRefs& refs, const SegmentMeta& meta) const {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = GetRefs(refs, meta);
  reader->_id = _id;
  reader->_doc_maps = _doc_maps;
  reader->_data = _data;
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Reopen(
  duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
  const SegmentMeta& meta, const IndexReaderOptions& options) const {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = _refs;
  reader->_id = _id;
  reader->_doc_maps = _doc_maps;
  reader->_data =
    std::make_shared<const ColumnData>(db, fs, root, meta, options.hnsw_bases);
  return reader;
}

NormReader::ptr SegmentReaderImpl::norms(field_id field) const {
  const auto* nc = _data->col_reader.NormColumn(field);
  if (!nc) {
    return {};
  }
  return MakePersistedNormReader(*nc);
}

lead::Node::ptr SegmentReaderImpl::docs_iterator() const {
  if (!_docs_mask) {
    return memory::make_managed<SegmentAllDocs>(_info.docs_count);
  }
  SDB_ASSERT(!_docs_mask->Empty());

  const auto live_end =
    static_cast<doc_id_t>(doc_limits::min() + _info.docs_count);
  return ResolveDocsMask(
    _docs_mask.get(),
    [&]<DocsMaskType Mask>(Mask docs_mask) -> lead::Node::ptr {
      return memory::make_managed<lead::Impl<detail::LiveDocs<Mask>>>(
        std::move(docs_mask), live_end);
    });
}

SegmentReaderImpl::ColumnData::ColumnData(duckdb::DatabaseInstance& db,
                                          duckdb::FileSystem& fs,
                                          std::string_view root,
                                          const SegmentMeta& meta,
                                          HnswBases* hnsw_bases)
  : col_reader{db,
               fs,
               root,
               meta.name,
               HasFile(meta, kFormatExt),
               duckdb::MappedFileAccess::RANDOM},
    idx_reader{fs, root, meta.name, HasFile(meta, kIdxFormatExt), hnsw_bases} {
  field_reader.prepare(ReaderState{
    .fs = fs,
    .root = root,
    .meta = meta,
    .idx = &idx_reader,
  });
}

}  // namespace irs
