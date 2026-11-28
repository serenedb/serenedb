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
#include <duckdb/main/database_file_opener.hpp>
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

}  // namespace

SegmentReaderImpl::Parts::Parts(duckdb::DatabaseInstance& db,
                                duckdb::FileSystem& fs, std::string_view root,
                                const SegmentMeta& meta, HnswBases* hnsw_bases)
  : _db{&db},
    _fs{&fs},
    _root{root},
    _meta{meta},
    _hnsw_bases{hnsw_bases},
    _indexed{true},
    _remote{duckdb::DatabaseFileSystem::IsRemote(_root)} {
  _meta.docs_mask.reset();
}

SegmentReaderImpl::Parts::Parts(duckdb::DatabaseInstance& db,
                                duckdb::FileSystem& fs, std::string_view root,
                                const SegmentMeta& meta, SegmentInputs inputs,
                                HnswBases* hnsw_bases)
  : Parts{db, fs, root, meta, hnsw_bases} {
  SDB_ASSERT(inputs.col && inputs.idx);
  _columns = std::move(inputs.col);
  _columns_once.Call([] {});
  _index = std::make_unique<Index>();
  _index->idx = std::move(inputs.idx);
  PrepareFields(*_index, &inputs.postings);
  _index_once.Call([] {});
}

SegmentReaderImpl::Parts::Parts(std::unique_ptr<ColReader> columns)
  : _columns{std::move(columns)} {
  _columns_once.Call([] {});
  _index_once.Call([] {});
}

void SegmentReaderImpl::Parts::PrepareFields(Index& index,
                                             PostingsInputs* postings) const {
  index.fields.prepare(ReaderState{
    .db = *_db,
    .fs = *_fs,
    .root = _root,
    .meta = _meta,
    .idx = index.idx.get(),
    .postings = postings,
  });
}

const ColReader& SegmentReaderImpl::Parts::Columns() const {
  _columns_once.Call([&] {
    _columns = std::make_unique<ColReader>(*_db, *_fs, _root, _meta.name,
                                           SizesOf(_meta, kColFile),
                                           duckdb::MappedFileAccess::RANDOM);
  });
  return *_columns;
}

const SegmentReaderImpl::Index* SegmentReaderImpl::Parts::GetIndex() const {
  if (!_indexed) {
    return nullptr;
  }
  _index_once.Call([&] {
    auto index = std::make_unique<Index>();
    index->idx = std::make_unique<IdxReader>(
      *_fs, _root, _meta.name, SizesOf(_meta, kIdxFile), _hnsw_bases);
    PrepareFields(*index, nullptr);
    _index = std::move(index);
  });
  return _index.get();
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
  const SegmentMeta& meta, const IndexReaderOptions& options,
  std::shared_ptr<const FileTracker> files) {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_files = std::move(files);
  ParseSegmentId(meta.name, reader->_id);
  reader->_parts =
    std::make_shared<const Parts>(db, fs, root, meta, options.hnsw_bases);
  if (reader->_info.byte_size == 0) {
    reader->_info.byte_size = SegmentBytes(meta.files);
  }
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
  const SegmentMeta& meta, const IndexReaderOptions& options,
  std::shared_ptr<const FileTracker> files, SegmentInputs inputs) {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_files = std::move(files);
  ParseSegmentId(meta.name, reader->_id);
  reader->_parts = std::make_shared<const Parts>(
    db, fs, root, meta, std::move(inputs), options.hnsw_bases);
  if (reader->_info.byte_size == 0) {
    reader->_info.byte_size = SegmentBytes(meta.files);
  }
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::OpenInMemory(
  const SegmentMeta& meta, uint64_t id, std::unique_ptr<ColReader> columns) {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_id = id;
  reader->_parts = std::make_shared<const Parts>(std::move(columns));
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::OpenPiece(
  const SegmentMeta& meta, uint64_t id, std::unique_ptr<ColReader> columns,
  std::shared_ptr<const PieceFields> fields) {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_id = id;
  reader->_parts = std::make_shared<const Parts>(std::move(columns));
  reader->_pieces = std::move(fields);
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::WithDocsMask(
  std::shared_ptr<const DocumentMask> mask) const {
  SegmentMeta meta;
  static_cast<SegmentInfo&>(meta) = _info;
  meta.docs_mask = std::move(mask);
  meta.live_docs_count = meta.docs_count - RemovalCount(meta);
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_files = _files;
  reader->_id = _id;
  reader->_patch_tick = _patch_tick;
  reader->_patched_docs = _patched_docs;
  reader->_patches = _patches;
  reader->_parts = _parts;
  reader->_patched = _patched;
  reader->_pieces = _pieces;
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::WithPatches(
  std::span<const FieldPatch> patches) const {
  SegmentMeta meta;
  static_cast<SegmentInfo&>(meta) = _info;
  meta.docs_mask = _docs_mask;
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_files = _files;
  reader->_id = _id;
  reader->_parts = _parts;
  reader->_pieces = _pieces;
  if (patches.empty()) {
    return reader;
  }
  reader->_patches.assign(patches.begin(), patches.end());
  for (const auto& [field, patch] : patches) {
    reader->_patch_tick = std::max(reader->_patch_tick, patch->MaxTick());
    reader->_patched_docs = std::max(reader->_patched_docs, patch->Count());
  }
  reader->_patched = std::make_shared<PatchedColumns>();
  return reader;
}

void SegmentReaderImpl::OpenFiles() const {
  _parts->Columns();
  _parts->GetIndex();
}

const ColReader* SegmentReaderImpl::GetColReader() const {
  if (!_patched) {
    return &_parts->Columns();
  }
  _patched->once.Call([&] {
    const auto& base = _parts->Columns();
    std::vector<std::shared_ptr<const ColumnReader>> replaced;
    replaced.reserve(_patches.size());
    for (const auto& [field, patch] : _patches) {
      if (const auto* column = base.Column(field)) {
        SDB_ASSERT(column->Type() == patch->Type());
        replaced.push_back(
          std::make_shared<PatchedColumnReader>(*column, patch));
        continue;
      }
      replaced.push_back(std::make_shared<PatchedColumnReader>(
        std::make_unique<NullColumnReader>(base.Database(), field,
                                           patch->Type(), _info.docs_count),
        patch));
    }
    _patched->columns = std::make_unique<ColReader>(base, std::move(replaced));
  });
  return _patched->columns.get();
}

NormReader::ptr SegmentReaderImpl::norms(field_id field) const {
  const auto* nc = GetColReader()->NormColumn(field);
  if (!nc) {
    return {};
  }
  return MakePersistedNormReader(*nc);
}

const ColumnReader* SegmentReaderImpl::Column(field_id field) const {
  return GetColReader()->Column(field);
}

const AnnIndex* SegmentReaderImpl::Ann(field_id field) const {
  const auto* index = _parts->GetIndex();
  if (!index) {
    return nullptr;
  }
  return index->idx->Ann(field);
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

}  // namespace irs
