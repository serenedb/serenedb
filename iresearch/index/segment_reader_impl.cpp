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

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
  const SegmentMeta& meta, const IndexReaderOptions& options,
  std::shared_ptr<const FileTracker> files) {
  SegmentInputs inputs{
    .col = std::make_unique<ColReader>(db, fs, root, meta.name,
                                       (meta.kinds & kColFile) != 0,
                                       duckdb::MappedFileAccess::RANDOM),
    .idx = std::make_unique<IdxReader>(
      fs, root, meta.name, (meta.kinds & kIdxFile) != 0, options.hnsw_bases),
  };
  return Open(db, fs, root, meta, options, std::move(files), std::move(inputs));
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
  const SegmentMeta& meta, const IndexReaderOptions& options,
  std::shared_ptr<const FileTracker> files, SegmentInputs inputs) {
  SDB_ASSERT(inputs.col && inputs.idx);
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_files = std::move(files);
  ParseSegmentId(meta.name, reader->_id);
  reader->_readers = std::make_shared<Readers>();
  reader->_readers->col_reader = std::move(inputs.col);
  reader->_readers->idx_reader = std::move(inputs.idx);
  reader->_field_reader = std::make_shared<term_dict::FieldReader>();
  reader->_field_reader->prepare(ReaderState{
    .db = db,
    .fs = fs,
    .root = root,
    .meta = meta,
    .idx = reader->_readers->idx_reader.get(),
    .postings = inputs.postings.doc ? &inputs.postings : nullptr,
  });
  if (reader->_info.byte_size == 0) {
    const auto handles = reader->_field_reader->Postings().Handles();
    for (const auto* file : {reader->_readers->col_reader->File(),
                             reader->_readers->idx_reader->File(), handles.doc,
                             handles.pos, handles.pay}) {
      if (file) {
        reader->_info.byte_size += file->GetSize();
      }
    }
  }
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::OpenInMemory(
  const SegmentMeta& meta, uint64_t id, std::unique_ptr<ColReader> columns) {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_id = id;
  reader->_readers = std::make_shared<Readers>();
  reader->_readers->col_reader = std::move(columns);
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::OpenPiece(
  const SegmentMeta& meta, uint64_t id, std::unique_ptr<ColReader> columns,
  std::unique_ptr<IdxReader> norms, std::shared_ptr<const PieceFields> fields) {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_id = id;
  reader->_readers = std::make_shared<Readers>();
  reader->_readers->col_reader = std::move(columns);
  reader->_readers->idx_reader = std::move(norms);
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
  reader->_field_reader = _field_reader;
  reader->_pieces = _pieces;
  reader->_readers = _readers;
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
  reader->_field_reader = _field_reader;
  reader->_pieces = _pieces;
  const auto& root = _readers->base ? _readers->base : _readers;
  if (patches.empty()) {
    reader->_readers = root;
    return reader;
  }
  reader->_patches.assign(patches.begin(), patches.end());
  std::vector<std::shared_ptr<const ColumnReader>> replaced;
  replaced.reserve(patches.size());
  for (const auto& [field, patch] : patches) {
    reader->_patch_tick = std::max(reader->_patch_tick, patch->MaxTick());
    reader->_patched_docs = std::max(reader->_patched_docs, patch->Count());
    if (const auto* column = root->col_reader->Column(field)) {
      SDB_ASSERT(column->Type() == patch->Type());
      replaced.push_back(std::make_shared<PatchedColumnReader>(*column, patch));
      continue;
    }
    replaced.push_back(std::make_shared<PatchedColumnReader>(
      std::make_unique<NullColumnReader>(root->col_reader->Database(), field,
                                         patch->Type(), _info.docs_count),
      patch));
  }
  auto readers = std::make_shared<Readers>();
  readers->col_reader =
    std::make_unique<ColReader>(*root->col_reader, std::move(replaced));
  readers->base = root;
  reader->_readers = std::move(readers);
  return reader;
}

NormReader::ptr SegmentReaderImpl::norms(field_id field) const {
  const auto* column = NormColumn(field);
  return column ? MakePersistedNormReader(*column) : NormReader::ptr{};
}

const NormColumnReader* SegmentReaderImpl::NormColumn(field_id field) const {
  const auto* idx = _readers ? _readers->Idx() : nullptr;
  return idx ? idx->NormColumn(field) : nullptr;
}

const ColumnReader* SegmentReaderImpl::Column(field_id field) const {
  return _readers->col_reader->Column(field);
}

const AnnIndex* SegmentReaderImpl::Ann(field_id field) const {
  const auto* idx = _readers ? _readers->Idx() : nullptr;
  if (!idx) {
    return nullptr;
  }
  return idx->Ann(field);
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
