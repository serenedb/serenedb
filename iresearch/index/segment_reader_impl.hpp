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

#pragma once

#include <duckdb/common/file_system.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_patch.hpp"
#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/piece_fields.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/store/storage.hpp"
#include "iresearch/utils/blocking_once.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

class NormColumnReader;

struct SegmentInputs {
  std::unique_ptr<ColReader> col;
  std::unique_ptr<IdxReader> idx;
  PostingsInputs postings;
};

struct WrittenSegment {
  std::shared_ptr<const FileTracker> files;
  SegmentInputs inputs;
};

class SegmentReaderImpl final : public SubReader {
  struct PrivateTag final {
    explicit PrivateTag() = default;
  };

 public:
  SegmentReaderImpl(PrivateTag, const SegmentMeta& meta) noexcept
    : _info{meta}, _docs_mask{meta.docs_mask} {
    SDB_ASSERT(meta.live_docs_count <= meta.docs_count);
    SDB_ASSERT(RemovalCount(meta) <= meta.docs_count);
    _info.live_docs_count = meta.docs_count - RemovalCount(meta);
  }

  static std::shared_ptr<const SegmentReaderImpl> Open(
    duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
    const SegmentMeta& meta, const IndexReaderOptions& options,
    std::shared_ptr<const FileTracker> files);

  static std::shared_ptr<const SegmentReaderImpl> Open(
    duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
    const SegmentMeta& meta, const IndexReaderOptions& options,
    std::shared_ptr<const FileTracker> files, SegmentInputs inputs);

  static std::shared_ptr<const SegmentReaderImpl> OpenInMemory(
    const SegmentMeta& meta, uint64_t id, std::unique_ptr<ColReader> columns);

  static std::shared_ptr<const SegmentReaderImpl> OpenPiece(
    const SegmentMeta& meta, uint64_t id, std::unique_ptr<ColReader> columns,
    std::shared_ptr<const PieceFields> fields);

  std::shared_ptr<const SegmentReaderImpl> WithDocsMask(
    std::shared_ptr<const DocumentMask> mask) const;
  std::shared_ptr<const SegmentReaderImpl> WithPatches(
    std::span<const FieldPatch> patches) const;

  const SegmentInfo& Meta() const final { return _info; }

  const DocumentMask* docs_mask() const final { return _docs_mask.get(); }

  uint64_t PatchedDocs() const noexcept final { return _patched_docs; }

  const std::shared_ptr<const DocumentMask>& SharedDocsMask() const noexcept {
    return _docs_mask;
  }

  uint64_t Id() const noexcept { return _id; }
  uint64_t PatchTick() const noexcept { return _patch_tick; }
  const std::shared_ptr<const FileTracker>& Files() const noexcept {
    return _files;
  }
  std::span<const FieldPatch> Patches() const noexcept { return _patches; }

  void OpenFiles() const;
  bool FilesOpened() const noexcept { return _parts->Opened(); }

  lead::Node::ptr docs_iterator() const final;

  std::span<const TermReader* const> fields(field_id id) const final {
    if (_pieces) {
      return _pieces->fields(id);
    }
    const auto* index = _parts->GetIndex();
    return index ? index->fields.fields(id)
                 : std::span<const TermReader* const>{};
  }

  std::span<const field_id> field_ids() const final {
    if (_pieces) {
      return _pieces->field_ids();
    }
    const auto* index = _parts->GetIndex();
    return index ? index->fields.field_ids() : std::span<const field_id>{};
  }

  NormReader::ptr norms(field_id field) const final;

  const ColumnReader* Column(field_id field) const final;
  const AnnIndex* Ann(field_id field) const final;
  const ColReader* GetColReader() const final;

 private:
  struct Index {
    std::unique_ptr<IdxReader> idx;
    term_dict::FieldReader fields;
  };

  class Parts {
   public:
    Parts(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
          std::string_view root, const SegmentMeta& meta,
          HnswBases* hnsw_bases);
    Parts(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
          std::string_view root, const SegmentMeta& meta, SegmentInputs inputs,
          HnswBases* hnsw_bases);
    explicit Parts(std::unique_ptr<ColReader> columns);

    const ColReader& Columns() const;
    const Index* GetIndex() const;
    bool Opened() const noexcept {
      return _columns_once.Done() && _index_once.Done();
    }

   private:
    void PrepareFields(Index& index, PostingsInputs* postings) const;

    duckdb::DatabaseInstance* _db = nullptr;
    duckdb::FileSystem* _fs = nullptr;
    std::string _root;
    SegmentMeta _meta;
    HnswBases* _hnsw_bases = nullptr;
    bool _indexed = false;
    mutable BlockingOnce _columns_once;
    mutable std::unique_ptr<ColReader> _columns;
    mutable BlockingOnce _index_once;
    mutable std::unique_ptr<Index> _index;
  };

  struct PatchedColumns {
    BlockingOnce once;
    std::unique_ptr<ColReader> columns;
  };

  std::shared_ptr<const FileTracker> _files;
  SegmentInfo _info;
  std::shared_ptr<const DocumentMask> _docs_mask;
  uint64_t _id = 0;
  uint64_t _patch_tick = 0;
  uint64_t _patched_docs = 0;
  std::vector<FieldPatch> _patches;
  std::shared_ptr<const Parts> _parts;
  std::shared_ptr<PatchedColumns> _patched;
  std::shared_ptr<const PieceFields> _pieces;
};

}  // namespace irs
