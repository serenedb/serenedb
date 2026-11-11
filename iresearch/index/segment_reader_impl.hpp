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

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/column/column_patch.hpp"
#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/piece_fields.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/hash_utils.hpp"

namespace irs {

class ColReader;
class NormColumnReader;
struct DocMapsSlot;

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
    const Directory& dir, const SegmentMeta& meta,
    const IndexReaderOptions& options);

  static std::shared_ptr<const SegmentReaderImpl> OpenInMemory(
    const SegmentMeta& meta, uint64_t id, std::unique_ptr<ColReader> columns);

  static std::shared_ptr<const SegmentReaderImpl> OpenPiece(
    const SegmentMeta& meta, uint64_t id, std::unique_ptr<ColReader> columns,
    std::shared_ptr<const PieceFields> fields);

  std::shared_ptr<const SegmentReaderImpl> WithDocsMask(
    std::shared_ptr<const DocumentMask> mask) const;
  std::shared_ptr<const SegmentReaderImpl> WithPatches(
    std::span<const FieldPatch> patches) const;

  uint64_t CountMappedMemory() const final;

  const SegmentInfo& Meta() const final { return _info; }

  const DocumentMask* docs_mask() const final { return _docs_mask.get(); }

  uint64_t PatchedDocs() const noexcept final { return _patched_docs; }

  const std::shared_ptr<const DocumentMask>& SharedDocsMask() const noexcept {
    return _docs_mask;
  }

  uint64_t Id() const noexcept { return _id; }
  uint64_t PatchTick() const noexcept { return _patch_tick; }

  const std::shared_ptr<DocMapsSlot>& DocMaps() const noexcept {
    return _doc_maps;
  }

  lead::Node::ptr docs_iterator() const final;

  std::span<const TermReader* const> fields(field_id id) const final {
    if (_pieces) {
      return _pieces->fields(id);
    }
    if (!_field_reader) {
      return {};
    }
    return _field_reader->fields(id);
  }

  std::span<const field_id> field_ids() const final {
    if (_pieces) {
      return _pieces->field_ids();
    }
    if (!_field_reader) {
      return {};
    }
    return _field_reader->field_ids();
  }

  NormReader::ptr norms(field_id field) const final;

  const ColumnReader* Column(field_id field) const final;
  const AnnIndex* Ann(field_id field) const final;
  IndexInput::ptr ReopenAnn() const final;
  const ColReader* GetColReader() const final {
    return _data ? _data->col_reader.get() : nullptr;
  }

 private:
  struct ColumnData {
    std::unique_ptr<ColReader> col_reader;
    std::unique_ptr<IdxReader> idx_reader;
    std::shared_ptr<ColumnData> base;

    const IdxReader* Idx() const noexcept {
      return idx_reader ? idx_reader.get() : base ? base->Idx() : nullptr;
    }
    void Open(const Directory& dir, const SegmentMeta& meta,
              const IndexReaderOptions& options, bool keep_terms);
  };

  FileRefs _refs;
  SegmentInfo _info;
  std::shared_ptr<const DocumentMask> _docs_mask;
  uint64_t _id = 0;
  uint64_t _patch_tick = 0;
  uint64_t _patched_docs = 0;
  std::shared_ptr<DocMapsSlot> _doc_maps;
  std::shared_ptr<ColumnData> _data;
  std::shared_ptr<term_dict::FieldReader> _field_reader;
  std::shared_ptr<const PieceFields> _pieces;
};

}  // namespace irs
