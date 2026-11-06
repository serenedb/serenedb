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

#include <duckdb/common/file_system.hpp>
#include <memory>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/store/index_files.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/hash_utils.hpp"

namespace irs {

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
    duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
    const SegmentMeta& meta, const IndexReaderOptions& options);

  std::shared_ptr<const SegmentReaderImpl> UpdateMeta(
    IndexFileRefs& refs, const SegmentMeta& meta) const;

  std::shared_ptr<const SegmentReaderImpl> Reopen(
    duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
    const SegmentMeta& meta, const IndexReaderOptions& options) const;

  const SegmentInfo& Meta() const final { return _info; }

  const DocumentMask* docs_mask() const final { return _docs_mask.get(); }

  const std::shared_ptr<const DocumentMask>& SharedDocsMask() const noexcept {
    return _docs_mask;
  }

  uint64_t Id() const noexcept { return _id; }

  const std::shared_ptr<DocMapsSlot>& DocMaps() const noexcept {
    return _doc_maps;
  }

  lead::Node::ptr docs_iterator() const final;

  std::span<const TermReader* const> fields(field_id id) const final {
    return _data->field_reader.fields(id);
  }

  std::span<const field_id> field_ids() const final {
    return _data->field_reader.field_ids();
  }

  NormReader::ptr norms(field_id field) const final;

  const ColumnReader* Column(field_id field) const final {
    return _data->col_reader.Column(field);
  }
  const AnnIndex* Ann(field_id field) const final {
    return _data->idx_reader.Ann(field);
  }
  const ColReader* GetColReader() const final { return &_data->col_reader; }

 private:
  struct ColumnData {
    ColumnData(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
               std::string_view root, const SegmentMeta& meta,
               HnswBases* hnsw_bases);

    ColReader col_reader;
    IdxReader idx_reader;
    term_dict::FieldReader field_reader;
  };

  FileRefs _refs;
  SegmentInfo _info;
  std::shared_ptr<const DocumentMask> _docs_mask;
  uint64_t _id = 0;
  std::shared_ptr<DocMapsSlot> _doc_maps;
  std::shared_ptr<const ColumnData> _data;
};

}  // namespace irs
