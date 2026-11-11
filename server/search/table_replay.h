////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/btree_map.h>

#include <cstdint>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/formats/column/column_patch.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "connector/column_id.h"
#include "search/table.h"

namespace sdb::search {

class TableReplay {
 public:
  struct Segment {
    std::string meta_file;
    irs::doc_id_t rows = 0;
    irs::DocumentMaskBuilder deleted;
    std::vector<irs::FieldPatch> patches;
    OwnSegment opened;
  };

  struct Chunk {
    uint64_t first = 0;
    duckdb::unique_ptr<duckdb::DataChunk> rows;
    std::shared_ptr<const std::vector<connector::ColumnId>> columns;
    irs::DocumentMaskBuilder deleted;
  };

  struct Piece {
    size_t begin = 0;
    size_t end = 0;
    std::shared_ptr<const irs::SegmentReaderImpl> reader;
  };

  void Drop(std::span<const uint64_t> segments, uint64_t memory_below);
  void Add(const LoggedSegment& segment);
  void Append(duckdb::Catalog& catalog, uint64_t first,
              std::shared_ptr<const std::vector<connector::ColumnId>> columns,
              duckdb::unique_ptr<duckdb::DataChunk> rows);
  void Delete(uint64_t segment, const irs::DocumentMask& docs);
  void DeleteMemory(uint64_t base, const irs::DocumentMask& docs);
  void Update(uint64_t segment, const irs::FieldPatch& patch);
  void UpdateMemory(const irs::FieldPatch& patch);
  void SetManifest(std::shared_ptr<const FileManifest> manifest) {
    _replayed = true;
    _manifest = std::move(manifest);
  }

  bool Replayed() const noexcept { return _replayed; }
  uint64_t Top() const noexcept { return _top; }
  uint64_t MemoryEnd() const noexcept { return _memory_end; }
  const std::shared_ptr<const FileManifest>& Manifest() const noexcept {
    return _manifest;
  }
  duckdb::Catalog& RowsCatalog() const noexcept { return *_catalog; }
  absl::btree_map<uint64_t, Segment>& Segments() noexcept { return _segments; }
  std::vector<Piece>& Cut();
  std::span<Chunk> Chunks(const Piece& piece) noexcept {
    return std::span{_chunks}.subspan(piece.begin, piece.end - piece.begin);
  }
  std::vector<std::shared_ptr<TableUnit>> Units(const Table& table) &&;

 private:
  Segment& Find(uint64_t id);

  absl::btree_map<uint64_t, Segment> _segments;
  std::vector<Chunk> _chunks;
  std::vector<irs::FieldPatch> _memory_patches;
  std::vector<Piece> _pieces;
  std::shared_ptr<const FileManifest> _manifest;
  duckdb::Catalog* _catalog = nullptr;
  uint64_t _memory_end = 0;
  uint64_t _top = 0;
  bool _replayed = false;
};

}  // namespace sdb::search
