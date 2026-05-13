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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <faiss/impl/HNSW.h>

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "basics/containers/flat_hash_map.h"
#include "iresearch/types.hpp"

namespace duckdb {

class DatabaseInstance;
class BinaryDeserializer;

}  // namespace duckdb
namespace irs {

class Directory;
struct SegmentMeta;
struct HNSWInfo;

namespace columnstore {

class ColumnReader;
class ColumnWriter;
class NormColumnReader;
class NormColumnWriter;
class HNSWReader;
class HNSWWriter;

// Maps an HNSW-bearing column id to its in-memory faiss graph. Produced
// by Writer::TakeBuiltHnswGraphs after Commit(); consumed by the Reader
// constructor to skip re-deserializing graphs we just serialized.
using PreloadedHnswGraphs =
  sdb::containers::FlatHashMap<field_id, std::shared_ptr<faiss::HNSW>>;

// One file per segment.
inline constexpr std::string_view kFormatExt = "cs";

inline constexpr std::string_view kFormatName = "iresearch_columnstore";
inline constexpr int32_t kFormatVersion = 0;

// Matches duckdb::STANDARD_ROW_GROUP_SIZE.
inline constexpr uint64_t kDefaultRowGroupSize = 122880;

// Writes a segment's columnstore into `<segment>.cs`. Forward-write only;
// footer at the tail via duckdb::BinarySerializer.
class Writer final {
 public:
  Writer(Directory& dir, std::string_view segment_name,
         duckdb::DatabaseInstance& db);
  ~Writer();

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  // STORE / norm / HNSW paths share one Writer per segment; they MUST
  // allocate ids here to avoid id collisions across paths.
  [[nodiscard]] field_id AllocateColumnId() noexcept;

  // row_group_size = 0 selects kDefaultRowGroupSize. Returned reference
  // is valid until Commit/Rollback/dtor.
  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type,
                           uint64_t row_group_size = 0,
                           bool skip_validity = false,
                           duckdb::CompressionType compression =
                             duckdb::CompressionType::COMPRESSION_AUTO);

  // Norm columns share the .cs file with a fixed 1/2/4-byte raw layout
  // per row group plus per-RG stats in the footer.
  NormColumnWriter& OpenNormColumn(field_id id, std::string_view name,
                                   uint64_t row_group_size = 0);

  // Attach an HNSW graph to a previously-opened ARRAY column. Graph is
  // built at Commit() from the just-flushed column bytes and emitted as
  // an inline side-payload referenced by footer slot 102.
  HNSWWriter& AttachHNSW(field_id column_id, HNSWInfo info);

  // Returns the file name; callers already track via TrackingDirectory so
  // the return is informational.
  std::string Commit();
  void Rollback() noexcept;

  PreloadedHnswGraphs TakeBuiltHnswGraphs();

 private:
  struct Impl;
  std::unique_ptr<Impl> _impl;
};

// Opens `<segment>.cs` and exposes per-column access.
class Reader final {
 public:
  Reader(const Directory& dir, std::string_view segment_name,
         duckdb::DatabaseInstance& db, PreloadedHnswGraphs preloaded = {});
  ~Reader();

  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  bool HasColumn(field_id id) const noexcept;
  // nullptr if absent.
  const ColumnReader* Column(field_id id) const noexcept;
  std::span<const std::unique_ptr<ColumnReader>> Columns() const noexcept;

  // Norm and typed maps are independent; a field_id may appear in both.
  bool HasNormColumn(field_id id) const noexcept;
  const NormColumnReader* NormColumn(field_id id) const noexcept;

  // HNSW(id) returns nullptr if id is not an HNSW column in this segment.
  bool HasHNSW(field_id id) const noexcept;
  const HNSWReader* HNSW(field_id id) const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> _impl;

  // Reader::Reader splits its footer-parse work across these three;
  // each iterates one of the kFooterSlot* lists and populates the
  // matching `_impl->*_readers` / `*_by_id` pair.
  void BuildColumnReaders(duckdb::BinaryDeserializer& deserializer,
                          duckdb::DatabaseInstance& db);
  void BuildNormReaders(duckdb::BinaryDeserializer& deserializer);
  void BuildHnswReaders(duckdb::BinaryDeserializer& deserializer);
};

}  // namespace columnstore
}  // namespace irs
