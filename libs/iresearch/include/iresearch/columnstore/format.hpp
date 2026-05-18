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

#include <faiss/impl/HNSW.h>

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "basics/containers/flat_hash_map.h"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_input.hpp"
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

using PreloadedHnswGraphs =
  sdb::containers::FlatHashMap<field_id, std::shared_ptr<const faiss::HNSW>>;

// One file per segment.
inline constexpr std::string_view kFormatExt = "cs";

inline constexpr std::string_view kFormatName = "iresearch_columnstore";
inline constexpr int32_t kFormatVersion = 0;

// Writes a segment's columnstore into `<segment>.cs`. Forward-write only;
// footer at the tail via duckdb::BinarySerializer.
class Writer final {
 public:
  Writer(Directory& dir, std::string_view segment_name,
         duckdb::DatabaseInstance& db,
         const ColumnOptionsProvider* column_options = nullptr,
         const NormColumnOptionsProvider* norm_column_options = nullptr);
  ~Writer();

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type);

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type,
                           bool skip_validity, uint32_t row_group_size,
                           duckdb::CompressionType compression);

  NormColumnWriter* OpenNormColumn(std::string_view field_name);

  NormColumnWriter& OpenNormColumn(field_id id, uint32_t row_group_size);

  std::span<const std::unique_ptr<NormColumnWriter>> NormWriters()
    const noexcept;

  // Attach an HNSW graph to a previously-opened ARRAY column. Graph is
  // built at Commit() from the just-flushed column bytes and emitted as
  // an inline side-payload referenced by footer slot 102.
  HNSWWriter& AttachHNSW(field_id column_id, HNSWInfo info);

  std::string Commit(uint64_t target_row);
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
         duckdb::DatabaseInstance& db,
         const PreloadedHnswGraphs& preloaded = {});
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

  IndexInput::ptr ReopenIn() const;
  duckdb::DatabaseInstance& Database() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> _impl;

  // Reader::Reader splits its footer-parse work across these three;
  // each iterates one of the kFooterSlot* lists and populates the
  // matching `_impl->*_readers` / `*_by_id` pair.
  void BuildColumnReaders(duckdb::BinaryDeserializer& deserializer,
                          duckdb::DatabaseInstance& db);
  void BuildNormReaders(duckdb::BinaryDeserializer& deserializer);
  void BuildHnswReaders(duckdb::BinaryDeserializer& deserializer,
                        const PreloadedHnswGraphs& preloaded);
};

}  // namespace columnstore
}  // namespace irs
