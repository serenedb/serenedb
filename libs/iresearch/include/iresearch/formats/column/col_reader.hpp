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

class ColumnReader;
class NormColumnReader;

using PreloadedHnswGraphs =
  sdb::containers::FlatHashMap<field_id, std::shared_ptr<const faiss::HNSW>>;

struct BuiltHnsw {
  field_id column_id;
  HNSWInfo info;
  std::shared_ptr<const faiss::HNSW> graph;
};

// One file per segment.
inline constexpr std::string_view kColFormatExt = "col";

inline constexpr std::string_view kColFormatName = "iresearch_columnstore";
inline constexpr int32_t kColFormatVersion = 0;

// Opens `<segment>.col` and exposes per-column access.
class ColReader final {
 public:
  ColReader(const Directory& dir, std::string_view segment_name,
            duckdb::DatabaseInstance& db);
  ~ColReader();

  ColReader(const ColReader&) = delete;
  ColReader& operator=(const ColReader&) = delete;

  bool HasColumn(field_id id) const noexcept;
  // nullptr if absent.
  const ColumnReader* Column(field_id id) const noexcept;
  std::span<const std::unique_ptr<ColumnReader>> Columns() const noexcept;

  // Norm and typed maps are independent; a field_id may appear in both.
  bool HasNormColumn(field_id id) const noexcept;
  const NormColumnReader* NormColumn(field_id id) const noexcept;

  IndexInput::ptr ReopenIn() const;
  duckdb::DatabaseInstance& Database() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> _impl;

  // ColReader::ColReader splits its footer-parse work across these two;
  // each iterates one of the kFooterSlot* lists and populates the
  // matching `_impl->*_readers` / `*_by_id` pair.
  void BuildColumnReaders(duckdb::BinaryDeserializer& deserializer,
                          duckdb::DatabaseInstance& db);
  void BuildNormReaders(duckdb::BinaryDeserializer& deserializer);
};

}  // namespace irs
