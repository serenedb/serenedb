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

#include <absl/strings/str_cat.h>
#include <faiss/impl/HNSW.h>

#include <cstdint>
#include <duckdb/common/serializer/serialization_traits.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/types.hpp"

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs {

class ColumnReader;
class NormColumnReader;
struct HNSWInfo;

using PreloadedHnswGraphs =
  sdb::containers::FlatHashMap<field_id, std::shared_ptr<const faiss::HNSW>>;

struct BuiltHnsw {
  field_id column_id;
  HNSWInfo info;
  std::shared_ptr<const faiss::HNSW> graph;
};

inline constexpr std::string_view kFormatName = "iresearch_col";
inline constexpr int32_t kFormatVersion = 0;
inline constexpr std::string_view kFormatExt = "col";

inline constexpr duckdb::field_id_t kFooterSlotColumns = 100;
inline constexpr duckdb::field_id_t kFooterSlotNormColumns = 101;

inline std::string FileName(std::string_view segment_name) {
  return absl::StrCat(segment_name, ".", kFormatExt);
}

class ColReader final {
 public:
  ColReader(const Directory& dir, std::string_view segment_name,
            duckdb::DatabaseInstance& db);
  ~ColReader();

  ColReader(const ColReader&) = delete;
  ColReader& operator=(const ColReader&) = delete;

  bool HasColumn(field_id id) const noexcept { return _by_id.contains(id); }
  const ColumnReader* Column(field_id id) const noexcept;
  std::span<const std::unique_ptr<ColumnReader>> Columns() const noexcept {
    return _columns;
  }

  bool HasNormColumn(field_id id) const noexcept;
  const NormColumnReader* NormColumn(field_id id) const noexcept;
  ReadContext& Ctx() noexcept { return _ctx; }

  IndexInput::ptr ReopenIn() const {
    return _ctx.HasIn() ? _ctx.In().Reopen() : nullptr;
  }
  duckdb::DatabaseInstance& Database() const noexcept { return *_db; }

 private:
  duckdb::DatabaseInstance* _db;
  ReadContext _ctx;
  std::vector<std::unique_ptr<ColumnReader>> _columns;
  sdb::containers::FlatHashMap<field_id, ColumnReader*> _by_id;
  std::vector<std::unique_ptr<NormColumnReader>> _norm_readers;
  sdb::containers::FlatHashMap<field_id, const NormColumnReader*> _norm_by_id;
};

}  // namespace irs
