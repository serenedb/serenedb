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

#include <cstdint>
#include <duckdb/common/serializer/serialization_traits.hpp>
#include <duckdb/main/database.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

inline constexpr std::string_view kFormatExt = "col";

inline constexpr duckdb::field_id_t kColFieldColumns = 0;
inline constexpr duckdb::field_id_t kColFieldNormColumns = 1;

inline std::string FileName(std::string_view segment_name) {
  return absl::StrCat(segment_name, ".", kFormatExt);
}

class ColReader final {
 public:
  ColReader(const Directory& dir, std::string_view segment_name,
            duckdb::DatabaseInstance& db, IOAdvice advice);
  ColReader(duckdb::DatabaseInstance& db,
            std::vector<std::unique_ptr<ColumnReader>> columns);
  ColReader(duckdb::DatabaseInstance& db,
            std::vector<std::unique_ptr<ColumnReader>> columns,
            IndexInput::ptr norms_in, std::vector<NormColumnSpec> norms);
  ColReader(const ColReader& base,
            std::vector<std::shared_ptr<const ColumnReader>> replaced);
  ~ColReader();

  ColReader(const ColReader&) = delete;
  ColReader& operator=(const ColReader&) = delete;

  bool HasColumn(field_id id) const noexcept { return _by_id.contains(id); }
  const ColumnReader* Column(field_id id) const noexcept;
  std::span<const ColumnReader* const> Columns() const noexcept {
    return _columns;
  }

  const NormColumnReader* NormColumn(field_id id) const noexcept;
  ReadContext& Ctx() noexcept { return _ctx; }

  IndexInput::ptr ReopenIn() const {
    if (_base) {
      return _base->ReopenIn();
    }
    return _ctx.HasIn() ? _ctx.In().Reopen() : nullptr;
  }
  duckdb::DatabaseInstance& Database() const noexcept { return *_db; }

 private:
  void Index(const ColumnReader& column);

  duckdb::DatabaseInstance* _db;
  ReadContext _ctx;
  const ColReader* _base = nullptr;
  std::vector<std::unique_ptr<ColumnReader>> _owned;
  std::vector<std::shared_ptr<const ColumnReader>> _replaced;
  std::vector<const ColumnReader*> _columns;
  irs::containers::FlatHashMap<field_id, const ColumnReader*> _by_id;
  std::vector<std::unique_ptr<NormColumnReader>> _norm_columns;
  irs::containers::FlatHashMap<field_id, const NormColumnReader*> _norm_by_id;
};

}  // namespace irs
