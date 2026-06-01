////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <string>

#include "catalog/column_expr.h"
#include "catalog/object.h"

namespace sdb::catalog {

// A SQL view stored in the catalog.
// Stores the full DuckDB CreateViewInfo -- preserves aliases, types, names,
// etc. Serialized to/from RocksDB via DuckDB's BinarySerializer.
class PgSqlView final : public Object {
 public:
  PgSqlView(ObjectId schema_id, ObjectId id, std::string_view name,
            duckdb::unique_ptr<duckdb::CreateViewInfo> info);

  static std::shared_ptr<PgSqlView> ReadInternal(vpack::Slice slice,
                                                 ReadContext ctx);

  void WriteInternal(vpack::Builder& b) const final;
  std::shared_ptr<Object> Clone() const final;

  const duckdb::CreateViewInfo& GetInfo() const noexcept { return *_info; }

  Refs GetRefs(RefKinds kinds) const;

 private:
  duckdb::unique_ptr<duckdb::CreateViewInfo> _info;
};

}  // namespace sdb::catalog
