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

#include <cstdint>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/function/macro_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <iresearch/utils/assert.hpp>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

class Constraint;
class Identifier;
class TableCatalogEntry;
class UniqueConstraint;

}  // namespace duckdb
namespace sdb::pg {

static constexpr size_t kSqlStateSize = 5;

// Unpack MAKE_SQLSTATE code.
template<typename T>
void UnpackSqlState(T& buf, int sql_state) {
  if constexpr (requires { std::size(buf); }) {
    SDB_ASSERT(std::size(buf) >= kSqlStateSize);
  }

  for (size_t i = 0; i < 5; i++) {
    buf[i] = (sql_state & 0x3F) + '0';
    sql_state >>= 6;
  }
}

int16_t TableEntryAttnum(const duckdb::TableCatalogEntry& table,
                         duckdb::idx_t column_id);

std::vector<int16_t> KeyConstraintAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::UniqueConstraint& constraint);

std::string ConstraintName(const duckdb::TableCatalogEntry& table,
                           const duckdb::Constraint& constraint);

std::string QuoteIdentifier(std::string_view ident);
std::string QuoteLiteral(std::string_view value);

struct KeyIndex {
  const duckdb::TableCatalogEntry* table = nullptr;
  const duckdb::UniqueConstraint* constraint = nullptr;
};

KeyIndex FindKeyIndex(duckdb::ClientContext& context, duckdb::Catalog& database,
                      duckdb::idx_t oid);

KeyIndex FindKeyIndex(duckdb::ClientContext& context,
                      duckdb::SchemaCatalogEntry& schema,
                      std::string_view name);

struct Relation {
  duckdb::CatalogEntry* entry = nullptr;
  KeyIndex key;

  explicit operator bool() const noexcept { return entry || key.table; }
};

struct IndexColumn {
  std::string name;
  const duckdb::ParsedExpression* expression = nullptr;
  duckdb::idx_t position = 0;
  int16_t attnum = 0;
};

struct IndexColumns {
  std::vector<IndexColumn> columns;
  size_t keys = 0;
};

IndexColumns ColumnsOfIndex(const duckdb::IndexCatalogEntry& index,
                            const duckdb::TableCatalogEntry* table);

IndexColumns ColumnsOfIndex(const KeyIndex& key);

Relation RelationByName(duckdb::ClientContext& context,
                        const duckdb::QualifiedName& name);

Relation RelationByOid(duckdb::ClientContext& context, duckdb::idx_t oid);

std::string MacroBody(const duckdb::MacroFunction& macro);

std::string MacroParameterName(const duckdb::MacroFunction& macro,
                               duckdb::idx_t index);

}  // namespace sdb::pg
