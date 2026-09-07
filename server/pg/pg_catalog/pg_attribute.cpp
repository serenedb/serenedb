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
#include "pg/pg_catalog/pg_attribute.h"

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/parser/constraints/list.hpp>

#include "app/app_server.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "catalog1/permissions.h"
#include "connector/primary_key.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"
#include "pg/system_catalog.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgAttribute::attcompression),
  GetIndex(&PgAttribute::attstattarget),
  GetIndex(&PgAttribute::attoptions),
  GetIndex(&PgAttribute::attfdwoptions),
  GetIndex(&PgAttribute::attmissingval),
});

struct PgTypePhysicalInfo {
  int16_t attlen;
  bool attbyval;
  PgType::Typalign attalign;
  PgAttribute::Attstorage attstorage;
};

PgTypePhysicalInfo GetPhysicalInfo(int32_t type_oid) {
  switch (type_oid) {
    case PgTypeOID::kBool:
      return {1, true, PgType::Typalign::Char, PgAttribute::Attstorage::Plain};
    case PgTypeOID::kChar:
      return {1, true, PgType::Typalign::Char, PgAttribute::Attstorage::Plain};
    case PgTypeOID::kInt2:
      return {2, true, PgType::Typalign::Short, PgAttribute::Attstorage::Plain};
    case PgTypeOID::kInt4:
      return {4, true, PgType::Typalign::Int, PgAttribute::Attstorage::Plain};
    case PgTypeOID::kInt8:
      return {8, true, PgType::Typalign::Double,
              PgAttribute::Attstorage::Plain};
    case PgTypeOID::kFloat4:
      return {4, true, PgType::Typalign::Int, PgAttribute::Attstorage::Plain};
    case PgTypeOID::kFloat8:
      return {8, true, PgType::Typalign::Double,
              PgAttribute::Attstorage::Plain};
    case PgTypeOID::kDate:
      return {4, true, PgType::Typalign::Int, PgAttribute::Attstorage::Plain};
    case PgTypeOID::kTimestamp:
    case PgTypeOID::kTimestampTz:
      return {8, true, PgType::Typalign::Double,
              PgAttribute::Attstorage::Plain};
    case PgTypeOID::kUuid:
      return {16, false, PgType::Typalign::Char,
              PgAttribute::Attstorage::Plain};
    case PgTypeOID::kRegtype:
    case PgTypeOID::kRegclass:
    case PgTypeOID::kRegnamespace:
      return {4, true, PgType::Typalign::Int, PgAttribute::Attstorage::Plain};
    default:
      // Variable-length types (text, varchar, bytea, json, numeric, arrays)
      return {-1, false, PgType::Typalign::Int,
              PgAttribute::Attstorage::Extended};
  }
}

Oid GetCollationForType(int32_t type_oid) {
  switch (type_oid) {
    case PgTypeOID::kText:
    case PgTypeOID::kChar:
      return 100;  // default collation
    default:
      return 0;
  }
}

void EmitColumnsForTable(const duckdb::TableCatalogEntry& table,
                         duckdb::ClientContext& context,
                         std::vector<PgAttribute>& values) {
  const auto& columns = table.GetColumns();

  // NOT NULL is a constraint on the entry, keyed by logical column index; a
  // primary key implies it for every key column, as in postgres.
  containers::FlatHashSet<duckdb::idx_t> notnull_cols;
  for (const auto& constraint : table.GetConstraints()) {
    if (constraint->type == duckdb::ConstraintType::NOT_NULL) {
      notnull_cols.insert(
        constraint->Cast<duckdb::NotNullConstraint>().index.index);
    }
  }
  for (const auto key_column : connector::primary_key::KeyColumns(table)) {
    notnull_cols.insert(key_column.index);
  }

  for (const auto& col : columns.Logical()) {
    auto type_oid = Type2Oid(col.Type(), &context);
    auto phys = GetPhysicalInfo(type_oid);

    auto generated = PgAttribute::Attgenerated::None;
    if (col.Category() == duckdb::TableColumnType::GENERATED_STORED) {
      generated = PgAttribute::Attgenerated::Stored;
    } else if (col.Category() == duckdb::TableColumnType::GENERATED_VIRTUAL) {
      generated = PgAttribute::Attgenerated::Virtual;
    }

    PgAttribute row{
      .attrelid = table.oid,
      .attname = col.Name().GetIdentifierName(),
      .atttypid = type_oid,
      .attlen = phys.attlen,
      .attnum = static_cast<int16_t>(col.Logical().index + 1),
      .atttypmod = -1,
      .attndims = 0,
      .attbyval = phys.attbyval,
      .attalign = phys.attalign,
      .attstorage = phys.attstorage,
      .attcompression = PgAttribute::Attcompression::None,
      .attnotnull = notnull_cols.contains(col.Logical().index),
      // A generation expression is a default in pg_attrdef's sense, so both
      // shapes set atthasdef; duckdb keeps them apart.
      .atthasdef = col.HasDefaultValue() || col.Generated(),
      .atthasmissing = false,
      .attidentity = PgAttribute::Attidentity::None,
      .attgenerated = generated,
      .attisdropped = false,
      .attislocal = true,
      .attinhcount = 0,
      .attcollation = GetCollationForType(type_oid),
      .attacl = {catalog::ColumnAclOf(table.permissions, col.Oid())},
    };
    values.push_back(std::move(row));
  }
}

void EmitColumnsForSystemTable(const VirtualTable& table,
                               duckdb::ClientContext& context,
                               std::vector<PgAttribute>& values) {
  auto row_type = table.RowType();
  if (row_type.id() != duckdb::LogicalTypeId::STRUCT) {
    return;
  }
  auto& children = duckdb::StructType::GetChildTypes(row_type);

  for (size_t i = 0; i < children.size(); ++i) {
    auto& child_type = children[i].second;
    auto type_oid = Type2Oid(child_type, &context);
    auto phys = GetPhysicalInfo(type_oid);

    PgAttribute row{
      .attrelid = table.Id(),
      .attname = children[i].first.GetIdentifierName(),
      .atttypid = type_oid,
      .attlen = phys.attlen,
      .attnum = static_cast<int16_t>(i + 1),
      .atttypmod = -1,
      .attndims = 0,
      .attbyval = phys.attbyval,
      .attalign = phys.attalign,
      .attstorage = phys.attstorage,
      .attcompression = PgAttribute::Attcompression::None,
      .attnotnull = false,
      .atthasdef = false,
      .atthasmissing = false,
      .attidentity = PgAttribute::Attidentity::None,
      .attgenerated = PgAttribute::Attgenerated::None,
      .attisdropped = false,
      .attislocal = true,
      .attinhcount = 0,
      .attcollation = GetCollationForType(type_oid),
    };
    values.push_back(std::move(row));
  }
}

// Emit pg_attribute rows for composite (record) types so that drivers can
// introspect the field list via the standard `attrelid = $oid` lookup. The
// synthetic relid we use is the type's own OID (matching what pg_type.typrelid
// reports).
void EmitColumnsForCompositeType(const duckdb::TypeCatalogEntry& type,
                                 duckdb::ClientContext& context,
                                 std::vector<PgAttribute>& values) {
  if (type.user_type.id() != duckdb::LogicalTypeId::STRUCT) {
    return;
  }
  const auto& children = duckdb::StructType::GetChildTypes(type.user_type);
  const auto type_oid = type.oid;
  for (size_t i = 0; i < children.size(); ++i) {
    auto& child_type = children[i].second;
    auto type_id = Type2Oid(child_type, &context);
    auto phys = GetPhysicalInfo(type_id);
    PgAttribute row{
      .attrelid = type_oid,
      .attname = children[i].first.GetIdentifierName(),
      .atttypid = type_id,
      .attlen = phys.attlen,
      .attnum = static_cast<int16_t>(i + 1),
      .atttypmod = -1,
      .attndims = 0,
      .attbyval = phys.attbyval,
      .attalign = phys.attalign,
      .attstorage = phys.attstorage,
      .attcompression = PgAttribute::Attcompression::None,
      .attnotnull = false,
      .atthasdef = false,
      .atthasmissing = false,
      .attidentity = PgAttribute::Attidentity::None,
      .attgenerated = PgAttribute::Attgenerated::None,
      .attisdropped = false,
      .attislocal = true,
      .attinhcount = 0,
      .attcollation = GetCollationForType(type_id),
    };
    values.push_back(std::move(row));
  }
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgAttribute>::GetTableData() {
  std::vector<PgAttribute> values;

  auto& context = _context;
  VisitEntries<duckdb::TableCatalogEntry>(
    context, GetDatabase(), [&](const duckdb::TableCatalogEntry& table) {
      EmitColumnsForTable(table, context, values);
    });
  VisitEntries<duckdb::TypeCatalogEntry>(
    context, GetDatabase(), [&](const duckdb::TypeCatalogEntry& type) {
      EmitColumnsForCompositeType(type, context, values);
    });

  VisitSystemTables([&](const VirtualTable& table, Oid /*schema_oid*/) {
    EmitColumnsForSystemTable(table, context, values);
  });

  auto result = CreateColumns<PgAttribute>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
