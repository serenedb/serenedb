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

#include "app/app_server.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/user_type.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"
#include "pg/system_catalog.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgAttribute::attcompression),
  GetIndex(&PgAttribute::attstattarget),
  GetIndex(&PgAttribute::attacl),
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

void EmitColumnsForTable(const catalog::Table& table,
                         std::vector<PgAttribute>& values) {
  auto& columns = table.Columns();
  auto& pk_columns = table.PKColumns();

  // Collect NOT NULL column indices from check constraints.
  containers::FlatHashSet<size_t> notnull_cols;
  for (const auto& check : table.CheckConstraints()) {
    if (auto idx = check.IsNotNull(columns)) {
      notnull_cols.insert(*idx);
    }
  }

  for (size_t i = 0; i < columns.size(); ++i) {
    auto& col = columns[i];
    auto type_oid = Type2Oid(col.type);
    auto phys = GetPhysicalInfo(type_oid);

    bool is_pk = false;
    for (auto pk_id : pk_columns) {
      if (pk_id == col.GetId()) {
        is_pk = true;
        break;
      }
    }

    auto generated = PgAttribute::Attgenerated::None;
    if (col.generated_type == catalog::Column::GeneratedType::kStored) {
      generated = PgAttribute::Attgenerated::Stored;
    } else if (col.generated_type == catalog::Column::GeneratedType::kVirtual) {
      generated = PgAttribute::Attgenerated::Virtual;
    }

    PgAttribute row{
      .attrelid = table.GetId().id(),
      .attname = col.GetName(),
      .atttypid = type_oid,
      .attlen = phys.attlen,
      .attnum = static_cast<int16_t>(i + 1),
      .atttypmod = -1,
      .attndims = 0,
      .attbyval = phys.attbyval,
      .attalign = phys.attalign,
      .attstorage = phys.attstorage,
      .attcompression = PgAttribute::Attcompression::None,
      .attnotnull = is_pk || notnull_cols.contains(i),
      .atthasdef = col.expr != nullptr,
      .atthasmissing = false,
      .attidentity = PgAttribute::Attidentity::None,
      .attgenerated = generated,
      .attisdropped = false,
      .attislocal = true,
      .attinhcount = 0,
      .attcollation = GetCollationForType(type_oid),
    };
    values.push_back(std::move(row));
  }
}

void EmitColumnsForSystemTable(const catalog::VirtualTable& table,
                               std::vector<PgAttribute>& values) {
  auto row_type = table.RowType();
  if (row_type.id() != duckdb::LogicalTypeId::STRUCT) {
    return;
  }
  auto& children = duckdb::StructType::GetChildTypes(row_type);

  for (size_t i = 0; i < children.size(); ++i) {
    auto& child_type = children[i].second;
    auto type_oid = Type2Oid(child_type);
    auto phys = GetPhysicalInfo(type_oid);

    PgAttribute row{
      .attrelid = table.Id().id(),
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
void EmitColumnsForCompositeType(const catalog::PgSqlType& type,
                                 std::vector<PgAttribute>& values) {
  const auto& info = type.GetInfo();
  if (info.type.id() != duckdb::LogicalTypeId::STRUCT) {
    return;
  }
  const auto& children = duckdb::StructType::GetChildTypes(info.type);
  const auto type_oid = type.GetId().id();
  for (size_t i = 0; i < children.size(); ++i) {
    auto& child_type = children[i].second;
    auto type_id = Type2Oid(child_type);
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
catalog::MaterializedData SystemTableSnapshot<PgAttribute>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();

  std::vector<PgAttribute> values;

  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    for (const auto& table :
         catalog->GetTables(GetDatabaseId(), schema->GetName())) {
      EmitColumnsForTable(*table, values);
    }
    for (const auto& type :
         catalog->GetTypes(GetDatabaseId(), schema->GetName())) {
      EmitColumnsForCompositeType(*type, values);
    }
  }

  VisitSystemTables(
    [&](const catalog::VirtualTable& table, Oid /*schema_oid*/) {
      EmitColumnsForSystemTable(table, values);
    });

  auto result = CreateColumns<PgAttribute>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row);
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
