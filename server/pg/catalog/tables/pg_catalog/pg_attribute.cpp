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

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <limits>
#include <optional>

#include "pg/catalog/tables/tables.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/types/mapping.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kIndexes[] = {
  {kPgAttributeSql["attrelid"], SystemLookup::Object},
};

struct Attribute {
  duckdb::idx_t relid;
  std::string_view name;
  int16_t attnum;
  const duckdb::LogicalType& type;
  const SystemSqlColumn* system = nullptr;
  mutable std::optional<ColumnType> described = std::nullopt;

  const ColumnType& Described() const {
    if (!described) {
      described = DescribeColumn(type, system);
    }
    return *described;
  }
};

struct TableAttribute : Attribute {
  const duckdb::ColumnDefinition& column;
  const std::vector<bool>& not_null;
};

char Generated(const duckdb::ColumnDefinition& column) {
  switch (column.Category()) {
    case duckdb::TableColumnType::STANDARD:
      return '\0';
    case duckdb::TableColumnType::GENERATED_VIRTUAL:
      return 'v';
    case duckdb::TableColumnType::GENERATED_STORED:
      return 's';
  }
}

constexpr std::tuple kAttribute{
  Col<"attrelid">(&Attribute::relid),
  Col<"attnum">(&Attribute::attnum),
  Col<"atttypid">([](const auto& row) { return row.Described().oid; }),
  Col<"attlen">([](const auto& row) { return row.Described().len; }),
  Col<"atttypmod">([](const auto& row) { return row.Described().typmod; }),
  Col<"attndims">([](const auto& row) { return row.Described().ndims; }),
  Col<"attbyval">([](const auto& row) { return row.Described().byval; }),
  Col<"attalign">([](const auto& row) { return row.Described().align; }),
  Col<"attstorage">([](const auto& row) { return row.Described().storage; }),
  Col<"attislocal">([](const auto&) { return true; }),
  Col<"attcollation">(
    [](const auto& row) { return row.Described().collation; })};

class PgAttribute final : public SystemTableScan<kPgAttributeSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr duckdb::idx_t kRows = 100000;

  static constexpr std::tuple kSources{
    CatalogSource{kRelationTypes, SystemSchemas::Visit, kIndexes}};

  static constexpr SystemFact kFacts[] = {
    {kSql["attnum"], 1, std::numeric_limits<int16_t>::max()},
    {kSql["attisdropped"], 0, 0},
  };

  static constexpr auto kColumn =
    Shape<kSql, const Attribute>(kAttribute, Col<"attname">(&Attribute::name));

  static constexpr auto kTableColumn = Shape<kSql, const TableAttribute>(
    kAttribute, Col<"attname">([](const auto& row) -> const auto& {
      return row.column.Name();
    }),
    Col<"attnotnull">(
      [](const auto& row) { return row.not_null[row.column.Logical().index]; }),
    Col<"atthasdef">([](const auto& row) { return HasAttrdef(row.column); }),
    Col<"attgenerated">([](const auto& row) { return Generated(row.column); }),
    Col<"attacl">(
      [](const auto& row) -> const auto& { return row.column.Acl(); }));

  static constexpr auto kSequenceColumn = Shape<kSql, const Attribute>(
    kAttribute, Col<"attname">(&Attribute::name),
    Col<"attnotnull">([](const auto&) { return true; }));

  static constexpr SystemTypeShape kTypeShapes[] = {
    {duckdb::CatalogType::TABLE_ENTRY, SystemShapeOf<kTableColumn>::kProvided},
    {duckdb::CatalogType::TABLE_ENTRY, SystemShapeOf<kColumn>::kProvided},
    {duckdb::CatalogType::VIEW_ENTRY, SystemShapeOf<kColumn>::kProvided},
    {duckdb::CatalogType::INDEX_ENTRY, SystemShapeOf<kColumn>::kProvided},
    {duckdb::CatalogType::SEQUENCE_ENTRY,
     SystemShapeOf<kSequenceColumn>::kProvided},
    {duckdb::CatalogType::TYPE_ENTRY, SystemShapeOf<kColumn>::kProvided},
  };

  void Row(duckdb::TableCatalogEntry& table) {
    _relations.emplace(table.oid, &table);
    if (Allows<"attrelid">(table.oid)) {
      const auto not_null =
        Reads<"attnotnull">() ? NotNullColumns(table) : std::vector<bool>{};
      const auto system = SystemColumns(table);
      for (const auto& column : table.GetColumns().Logical()) {
        Emit<kTableColumn>(
          {{table.oid,
            {},
            Attnum(column),
            column.Type(),
            system.empty() ? nullptr : &system[column.Logical().index]},
           column,
           not_null});
      }
    }
    for (const auto& key : KeyIndexes(table)) {
      if (Allows<"attrelid">(key.index_oid)) {
        IndexColumns(key.index_oid, table, KeyAttnums(table.GetColumns(), key),
                     {});
      }
    }
  }

  void Row(duckdb::ViewCatalogEntry& view) {
    _relations.emplace(view.oid, &view);
    if (!Allows<"attrelid">(view.oid)) {
      return;
    }
    const auto columns = ViewColumns(Context(), view);
    if (!columns) {
      return;
    }
    for (size_t i = 0; i < columns->types.size(); ++i) {
      Emit<kColumn>({view.oid, columns->names[i].GetIdentifierName(),
                     static_cast<int16_t>(i + 1), columns->types[i]});
    }
  }

  void Row(const duckdb::IndexCatalogEntry& index) {
    if (!Allows<"attrelid">(index.oid)) {
      return;
    }
    const auto it = _relations.find(index.table_oid);
    if (auto relation =
          it != _relations.end()
            ? duckdb::optional_ptr<duckdb::CatalogEntry>{it->second}
            : FindById(index.table_oid)) {
      IndexColumns(index.oid, *relation,
                   IndexAttnums(Context(), index, *relation),
                   IndexExpressions(index));
    }
  }

  void Row(duckdb::SequenceCatalogEntry& sequence) {
    if (!Allows<"attrelid">(sequence.oid) || NumbersRows(sequence)) {
      return;
    }
    static const std::array<duckdb::LogicalType, 3> kTypes{
      duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
      duckdb::LogicalType::BOOLEAN};
    for (size_t i = 0; i < kSequenceColumns.size(); ++i) {
      Emit<kSequenceColumn>({sequence.oid, kSequenceColumns[i],
                             static_cast<int16_t>(i + 1), kTypes[i]});
    }
  }

  void Row(const duckdb::TypeCatalogEntry& type) {
    if (!duckdb::StructType::IsStruct(type.user_type) ||
        !Allows<"attrelid">(type.oid)) {
      return;
    }
    const auto tuple = type.user_type.id() == duckdb::LogicalTypeId::TUPLE;
    const auto& children = duckdb::StructType::GetChildTypes(type.user_type);
    for (size_t i = 0; i < children.size(); ++i) {
      const auto attnum = static_cast<int16_t>(i + 1);
      if (tuple) {
        Emit<kColumn>({type.oid, duckdb::TupleType::GetChildName(i), attnum,
                       children[i].second});
      } else {
        Emit<kColumn>({type.oid, children[i].first.GetIdentifierName(), attnum,
                       children[i].second});
      }
    }
  }

 private:
  template<typename R>
  void IndexColumns(
    duckdb::idx_t oid, duckdb::CatalogEntry& relation, const R& attnums,
    std::span<const duckdb::ParsedExpression* const> expressions) {
    const auto columns =
      relation.type == duckdb::CatalogType::VIEW_ENTRY
        ? ViewColumns(Context(), relation.Cast<duckdb::ViewCatalogEntry>())
        : nullptr;
    const auto names =
      IndexAttributeNames(relation, columns.get(), attnums, expressions);
    int16_t attnum = 0;
    for (const int16_t key : attnums) {
      const auto& name = names[static_cast<size_t>(attnum)];
      if (key != 0) {
        Emit<kColumn>({oid, name, ++attnum,
                       IndexColumnAt(relation, columns.get(), key).type});
        continue;
      }
      const auto type = IndexExpressionType(
        Context(), relation, *expressions[static_cast<size_t>(attnum)]);
      Emit<kColumn>({oid, name, ++attnum, type});
    }
  }

  irs::containers::FlatHashMap<duckdb::idx_t, duckdb::CatalogEntry*> _relations;
};

}  // namespace

SystemTable gPgAttribute = SystemTableOf<PgAttribute>();

}  // namespace sdb::pg
