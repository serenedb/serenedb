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

#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

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
  mutable std::optional<ColumnType> described = std::nullopt;

  duckdb::idx_t Relid() const noexcept { return relid; }
  std::string_view Name() const noexcept { return name; }
  int16_t Number() const noexcept { return attnum; }
  const ColumnType& Described() const {
    if (!described) {
      described = DescribeColumnType(type);
    }
    return *described;
  }
};

struct TableAttribute {
  duckdb::idx_t relid;
  const duckdb::ColumnDefinition& column;
  const std::vector<bool>& not_null;
  mutable std::optional<ColumnType> described = std::nullopt;

  duckdb::idx_t Relid() const noexcept { return relid; }
  std::string_view Name() const noexcept {
    return column.Name().GetIdentifierName();
  }
  int16_t Number() const noexcept { return Attnum(column); }
  bool NotNull() const { return not_null[column.Logical().index]; }
  const ColumnType& Described() const {
    if (!described) {
      described = DescribeColumnType(column.Type());
    }
    return *described;
  }
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
  Col<"attrelid">([](const auto& row) { return row.Relid(); }),
  Col<"attname">([](const auto& row) { return row.Name(); }),
  Col<"attnum">([](const auto& row) { return row.Number(); }),
  Col<"atttypid">([](const auto& row) { return row.Described().oid; }),
  Col<"attlen">([](const auto& row) { return row.Described().len; }),
  Col<"atttypmod">([](const auto& row) { return row.Described().typmod; }),
  Col<"attndims">([](const auto& row) { return row.Described().ndims; }),
  Col<"attbyval">([](const auto& row) { return row.Described().byval; }),
  Col<"attalign">([](const auto& row) { return row.Described().align; }),
  Col<"attstorage">([](const auto& row) { return row.Described().storage; }),
  Col<"attcollation">(
    [](const auto& row) { return row.Described().collation; })};

class PgAttribute final : public SystemTableScan<kPgAttributeSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kRelationTypes, SystemSchemas::Visit, kIndexes}};

  static constexpr SystemFact kFacts[] = {
    {kSql["attnum"], 1, std::numeric_limits<int16_t>::max()},
    {kSql["attisdropped"], 0, 0},
  };

  static constexpr auto kColumn = Shape<kSql, const Attribute>(kAttribute);

  static constexpr auto kTableColumn = Shape<kSql, const TableAttribute>(
    kAttribute,
    Col<"attnotnull">([](const auto& row) { return row.NotNull(); }),
    Col<"atthasdef">([](const auto& row) { return HasAttrdef(row.column); }),
    Col<"attgenerated">([](const auto& row) { return Generated(row.column); }),
    Col<"attacl">(
      [](const auto& row) -> const auto& { return row.column.Acl(); }));

  static constexpr auto kSequenceColumn = Shape<kSql, const Attribute>(
    kAttribute, Col<"attnotnull">([](const auto&) { return true; }));

  void Row(duckdb::TableCatalogEntry& table) {
    _relations.emplace(table.oid, &table);
    if (Allows<"attrelid">(table.oid)) {
      const auto not_null =
        Reads<"attnotnull">() ? NotNullColumns(table) : std::vector<bool>{};
      for (const auto& column : table.GetColumns().Logical()) {
        Emit<kTableColumn>({table.oid, column, not_null});
      }
    }
    for (const auto& key : KeyIndexes(table)) {
      if (Allows<"attrelid">(key.index_oid)) {
        IndexColumns(key.index_oid, table,
                     Attnums(table.GetColumns(),
                             key.GetLogicalIndexes(table.GetColumns())));
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
      Emit<kColumn>({view.oid, view.ColumnName(*columns, i).GetIdentifierName(),
                     static_cast<int16_t>(i + 1), columns->types[i]});
    }
  }

  void Row(const duckdb::IndexCatalogEntry& index) {
    if (!Allows<"attrelid">(index.oid)) {
      return;
    }
    const auto it = _relations.find(index.table_oid);
    auto relation = it != _relations.end()
                      ? duckdb::optional_ptr<duckdb::CatalogEntry>{it->second}
                      : index.GetRelation(Transaction());
    if (relation) {
      IndexColumns(index.oid, *relation,
                   IndexAttnums(Context(), index, *relation));
    }
  }

  void Row(duckdb::SequenceCatalogEntry& sequence) {
    if (!Allows<"attrelid">(sequence.oid) || NumbersRows(sequence)) {
      return;
    }
    int16_t attnum = 0;
    for (const auto& [name, type] :
         {std::pair{"last_value", duckdb::LogicalType::BIGINT},
          std::pair{"log_cnt", duckdb::LogicalType::BIGINT},
          std::pair{"is_called", duckdb::LogicalType::BOOLEAN}}) {
      Emit<kSequenceColumn>({sequence.oid, name, ++attnum, type});
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
  void IndexColumns(duckdb::idx_t oid, duckdb::CatalogEntry& relation,
                    std::span<const int16_t> attnums) {
    auto* view = relation.type == duckdb::CatalogType::VIEW_ENTRY
                   ? &relation.Cast<duckdb::ViewCatalogEntry>()
                   : nullptr;
    const auto columns = view ? ViewColumns(Context(), *view) : nullptr;
    for (size_t i = 0; i < attnums.size(); ++i) {
      const auto column = static_cast<duckdb::idx_t>(attnums[i] - 1);
      const auto attnum = static_cast<int16_t>(i + 1);
      if (attnums[i] == 0) {
        Emit<kColumn>({oid, "expr", attnum, duckdb::LogicalType::VARCHAR});
      } else if (view) {
        Emit<kColumn>({oid,
                       view->ColumnName(*columns, column).GetIdentifierName(),
                       attnum, columns->types[column]});
      } else {
        const auto& definition =
          relation.Cast<duckdb::TableCatalogEntry>().GetColumn(
            duckdb::LogicalIndex{column});
        Emit<kColumn>({oid, definition.Name().GetIdentifierName(), attnum,
                       definition.Type()});
      }
    }
  }

  irs::containers::FlatHashMap<duckdb::idx_t, duckdb::CatalogEntry*> _relations;
};

}  // namespace

SystemTable gPgAttribute = SystemTableOf<PgAttribute>();

}  // namespace sdb::pg
