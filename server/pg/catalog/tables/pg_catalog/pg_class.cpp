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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/storage/data_table.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/shared.hpp>
#include <optional>
#include <string>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "catalog/entry/system_table.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/functions/reg_types.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kViewTypes[] = {duckdb::CatalogType::TABLE_ENTRY,
                                              duckdb::CatalogType::VIEW_ENTRY};
constexpr duckdb::CatalogType kIndexTypes[] = {
  duckdb::CatalogType::TABLE_ENTRY, duckdb::CatalogType::INDEX_ENTRY};
constexpr duckdb::CatalogType kSequenceTypes[] = {
  duckdb::CatalogType::SEQUENCE_ENTRY};
constexpr duckdb::CatalogType kCompositeTypes[] = {
  duckdb::CatalogType::TYPE_ENTRY};

constexpr SystemKindTypes kRelkinds[] = {
  {'r', kTableTypes},    {'v', kViewTypes},      {'i', kIndexTypes},
  {'S', kSequenceTypes}, {'c', kCompositeTypes},
};

constexpr SystemIndex kIndexes[] = {
  {kPgClassSql["oid"], SystemLookup::Object},
  {kPgClassSql["relname"], SystemLookup::Object},
  {kPgClassSql["relnamespace"], SystemLookup::Namespace},
  {kPgClassSql["relkind"], SystemLookup::Kind, kRelkinds},
};

std::optional<std::vector<std::string>> Reloptions(
  const catalog::SearchTableEntry& table) {
  const auto info = table.GetInfo();
  const auto& options = info->Cast<duckdb::CreateTableInfo>().options;
  std::vector<std::string> result;
  for (const auto name : catalog::kSearchTableOptions) {
    if (const auto it = options.find(name); it != options.end()) {
      result.emplace_back(
        absl::StrCat(name, "=",
                     it->second->Cast<duckdb::ConstantExpression>()
                       .GetLiteral()
                       .ToValue()
                       .ToString()));
    }
  }
  return NonEmpty(std::move(result));
}

std::optional<std::vector<std::string>> Reloptions(
  const duckdb::IndexCatalogEntry& index) {
  std::vector<std::string> result;
  for (const auto name : catalog::kInvertedIndexSettings) {
    if (const auto it = index.options.find(name); it != index.options.end()) {
      result.emplace_back(absl::StrCat(name, "=", it->second.ToString()));
    }
  }
  return NonEmpty(std::move(result));
}

uint64_t RelationRowType(const duckdb::CatalogEntry& relation) {
  if (const auto* row = relation.oid < kMaxSystem
                          ? FindBuiltinRowType(relation.oid)
                          : nullptr) {
    return row->oid;
  }
  return relation.internal ? kInvalidOid : RowTypeOid(relation.oid);
}

bool Indexed(SystemScan& scan, duckdb::CatalogEntry& relation) {
  if (const auto known = scan.KnownIndexed(relation.oid)) {
    return *known;
  }
  bool indexed = false;
  scan.Dependencies().ScanEdges(
    scan.Transaction(), relation, false,
    [&](duckdb::CatalogEntry& dependent,
        const duckdb::DependencyDependentFlags&) {
      indexed = indexed || dependent.type == duckdb::CatalogType::INDEX_ENTRY;
    });
  return indexed;
}

constexpr std::tuple kRelationBase{
  Col<"oid">(&duckdb::CatalogEntry::oid),
  Col<"relname">(&duckdb::CatalogEntry::name),
  Col<"relnamespace">(&duckdb::CatalogEntry::ParentSchemaOid),
  Col<"relowner">(kOwner)};

template<char Kind>
constexpr auto kRelation = std::tuple_cat(
  kRelationBase, std::tuple{Col<"relkind">([](const auto&) { return Kind; })});

struct Ordinary {
  duckdb::TableCatalogEntry& table;
  const catalog::SearchTableEntry* search;
};

struct Index {
  const duckdb::IndexCatalogEntry& index;
  const irs::containers::FlatHashMap<duckdb::idx_t, duckdb::idx_t>& owners;
};

struct KeyIndex {
  const duckdb::TableCatalogEntry& table;
  const duckdb::UniqueConstraint& key;
};

class PgClass final : public SystemTableScan<kPgClassSql> {
 public:
  PgClass(duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input)
    : SystemTableScan{context, input} {
    if (Needs<"relhasindex">()) {
      CollectIndexed();
    }
    if (Needs<"relhastriggers">()) {
      CollectTriggered();
    }
    if (VisibleOnly()) {
      _session = MakeSession(&context);
    }
  }

  static constexpr std::tuple kSources{
    CatalogSource{kRelationTypes, SystemSchemas::Visit, kIndexes}};

  static constexpr SystemVisibility kVisibility{kSql["oid"],
                                                "pg_table_is_visible"};

  static constexpr auto kSystem = Shape<kSql, const catalog::SystemTableEntry>(
    kRelationBase, Col<"relkind">([](const auto& entry) {
      return entry.Table().Sql().relkind;
    }),
    Col<"relam">([](const auto& entry) {
      return entry.Table().Sql().relkind == 'v' ? duckdb::idx_t{0} : kPgAmHeap;
    }),
    Col<"relhasrules">(
      [](const auto& entry) { return entry.Table().Sql().relkind == 'v'; }),
    Col<"relnatts">(
      [](const auto& entry) { return entry.Table().Sql().columns.size(); }),
    Col<"relisshared">(
      [](const auto& entry) { return entry.Table().Sql().shared; }),
    Col<"reltablespace">([](const auto& entry) {
      return entry.Table().Sql().shared ? kPgGlobalTablespace
                                        : duckdb::idx_t{0};
    }),
    Col<"reltype">(&RelationRowType), Col<"relacl">(kAcl));

  static constexpr auto kOrdinary = Shape<kSql, const Ordinary>(
    Col<"oid">([](const auto& row) { return row.table.oid; }),
    Col<"relname">(
      [](const auto& row) -> const auto& { return row.table.name; }),
    Col<"relnamespace">(
      [](const auto& row) { return row.table.ParentSchemaOid(); }),
    Col<"relowner">(
      [](const auto& row) { return row.table.permissions.owner; }),
    Col<"relam">(
      [](const auto& row) { return row.search ? kPgAmIResearch : kPgAmHeap; }),
    Col<"relnatts">([](const auto& row) {
      return row.table.GetColumns().LogicalColumnCount();
    }),
    Col<"relreplident">([](const auto&) { return 'd'; }),
    Col<"reltype">([](const auto& row) { return RelationRowType(row.table); }),
    Col<"reloptions">(
      [](const auto& row) -> std::optional<std::vector<std::string>> {
        if (!row.search) {
          return std::nullopt;
        }
        return Reloptions(*row.search);
      }),
    Col<"relchecks">([](const auto& row) {
      return absl::c_count_if(
        row.table.GetConstraints(), [](const auto& constraint) {
          return constraint->type == duckdb::ConstraintType::CHECK;
        });
    }),
    Col<"relhasindex">([](const auto& row, SystemScan& scan) {
      return !KeyIndexes(row.table).empty() || Indexed(scan, row.table);
    }),
    Col<"relhastriggers">([](const auto& row, SystemScan& scan) {
      if (absl::c_any_of(
            row.table.GetConstraints(), [](const auto& constraint) {
              return constraint->type == duckdb::ConstraintType::FOREIGN_KEY;
            })) {
        return true;
      }
      if (const auto known = scan.KnownTriggered(row.table.oid)) {
        return *known;
      }
      bool triggers = false;
      row.table.ScanTriggers(scan.Transaction(),
                             [&](duckdb::CatalogEntry&) { triggers = true; });
      return triggers;
    }),
    Col<"reltuples">([](const auto& row) {
      return row.table.IsDuckTable()
               ? static_cast<float>(row.table.GetStorage().GetTotalRows())
               : -1.0F;
    }),
    Col<"relacl">([](const auto& row) -> const auto& {
      return row.table.permissions.acl;
    }));

  static constexpr auto kKeyIndex = Shape<kSql, const KeyIndex>(
    Col<"oid">([](const auto& row) { return row.key.index_oid; }),
    Col<"relname">(
      [](const auto& row) -> const auto& { return row.key.constraint_name; }),
    Col<"relnamespace">(
      [](const auto& row) { return row.table.ParentSchemaOid(); }),
    Col<"relowner">(
      [](const auto& row) { return row.table.permissions.owner; }),
    Col<"relkind">([](const auto&) { return 'i'; }),
    Col<"relam">([](const auto&) { return kPgAmSecondary; }),
    Col<"reltuples">([](const auto&) { return 0.0F; }),
    Col<"relnatts">([](const auto& row) {
      return row.key.GetLogicalIndexes(row.table.GetColumns()).size();
    }));

  static constexpr auto kIndex = Shape<kSql, const Index>(
    Col<"oid">([](const auto& row) { return row.index.oid; }),
    Col<"relname">(
      [](const auto& row) -> const auto& { return row.index.name; }),
    Col<"relnamespace">(
      [](const auto& row) { return row.index.ParentSchemaOid(); }),
    Col<"relkind">([](const auto&) { return 'i'; }),
    Col<"relam">([](const auto& row) {
      return row.index.index_type == catalog::kInvertedIndexTypeName
               ? kPgAmInverted
               : kPgAmSecondary;
    }),
    Col<"reltuples">([](const auto&) { return 0.0F; }),
    Col<"relnatts">(
      [](const auto& row) { return row.index.parsed_expressions.size(); }),
    Col<"reloptions">(
      [](const auto& row) -> std::optional<std::vector<std::string>> {
        if (row.index.index_type != catalog::kInvertedIndexTypeName) {
          return std::nullopt;
        }
        return Reloptions(row.index);
      }),
    Col<"relowner">([](const auto& row, SystemScan& scan) {
      if (const auto it = row.owners.find(row.index.table_oid);
          it != row.owners.end()) {
        return it->second;
      }
      const auto relation = row.index.GetRelation(scan.Transaction());
      return relation ? relation->permissions.owner
                      : row.index.permissions.owner;
    }));

  static constexpr auto kView = Shape<kSql, duckdb::ViewCatalogEntry>(
    kRelation<'v'>, Col<"relhasrules">([](const auto&) { return true; }),
    Col<"reltype">(&RelationRowType),
    Col<"relnatts">([](auto& view, SystemScan& scan) {
      const auto columns = ViewColumns(scan.Context(), view);
      return columns ? columns->types.size() : 0;
    }),
    Col<"relhasindex">([](auto& view, SystemScan& scan) {
      return !view.internal && Indexed(scan, view);
    }),
    Col<"relacl">(kAcl));

  static constexpr auto kSequence =
    Shape<kSql, const duckdb::SequenceCatalogEntry>(
      kRelation<'S'>, Col<"relnatts">([](const auto&) { return 3; }),
      Col<"reltuples">([](const auto&) { return 1.0F; }), Col<"relacl">(kAcl));

  static constexpr auto kComposite =
    Shape<kSql, const duckdb::TypeCatalogEntry>(
      kRelation<'c'>, Col<"reltype">(&duckdb::CatalogEntry::oid),
      Col<"relnatts">([](const auto& type) {
        return duckdb::StructType::GetChildCount(type.user_type);
      }));

  void Row(duckdb::TableCatalogEntry& table) {
    if (table.internal) {
      if (const auto* system =
            dynamic_cast<const catalog::SystemTableEntry*>(&table)) {
        if (Visible(table, table.name.GetIdentifierName())) {
          Emit<kSystem>(*system);
        }
        return;
      }
    }
    const bool emitted =
      Visible(table, table.name.GetIdentifierName()) &&
      Emit<kOrdinary>(
        {table, dynamic_cast<const catalog::SearchTableEntry*>(&table)});
    if (Allows<"relkind">('i')) {
      if (emitted && Needs<"relowner">()) {
        _owners.emplace(table.oid, table.permissions.owner);
      }
      for (const auto& key : KeyIndexes(table)) {
        if (Visible(table, key.constraint_name)) {
          Emit<kKeyIndex>({table, key});
        }
      }
    }
  }

  void Row(duckdb::ViewCatalogEntry& view) {
    if (Visible(view, view.name.GetIdentifierName())) {
      Emit<kView>(view);
    }
  }

  void Row(const duckdb::IndexCatalogEntry& index) {
    if (Visible(index, index.name.GetIdentifierName())) {
      Emit<kIndex>({index, _owners});
    }
  }

  void Row(duckdb::SequenceCatalogEntry& sequence) {
    if (!NumbersRows(sequence) &&
        Visible(sequence, sequence.name.GetIdentifierName())) {
      Emit<kSequence>(sequence);
    }
  }

  void Row(const duckdb::TypeCatalogEntry& type) {
    if (duckdb::StructType::IsStruct(type.user_type) &&
        Visible(type, type.name.GetIdentifierName())) {
      Emit<kComposite>(type);
    }
  }

 private:
  bool Visible(const duckdb::CatalogEntry& owner,
               const std::string& name) const {
    if (!_session) [[likely]] {
      return true;
    }
    return Shown(owner, name);
  }

  IRS_NO_INLINE bool Shown(const duckdb::CatalogEntry& owner,
                           std::string_view name) const {
    const auto schema = owner.ParentSchemaName();
    return RelationVisible(*_session, schema.GetIdentifierName(), name);
  }

  irs::containers::FlatHashMap<duckdb::idx_t, duckdb::idx_t> _owners;
  std::optional<Session> _session;
};

}  // namespace

SystemTable gPgClass = SystemTableOf<PgClass>();

}  // namespace sdb::pg
