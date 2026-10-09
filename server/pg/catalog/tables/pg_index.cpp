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
#include <absl/strings/str_join.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypes[] = {duckdb::CatalogType::TABLE_ENTRY,
                                          duckdb::CatalogType::INDEX_ENTRY};

constexpr SystemIndex kIndexes[] = {
  {kPgIndexSql["indexrelid"], SystemLookup::Object},
  {kPgIndexSql["indrelid"], SystemLookup::Dependents},
};

struct KeyRow {
  const duckdb::TableCatalogEntry& table;
  const duckdb::UniqueConstraint& key;
  mutable std::optional<std::vector<int16_t>> attnums = std::nullopt;

  const duckdb::CatalogEntry& Relation() const { return table; }

  std::span<const int16_t> Attnums() const {
    if (!attnums) {
      attnums = pg::Attnums(table.GetColumns(),
                            key.GetLogicalIndexes(table.GetColumns()));
    }
    return *attnums;
  }

  size_t Keys() const { return Attnums().size(); }
};

struct IndexRow {
  const duckdb::IndexCatalogEntry& index;
  duckdb::CatalogEntry& relation;
  duckdb::ClientContext& context;
  mutable std::optional<std::vector<int16_t>> attnums = std::nullopt;

  const duckdb::CatalogEntry& Relation() const { return relation; }

  std::span<const int16_t> Attnums() const {
    if (!attnums) {
      attnums = IndexAttnums(context, index, relation);
    }
    return *attnums;
  }

  size_t Keys() const {
    return Attnums().size() -
           absl::c_count(index.column_opclasses, catalog::kIncludedKind);
  }
};

std::vector<int64_t> Collations(const duckdb::CatalogEntry& relation,
                                std::span<const int16_t> attnums, size_t keys) {
  std::vector<int64_t> collations(keys);
  if (relation.type != duckdb::CatalogType::TABLE_ENTRY) {
    return collations;
  }
  const auto& columns = relation.Cast<duckdb::TableCatalogEntry>().GetColumns();
  for (size_t i = 0; i < keys; ++i) {
    if (attnums[i] > 0) {
      collations[i] =
        DescribeColumnType(
          columns.GetColumn(duckdb::LogicalIndex(attnums[i] - 1)).Type())
          .collation;
    }
  }
  return collations;
}

constexpr std::tuple kIndexColumns{
  Col<"indnatts">([](const auto& row) { return row.Attnums().size(); }),
  Col<"indnkeyatts">([](const auto& row) { return row.Keys(); }),
  Col<"indkey">([](const auto& row) { return row.Attnums(); }),
  Col<"indcollation">([](const auto& row) {
    return Collations(row.Relation(), row.Attnums(), row.Keys());
  }),
  Col<"indclass">([](const auto& row) {
    return std::views::repeat(int64_t{0},
                              static_cast<std::ptrdiff_t>(row.Keys()));
  }),
  Col<"indoption">([](const auto& row) {
    return std::views::repeat(int16_t{0},
                              static_cast<std::ptrdiff_t>(row.Keys()));
  })};

class PgIndex final : public SystemTableScan<kPgIndexSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kTypes, SystemSchemas::Skip, kIndexes}};

  static constexpr auto kKey = Shape<kSql, const KeyRow>(
    Col<"indexrelid">([](const auto& row) { return row.key.index_oid; }),
    Col<"indrelid">([](const auto& row) { return row.table.oid; }),
    Col<"indisunique">([](const auto&) { return true; }),
    Col<"indisprimary">([](const auto& row) { return row.key.IsPrimaryKey(); }),
    Col<"indimmediate">([](const auto& row) { return !row.key.IsDeferred(); }),
    kIndexColumns);

  static constexpr auto kIndex = Shape<kSql, const IndexRow>(
    Col<"indexrelid">([](const auto& row) { return row.index.oid; }),
    Col<"indrelid">([](const auto& row) { return row.relation.oid; }),
    Col<"indisunique">([](const auto& row) { return row.index.IsUnique(); }),
    Col<"indisprimary">([](const auto& row) { return row.index.IsPrimary(); }),
    kIndexColumns,
    Col<"indexprs">([](const auto& row) -> std::optional<std::string> {
      const auto expressions = IndexExpressions(row.index);
      const auto attnums = row.Attnums();
      std::vector<std::string> texts;
      for (size_t i = 0; i < attnums.size(); ++i) {
        if (attnums[i] == 0) {
          texts.emplace_back(ExpressionText(*expressions[i]));
        }
      }
      if (texts.empty()) {
        return std::nullopt;
      }
      return absl::StrJoin(texts, ", ");
    }),
    Col<"indpred">([](const auto& row) -> std::optional<std::string> {
      if (!row.index.where_clause) {
        return std::nullopt;
      }
      return ExpressionText(*row.index.where_clause);
    }));

  void Row(const duckdb::TableCatalogEntry& table) {
    if (!Allows<"indrelid">(table.oid)) {
      return;
    }
    for (const auto& key : KeyIndexes(table)) {
      Emit<kKey>({table, key});
    }
  }

  void Row(const duckdb::IndexCatalogEntry& index) {
    if (!Allows<"indexrelid">(index.oid)) {
      return;
    }
    if (auto relation = index.GetRelation(Transaction())) {
      Emit<kIndex>({index, *relation, Context()});
    }
  }
};

}  // namespace

SystemTable gPgIndex = SystemTableOf<PgIndex>();

}  // namespace sdb::pg
