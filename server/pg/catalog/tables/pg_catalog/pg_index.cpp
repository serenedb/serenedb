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

constexpr SystemIndex kIndexes[] = {
  {kPgIndexSql["indexrelid"], SystemLookup::Object},
  {kPgIndexSql["indrelid"], SystemLookup::Dependents},
};

struct KeyRow {
  const duckdb::TableCatalogEntry& table;
  const duckdb::UniqueConstraint& key;

  const duckdb::CatalogEntry& Relation() const { return table; }

  auto Attnums() const { return KeyAttnums(table.GetColumns(), key); }

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

template<typename R>
auto Collations(const duckdb::CatalogEntry& relation, R attnums, size_t keys) {
  const auto* table = relation.type == duckdb::CatalogType::TABLE_ENTRY
                        ? &relation.Cast<duckdb::TableCatalogEntry>()
                        : nullptr;
  return std::views::iota(size_t{0}, keys) |
         std::views::transform([table, attnums](size_t i) -> int64_t {
           if (!table || attnums[i] <= 0) {
             return 0;
           }
           return DescribeColumnType(
                    table->GetColumns()
                      .GetColumn(duckdb::LogicalIndex(attnums[i] - 1))
                      .Type())
             .collation;
         });
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
  }),
  Col<"indisvalid">([](const auto&) { return true; }),
  Col<"indisready">([](const auto&) { return true; }),
  Col<"indislive">([](const auto&) { return true; })};

class PgIndex final : public SystemTableScan<kPgIndexSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr duckdb::idx_t kRows = 10000;

  static constexpr std::tuple kSources{
    CatalogSource{kIndexTypes, SystemSchemas::Skip, kIndexes}};

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
    Col<"indimmediate">([](const auto&) { return true; }), kIndexColumns,
    Col<"indexprs">([](const auto& row) -> std::optional<std::string> {
      const auto attnums = row.Attnums();
      if (absl::c_none_of(attnums,
                          [](int16_t attnum) { return attnum == 0; })) {
        return std::nullopt;
      }
      const auto expressions = IndexExpressions(row.index);
      std::string texts;
      for (size_t i = 0; i < attnums.size(); ++i) {
        if (attnums[i] == 0) {
          absl::StrAppend(&texts, texts.empty() ? "" : ", ",
                          ExpressionText(*expressions[i]));
        }
      }
      return texts;
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
    if (auto relation = FindById(index.table_oid)) {
      Emit<kIndex>({index, *relation, Context()});
    }
  }
};

}  // namespace

SystemTable gPgIndex = SystemTableOf<PgIndex>();

}  // namespace sdb::pg
