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

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <limits>

#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypes[] = {
  duckdb::CatalogType::SCHEMA_ENTRY,    duckdb::CatalogType::TABLE_ENTRY,
  duckdb::CatalogType::VIEW_ENTRY,      duckdb::CatalogType::INDEX_ENTRY,
  duckdb::CatalogType::SEQUENCE_ENTRY,  duckdb::CatalogType::TYPE_ENTRY,
  duckdb::CatalogType::MACRO_ENTRY,     duckdb::CatalogType::TABLE_MACRO_ENTRY,
  duckdb::CatalogType::TOKENIZER_ENTRY,
};

constexpr SystemKindTypes kClasses[] = {
  {kPgNamespaceTable, kSchemaTypes},
  {kPgClassTable, std::span{kSchemaObjectTypes}.first<4>()},
  {kPgTypeTable, kTypeTypes},
  {kPgProcTable, kFunctionTypes},
  {kPgTsDictTable, kTokenizerTypes},
};

constexpr SystemIndex kIndexes[] = {
  {kPgDescriptionSql["objoid"], SystemLookup::Object},
  {kPgDescriptionSql["classoid"], SystemLookup::Kind, kClasses},
};

struct Description {
  duckdb::idx_t objoid;
  duckdb::idx_t classoid;
  size_t objsubid;
  std::string_view text;
};

class PgDescription final : public SystemTableScan<kPgDescriptionSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kTypes, SystemSchemas::Skip, kIndexes}};

  static constexpr auto kDescription =
    Shape<kSql, const Description>(Col<"objoid">(&Description::objoid),
                                   Col<"classoid">(&Description::classoid),
                                   Col<"objsubid">(&Description::objsubid),
                                   Col<"description">(&Description::text));

  void Row(const duckdb::CatalogEntry& entry) {
    if (const auto classoid = CatalogClassOid(entry.type);
        classoid != kInvalidOid) {
      Comment(classoid, entry.oid, 0, entry.comment);
    }
  }

  void Row(const duckdb::TableCatalogEntry& table) {
    if (!Allows<"objoid">(table.oid)) {
      return;
    }
    Comment(kPgClassTable, table.oid, 0, table.comment);
    if (!Allows<"objsubid">(1, std::numeric_limits<int32_t>::max())) {
      return;
    }
    for (const auto& column : table.GetColumns().Logical()) {
      if (const auto* text = Text(column.Comment())) {
        Emit<kDescription>({table.oid, kPgClassTable,
                            static_cast<size_t>(Attnum(column)), *text});
      }
    }
  }

  void Row(duckdb::ViewCatalogEntry& view) {
    if (!Allows<"objoid">(view.oid)) {
      return;
    }
    Comment(kPgClassTable, view.oid, 0, view.comment);
    if (!Allows<"objsubid">(1, std::numeric_limits<int32_t>::max())) {
      return;
    }
    if (const auto columns = ViewColumns(Context(), view)) {
      for (size_t i = 0; i < columns->names.size(); ++i) {
        Comment(kPgClassTable, view.oid, i + 1, view.GetColumnComment(i));
      }
    }
  }

 private:
  static const std::string* Text(const duckdb::Value& comment) {
    if (comment.IsNull()) {
      return nullptr;
    }
    const auto& text = duckdb::StringValue::Get(comment);
    return text.empty() ? nullptr : &text;
  }

  void Comment(duckdb::idx_t classoid, duckdb::idx_t objoid, size_t objsubid,
               const duckdb::Value& comment) {
    if (const auto* text = Text(comment)) {
      Emit<kDescription>({objoid, classoid, objsubid, *text});
    }
  }
};

}  // namespace

SystemTable gPgDescription = SystemTableOf<PgDescription>();

}  // namespace sdb::pg
