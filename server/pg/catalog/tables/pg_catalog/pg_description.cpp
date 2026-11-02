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

#include "pg/catalog/tables/tables.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"

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
  int32_t objsubid;
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

  void Row(const duckdb::SchemaCatalogEntry& schema) {
    Comment(kPgNamespaceTable, schema.oid, 0, schema.comment);
  }

  void Row(const duckdb::IndexCatalogEntry& index) {
    Comment(kPgClassTable, index.oid, 0, index.comment);
  }

  void Row(const duckdb::SequenceCatalogEntry& sequence) {
    Comment(kPgClassTable, sequence.oid, 0, sequence.comment);
  }

  void Row(const duckdb::TypeCatalogEntry& type) {
    Comment(kPgTypeTable, type.oid, 0, type.comment);
  }

  void Row(const duckdb::MacroCatalogEntry& macro) {
    Comment(kPgProcTable, macro.oid, 0, macro.comment);
  }

  void Row(const catalog::TokenizerCatalogEntry& tokenizer) {
    Comment(kPgTsDictTable, tokenizer.oid, 0, tokenizer.comment);
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
      Comment(kPgClassTable, table.oid, Attnum(column), column.Comment());
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
        Comment(kPgClassTable, view.oid, static_cast<int32_t>(i + 1),
                view.GetColumnComment(i));
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

  void Comment(duckdb::idx_t classoid, duckdb::idx_t objoid, int32_t objsubid,
               const duckdb::Value& comment) {
    if (const auto* text = Text(comment)) {
      Emit<kDescription>({objoid, classoid, objsubid, *text});
    }
  }
};

}  // namespace

SystemTable gPgDescription = SystemTableOf<PgDescription>();

}  // namespace sdb::pg
