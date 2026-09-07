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

#include "pg/pg_catalog/pg_description.h"

#include <deque>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <string>
#include <vector>

#include "basics/down_cast.h"
#include "catalog1/entry/inverted_index.h"
#include "pg/pg_catalog/pg_class.h"
#include "pg/pg_catalog/pg_proc.h"
#include "pg/pg_catalog/pg_type.h"

namespace sdb::pg {
namespace {

// A row exists only where there is a comment, so no column of it is ever null.
constexpr uint64_t kNullMask = 0;

// The comment a CreateInfo carries: NULL and the empty string both mean none.
std::string_view InfoComment(const duckdb::Value& value) {
  return value.IsNull() ? std::string_view{}
                        : std::string_view{duckdb::StringValue::Get(value)};
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgDescription>::GetTableData() {
  auto& context = _context;
  auto& database = GetDatabase();

  std::vector<PgDescription> values;
  // A row borrows its text, and a definition the scan walks is only pinned for
  // the callback, so every comment is copied into storage that outlives the
  // write below.
  std::deque<std::string> text;

  const auto add = [&](Oid classoid, duckdb::idx_t objoid, int32_t objsubid,
                       std::string_view comment) {
    if (comment.empty()) {
      return;
    }
    text.emplace_back(comment);
    values.push_back(PgDescription{
      .objoid = objoid,
      .classoid = classoid,
      .objsubid = objsubid,
      .description = text.back(),
    });
  };

  VisitSchemas(context, database, [&](duckdb::SchemaCatalogEntry& schema_ref) {
    schema_ref.Scan(
      context, duckdb::CatalogType::TABLE_ENTRY,
      [&](duckdb::CatalogEntry& entry) {
        if (const auto* table =
              dynamic_cast<const duckdb::TableCatalogEntry*>(&entry)) {
          const auto id = (*table).oid;
          add(PgClass::kId, id, 0, InfoComment(table->comment));
          for (const auto& column : table->GetColumns().Logical()) {
            add(PgClass::kId, id,
                static_cast<int32_t>(column.Logical().index + 1),
                InfoComment(column.Comment()));
          }
          return;
        }
        auto* view_entry = dynamic_cast<duckdb::ViewCatalogEntry*>(&entry);
        if (view_entry == nullptr) {
          return;
        }
        const auto view_id = view_entry->oid;
        add(PgClass::kId, view_id, 0, InfoComment(view_entry->comment));
        const auto view_columns = view_entry->GetColumnInfo();
        if (!view_columns) {
          return;
        }
        for (size_t i = 0; i < view_columns->names.size(); ++i) {
          const auto comment = view_entry->GetColumnComment(i);
          if (!comment.IsNull()) {
            add(PgClass::kId, view_id, static_cast<int32_t>(i + 1),
                InfoComment(comment));
          }
        }
      });
  });

  VisitEntries<duckdb::DuckIndexEntry>(
    context, database, [&](const duckdb::DuckIndexEntry& entry) {
      add(PgClass::kId, entry.oid, 0, InfoComment(entry.comment));
    });

  VisitEntries<duckdb::SequenceCatalogEntry>(
    context, database, [&](const duckdb::SequenceCatalogEntry& seq) {
      add(PgClass::kId, seq.oid, 0, InfoComment(seq.comment));
    });

  VisitEntries<duckdb::TypeCatalogEntry>(
    context, database, [&](const duckdb::TypeCatalogEntry& type) {
      add(PgType::kId, type.oid, 0, InfoComment(type.comment));
    });

  const auto add_function = [&](const duckdb::MacroCatalogEntry& function) {
    add(PgProc::kId, function.oid, 0, InfoComment(function.comment));
  };
  VisitEntries<duckdb::ScalarMacroCatalogEntry>(context, database,
                                                add_function);
  VisitEntries<duckdb::TableMacroCatalogEntry>(context, database, add_function);

  auto result = CreateColumns<PgDescription>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
