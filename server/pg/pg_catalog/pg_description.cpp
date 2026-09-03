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
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <string>
#include <vector>

#include "basics/down_cast.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/index.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/schema.h"
#include "catalog/sequence.h"
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
catalog::MaterializedData SystemTableSnapshot<PgDescription>::GetTableData() {
  auto& context = _config.GetClientContext();
  const auto database_id = GetDatabaseId();

  std::vector<PgDescription> values;
  // A row borrows its text, and a definition the scan walks is only pinned for
  // the callback, so every comment is copied into storage that outlives the
  // write below.
  std::deque<std::string> text;

  const auto add = [&](Oid classoid, ObjectId objoid, int32_t objsubid,
                       std::string_view comment) {
    if (comment.empty()) {
      return;
    }
    text.emplace_back(comment);
    values.push_back(PgDescription{
      .objoid = objoid.id(),
      .classoid = classoid,
      .objsubid = objsubid,
      .description = text.back(),
    });
  };

  catalog::VisitCatalogSetEntries(
    context, database_id, duckdb::CatalogType::TABLE_ENTRY,
    [&](const catalog::SereneDBSchemaEntry&, duckdb::CatalogEntry& entry) {
      if (const auto* table =
            dynamic_cast<const catalog::SereneDBTableEntry*>(&entry)) {
        const auto id = catalog::IdOf(*table);
        add(PgClass::kId, id, 0, InfoComment(table->comment));
        for (const auto& column : table->GetColumns().Logical()) {
          add(PgClass::kId, id,
              static_cast<int32_t>(column.Logical().index + 1),
              InfoComment(column.Comment()));
        }
        return;
      }
      const auto* view_entry =
        dynamic_cast<const catalog::SereneDBViewEntry*>(&entry);
      if (view_entry == nullptr) {
        return;
      }
      const auto view_id = ObjectId{view_entry->oid};
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

  catalog::Visit<catalog::SereneDBIndexEntry>(
    &context, database_id, [&](const catalog::SereneDBIndexEntry& entry) {
      add(PgClass::kId, ObjectId{entry.oid}, 0, InfoComment(entry.comment));
    });

  catalog::Visit<catalog::SereneDBSequenceEntry>(
    &context, database_id, [&](const catalog::SereneDBSequenceEntry& seq) {
      add(PgClass::kId, ObjectId{seq.oid}, 0, seq.Comment());
    });

  catalog::Visit<catalog::SereneDBTypeEntry>(
    &context, database_id, [&](const duckdb::TypeCatalogEntry& type) {
      add(PgType::kId, ObjectId{type.oid}, 0, InfoComment(type.comment));
    });

  catalog::VisitFunctions(&context, database_id,
                          [&](const duckdb::MacroCatalogEntry& function) {
                            add(PgProc::kId, ObjectId{function.oid}, 0,
                                InfoComment(function.comment));
                          });

  auto result = CreateColumns<PgDescription>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
