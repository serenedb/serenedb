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

#include "docs/docs_loader.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/query_result.hpp>
#include <exception>
#include <filesystem>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/cluster.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/duckdb_client_state.h"
#include "docs/docs_data.h"
#include "docs/docs_index_data.h"
#include "docs/docs_search.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "search/search_table.h"
#include "utils/file_utils.h"

namespace sdb::docs {
namespace {

constexpr std::string_view kSchema = "sdb_docs_build";
constexpr std::string_view kTable = "sdb_docs_build.docs";
constexpr size_t kInsertBatch = 32;

constexpr std::string_view kStripLinksToken = "@striplinks@";
constexpr std::string_view kStripLinks =
  R"sql('\[((?:[^\[\]]|\[(?:[^\[\]]|\[[^\[\]]*\])*\])*)\]\([^)]*\)', '\1', 'g')sql";

std::string Sql(std::string_view statement) {
  return absl::StrReplaceAll(statement, {{kStripLinksToken, kStripLinks}});
}

constexpr std::string_view kBuildSql = R"sql(
CREATE SCHEMA sdb_docs_build;

CREATE TEXT SEARCH DICTIONARY sdb_docs_build.tokenizer
  AS split_text(case := 'lower', break := 'alpha')
  WITH (frequency, position);

CREATE TABLE sdb_docs_build.docs (
  path TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  breadcrumb TEXT NOT NULL,
  content TEXT NOT NULL USING COMPRESSION zstd
) WITH (storage = 'search', refresh_interval = 0, compaction_interval = 0);

CREATE INDEX docs_fts ON sdb_docs_build.docs USING inverted (
  title sdb_docs_build.tokenizer,
  breadcrumb sdb_docs_build.tokenizer,
  (md_to_text(content)) sdb_docs_build.tokenizer,
  path);
)sql";

constexpr std::string_view kObjectsSql = R"sql(
WITH d AS (
  SELECT path, title, breadcrumb, content, split_part(path, '#', 1) AS page,
         path = split_part(path, '#', 1) || '#' ||
                replace(replace(title, '#', '\#'), ' ', '_') AS is_title_row,
         position('#' IN path) = 0 AS is_whole_page,
         regexp_replace(coalesce(
           nullif(regexp_replace(trim(regexp_replace(
             split_part(regexp_replace(ltrim(content),
                                       '^# [^' || chr(10) || ']*' || chr(10) || '+', ''),
                        chr(10) || chr(10), 1),
             '^(\||#|```|> |[-*+] |[0-9]+\. )(?s:.*)', '')), '\s+', ' ', 'g'), ''),
           nullif(trim(regexp_replace(
             regexp_extract(content, '\|\s*\*\*Description\*\*\s*\|([^|]*)\|', 1),
             '\s+', ' ', 'g')), '')), @striplinks@) AS summary
  FROM sdb_docs_build.docs
),
tables AS (
  SELECT page, path, breadcrumb,
         unnest(md_extract_tables_json(content)) AS tbl
  FROM d
  WHERE (is_title_row OR is_whole_page)
    AND (starts_with(page, 'sql/') OR starts_with(page, 'data_import_and_export/')
         OR page = 'configuration/overview.md')
),
table_rows AS (
  SELECT page, path, breadcrumb, tbl.headers AS headers,
         unnest(tbl.table_data) AS cells
  FROM tables
),
named AS (
  SELECT page, path, breadcrumb, headers, headers[1] AS first_header, name,
         nullif(regexp_replace(coalesce(cells[list_position(headers, 'Aliases')],
                                        cells[list_position(headers, 'Alias')]),
                               @striplinks@), '') AS aliases,
         regexp_replace(coalesce(cells[list_position(headers, 'Description')],
                                 cells[list_position(headers, 'Purpose')]),
                        @striplinks@) AS summary,
         coalesce(nullif(regexp_extract(name, '^([A-Za-z_][A-Za-z0-9_]*)\(', 1), ''),
                  name) AS bare
  FROM (SELECT *,
               CASE WHEN headers[1] = 'Index'
                    THEN regexp_extract(cells[1], '\]\(\.?/?([^)]*?)(?:/index)?\.mdx?\)', 1)
                    ELSE regexp_replace(cells[1], @striplinks@) END AS name
        FROM table_rows)
),
fn_rows AS (
  SELECT page, path, breadcrumb, name, aliases, summary
  FROM named
  WHERE CASE WHEN starts_with(page, 'sql/functions/')
                  OR (starts_with(page, 'sql/data_types/')
                      AND page <> 'sql/data_types/index.md')
             THEN headers[1] IN ('Function', 'Aggregate', 'Name')
             ELSE headers[1] IN ('Function', 'Aggregate')
                  AND starts_with(page, 'data_import_and_export/') END
),
fn_union AS (
  SELECT 1 AS pref,
         regexp_extract(title, '^(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\(', 1) AS name,
         title AS signature, summary, NULL AS aliases,
         path AS path, page AS page, breadcrumb AS breadcrumb
  FROM d WHERE starts_with(page, 'sql/functions/') AND position('#' IN path) > 0
     AND regexp_matches(title, '^(?:[A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*\(')
  UNION ALL
  SELECT 2,
         regexp_extract(name, '^(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)', 1),
         name, summary, aliases, path, page, breadcrumb
  FROM fn_rows
  WHERE regexp_matches(name, '^[A-Za-z_][A-Za-z0-9_]*$|^(?:[A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*\s*\(')
    AND NOT regexp_matches(name, '^[A-Z][A-Z0-9_ ]*$')
),
fn_merged AS (
  SELECT name, signature, page,
         arg_min(summary, pref) FILTER (WHERE nullif(trim(summary), '') IS NOT NULL) AS summary,
         arg_min(aliases, pref) FILTER (WHERE aliases IS NOT NULL) AS aliases,
         arg_min({'path': path, 'breadcrumb': breadcrumb}, pref) AS origin
  FROM fn_union GROUP BY name, signature, page
),
cmd_rows AS (
  SELECT path, page, breadcrumb, tbl.headers AS headers,
         unnest(tbl.table_data) AS cells
  FROM (SELECT path, page, breadcrumb,
               unnest(md_extract_tables_json(content)) AS tbl
        FROM d WHERE page = 'clients/serened-shell.md' AND title = 'Dot commands')
),
commands AS (
  SELECT regexp_replace(cells[1], @striplinks@) AS name,
         nullif(trim(cells[2]), '') AS arguments,
         regexp_replace(cells[3], @striplinks@) AS summary,
         path, page, breadcrumb
  FROM cmd_rows WHERE headers[1] = 'Command'
)
SELECT 'function' AS kind, name, signature, summary, aliases, origin.path, page,
       regexp_replace(regexp_replace(page, '^sql/functions/', ''), '(/index)?\.mdx?$', '') AS category,
       origin.breadcrumb
FROM fn_merged
UNION ALL
SELECT 'statement', title, title, summary, NULL, path, page, NULL, breadcrumb
FROM d WHERE starts_with(page, 'sql/statements/') AND is_title_row
   AND NOT starts_with(page, 'sql/statements/create_text_search_dictionary/')
UNION ALL
SELECT 'tokenizer', title, title, summary, NULL, path, page, NULL, breadcrumb
FROM d WHERE starts_with(page, 'sql/statements/create_text_search_dictionary/') AND is_title_row
   AND page <> 'sql/statements/create_text_search_dictionary/index.md'
UNION ALL
SELECT 'type', bare, name, summary, aliases, path, page, NULL, breadcrumb
FROM named WHERE page = 'sql/data_types/index.md' AND first_header = 'Name'
UNION ALL
SELECT 'setting', bare, name, summary, aliases, path, page, NULL, breadcrumb
FROM named WHERE page = 'configuration/overview.md' AND first_header = 'Name'
UNION ALL
SELECT 'index_type', bare, name, summary, NULL, path, page, NULL, breadcrumb
FROM named WHERE page = 'sql/indexes/index.md' AND first_header = 'Index'
UNION ALL
SELECT 'command', name, concat_ws(' ', name, arguments), summary,
       nullif(ltrim(name, '.'), ''), path, page, NULL, breadcrumb
FROM commands
ORDER BY kind, name, signature, path
)sql";

std::optional<std::vector<IndexBlob>> ExportImage(
  duckdb::ClientContext& context, std::string_view database);

class Loader {
 public:
  Loader(std::string_view database, duckdb::idx_t database_id)
    : _conn{irs::DuckDBEngine::Instance().CreateConnection()},
      _ctx{std::make_shared<ConnectionContext>(
        *_conn->context, irs::StaticStrings::kDefaultUser, pg::kRootUser,
        database, database_id, nullptr, 0, nullptr)} {
    connector::SereneDBClientState::Register(*_conn->context, _ctx);
    _conn->context->session_user =
      std::string{irs::StaticStrings::kDefaultUser};
    std::vector<duckdb::CatalogSearchEntry> paths{
      duckdb::CatalogSearchEntry{duckdb::Identifier{std::string{database}},
                                 duckdb::Identifier{"$user"}},
      duckdb::CatalogSearchEntry{duckdb::Identifier{std::string{database}},
                                 duckdb::Identifier{"public"}},
    };
    _conn->context->client_data->catalog_search_path->SetDefaultPaths(
      std::vector{paths});
    _conn->context->client_data->catalog_search_path->Set(
      std::move(paths), duckdb::CatalogSetPathType::SET_DIRECTLY);
  }

  ~Loader() {
    _ctx->ConsumeNotices([](auto& notice) {
      SDB_INFO(STARTUP, "embedded docs: ", notice.errmsg);
    });
  }

  std::optional<std::vector<IndexBlob>> Build() {
    if (!Run(Sql(kBuildSql)) || !Insert() ||
        !Run(absl::StrCat("VACUUM (REFRESH_TABLE) ", kTable))) {
      return std::nullopt;
    }
    auto objects = Catalog();
    auto image = ExportImage(*_conn->context, _ctx->GetDatabase());
    if (!objects || !image) {
      return std::nullopt;
    }
    image->push_back(
      {.name = std::string{kObjectsFile}, .bytes = std::move(*objects)});
    return image;
  }

  bool Run(const std::string& sql) {
    auto result = _conn->Query(sql);
    if (!result->HasError()) {
      return true;
    }
    SDB_WARN(GENERAL, "embedded docs: '", sql,
             "' failed: ", result->GetError());
    return false;
  }

 private:
  static constexpr size_t kInsertColumns = 4;

  std::optional<std::string> Catalog() {
    auto result = _conn->Query(Sql(kObjectsSql));
    if (result->HasError()) {
      SDB_WARN(GENERAL,
               "embedded docs: object catalog failed: ", result->GetError());
      return std::nullopt;
    }
    std::vector<Object> objects;
    objects.reserve(result->RowCount());
    for (size_t row = 0; row < result->RowCount(); ++row) {
      ObjectRow fields;
      for (size_t column = 0; column < fields.size(); ++column) {
        if (const auto value = result->GetValue(column, row); !value.IsNull()) {
          fields[column] = duckdb::StringValue::Get(value);
        }
      }
      objects.push_back(ObjectFromFields(std::move(fields)));
    }
    return EncodeObjects(objects);
  }

  duckdb::unique_ptr<duckdb::PreparedStatement> PrepareInsert(size_t rows) {
    std::string sql = absl::StrCat("INSERT INTO ", kTable, " VALUES ");
    for (size_t i = 0; i < rows; ++i) {
      const auto first = kInsertColumns * i + 1;
      absl::StrAppend(&sql, i == 0 ? "(" : ", (");
      for (size_t column = 0; column < kInsertColumns; ++column) {
        absl::StrAppend(&sql, column == 0 ? "$" : ", $", first + column);
      }
      sql.push_back(')');
    }
    auto prepared = _conn->Prepare(sql);
    if (prepared->HasError()) {
      SDB_WARN(GENERAL,
               "embedded docs: prepare insert failed: ", prepared->GetError());
      return nullptr;
    }
    return prepared;
  }

  bool Insert() {
    const auto docs = GetDocs();
    auto full = PrepareInsert(kInsertBatch);
    if (!full) {
      return false;
    }
    for (size_t begin = 0; begin < docs.size(); begin += kInsertBatch) {
      const auto batch =
        docs.subspan(begin, std::min(kInsertBatch, docs.size() - begin));
      auto tail =
        batch.size() == kInsertBatch ? nullptr : PrepareInsert(batch.size());
      if (batch.size() != kInsertBatch && !tail) {
        return false;
      }
      duckdb::vector<duckdb::Value> values;
      values.reserve(batch.size() * kInsertColumns);
      for (const auto& doc : batch) {
        values.emplace_back(std::string{doc.path});
        values.emplace_back(std::string{doc.title});
        values.emplace_back(std::string{doc.breadcrumb});
        values.emplace_back(std::string{doc.content});
      }
      auto& statement = tail ? *tail : *full;
      auto result = statement.Execute(values, /*allow_stream_result=*/false);
      if (result->HasError()) {
        SDB_WARN(GENERAL, "embedded docs: insert failed: ", result->GetError());
        return false;
      }
    }
    return true;
  }

  duckdb::unique_ptr<duckdb::Connection> _conn;
  std::shared_ptr<ConnectionContext> _ctx;
};

std::optional<std::vector<IndexBlob>> BuildImage() {
  const auto database =
    catalog::FindDatabase(irs::StaticStrings::kDefaultDatabase);
  if (!database) {
    SDB_ERROR(STARTUP,
              "cannot build the docs index: default database not found");
    return std::nullopt;
  }
  const std::string_view name = database->name.GetIdentifierName();
  const auto begin = std::chrono::steady_clock::now();
  try {
    Loader loader{name, database->oid};
    auto image = loader.Build();
    if (image) {
      SDB_INFO(STARTUP, "embedded docs indexed in database \"", name, "\" in ",
               absl::FormatDuration(
                 absl::FromChrono(std::chrono::steady_clock::now() - begin)));
    }
    return image;
  } catch (const std::exception& e) {
    SDB_ERROR(STARTUP, "cannot build the docs index in database \"", name,
              "\": ", e.what());
    return std::nullopt;
  }
}

std::string DescribeLayout(const catalog::SearchTableEntry& table) {
  const auto config = table.Storage()->Config();
  std::string layout;
  for (const auto& column : table.GetColumns().Logical()) {
    const auto id = static_cast<irs::field_id>(column.Logical().index);
    absl::StrAppend(&layout, column.Name().GetIdentifierName(), " ", id);
    if (const auto* entry = config ? config->FindColumnInfo(id) : nullptr;
        entry && entry->IsTermDict()) {
      absl::StrAppend(&layout, " ", config->TermField(id));
    }
    absl::StrAppend(&layout, "\n");
  }
  if (config) {
    for (const auto& key : config->keys) {
      if (!key.normalized_expression.empty()) {
        absl::StrAppend(&layout, "content_text - ", key.field_id, "\n");
      }
    }
  }
  return layout;
}

std::optional<std::vector<IndexBlob>> ExportImage(
  duckdb::ClientContext& context, std::string_view database) {
  std::optional<std::vector<IndexBlob>> image;
  context.RunFunctionInTransaction([&] {
    const auto entry = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
      context,
      duckdb::QualifiedName{duckdb::Identifier{database},
                            duckdb::Identifier{kSchema},
                            duckdb::Identifier{"docs"}},
      duckdb::OnEntryNotFound::RETURN_NULL);
    const auto* table =
      dynamic_cast<const catalog::SearchTableEntry*>(entry.get());
    if (!table) {
      SDB_ERROR(STARTUP, "cannot build the docs index: ", kTable,
                " is not a search table in the catalog");
      return;
    }
    const auto store = search::SearchTable::GetPath(
      table->ParentCatalog().GetAttached().oid,
      table->ParentSchema(context).oid, table->oid);
    std::vector<IndexBlob> blobs;
    for (const auto& file : std::filesystem::directory_iterator{store}) {
      if (file.is_regular_file()) {
        blobs.push_back(
          {.name = file.path().filename().string(),
           .bytes = utils::file_utils::Slurp(file.path().string())});
      }
    }
    blobs.push_back(
      {.name = std::string{kLayoutFile}, .bytes = DescribeLayout(*table)});
    image = std::move(blobs);
  });
  return image;
}

bool WriteImage(std::span<const IndexBlob> image,
                const std::filesystem::path& out) {
  try {
    if (std::filesystem::exists(out) && !std::filesystem::is_empty(out)) {
      SDB_ERROR(STARTUP, "cannot build the docs index: ", out.string(),
                " already exists and is not empty; point --build_docs_index at "
                "a new or empty directory");
      return false;
    }
    std::filesystem::create_directories(out);
    size_t bytes = 0;
    for (const auto& blob : image) {
      utils::file_utils::Spit((out / blob.name).string(), blob.bytes);
      bytes += blob.bytes.size();
    }
    SDB_INFO(STARTUP, "docs index written to ", out.string(), ": ",
             image.size(), " files, ", bytes, " bytes");
    return true;
  } catch (const std::exception& e) {
    SDB_ERROR(STARTUP, "cannot build the docs index: writing ", out.string(),
              " failed: ", e.what());
    return false;
  }
}

}  // namespace

bool BuildEmbeddedIndex(const std::filesystem::path& out) {
  const auto image = BuildImage();
  return image && WriteImage(*image, out);
}

}  // namespace sdb::docs
