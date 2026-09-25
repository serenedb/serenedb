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

#include "network/http/mcp/tools.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <yaclib/lazy/make.hpp>

#include "connector/functions/markdown_render.h"
#include "docs/docs_search.h"
#include "network/http/common.h"

namespace sdb::network::http::mcp {
namespace {

constexpr int64_t kDefaultLimit = 5;
constexpr int64_t kMaxLimit = 10;
constexpr size_t kMaxDocChars = 40000;
constexpr int64_t kMentions = 3;
constexpr int64_t kMaxObjects = 2000;
constexpr int64_t kSummaryChars = 120;
constexpr std::string_view kKinds =
  "function, statement, tokenizer, type, setting, index_type, command";
constexpr int64_t kContentChars = 4000;

std::string Cell(duckdb::MaterializedQueryResult& result,
                 std::string_view column, size_t row) {
  const auto it = absl::c_find(result.names, column);
  SDB_ASSERT(it != result.names.end(), "no column ", column);
  return duckdb::StringValue::Get(
    result.GetValue(static_cast<size_t>(it - result.names.begin()), row));
}

ToolResult Error(std::string text) { return {std::move(text), true}; }

yaclib::Task<ToolResult> SearchDocs(RequestContext& ctx, const ToolArgs& args) {
  if (!args.query || absl::StripAsciiWhitespace(*args.query).empty()) {
    co_return Error("search_docs: query must not be empty");
  }
  const auto limit =
    std::clamp(args.limit.value_or(kDefaultLimit), int64_t{1}, kMaxLimit);
  auto result = co_await ctx.RunQuery(
    absl::StrCat("SELECT path, title, breadcrumb, snippet FROM "
                 "sdb_docs.search(",
                 SqlLiteral(*args.query), ", ", limit, ")"),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(absl::StrCat("search_docs failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    co_return ToolResult{"No results."};
  }
  std::string text;
  for (size_t row = 0; row < result->RowCount(); ++row) {
    const auto breadcrumb = Cell(*result, "breadcrumb", row);
    absl::StrAppend(&text, row == 0 ? "" : "\n\n", "[", row + 1, "] ",
                    Cell(*result, "title", row));
    if (!breadcrumb.empty()) {
      absl::StrAppend(&text, " - ", breadcrumb);
    }
    absl::StrAppend(&text, "\npath: ", Cell(*result, "path", row), "\n",
                    Cell(*result, "snippet", row));
  }
  co_return ToolResult{std::move(text)};
}

yaclib::Task<ToolResult> ReadDoc(RequestContext& ctx, const ToolArgs& args) {
  if (!args.path || args.path->empty()) {
    co_return Error("read_doc: path is required");
  }
  auto& db = duckdb::DatabaseInstance::GetDatabase(*ctx.Connection().context);
  const auto resolved = docs::ResolveLink(db, *args.path);
  if (!resolved) {
    co_return Error(absl::StrCat("No documentation at path: ", *args.path,
                                 ". Use list_docs or search_docs to find "
                                 "valid paths."));
  }
  const auto& path = resolved->path;
  auto result = co_await ctx.RunQuery(
    absl::StrCat(
      "SELECT repeat('#', greatest(depth, 1)) || ' ' || title AS "
      "heading, breadcrumb, content FROM sdb_docs.docs WHERE path = ",
      SqlLiteral(path)),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(absl::StrCat("read_doc failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    co_return Error(absl::StrCat("No documentation at path: ", *args.path,
                                 ". Use list_docs or search_docs to find "
                                 "valid paths."));
  }
  std::string text = absl::StrCat("path: ", path, "\n");
  if (const auto breadcrumb = Cell(*result, "breadcrumb", 0);
      !breadcrumb.empty()) {
    absl::StrAppend(&text, "in: ", breadcrumb, "\n");
  }
  absl::StrAppend(&text, "\n", Cell(*result, "heading", 0), "\n");
  auto content = connector::AbsoluteLinks(Cell(*result, "content", 0), path);
  if (content.size() > kMaxDocChars) {
    const auto cut = content.rfind('\n', kMaxDocChars);
    const auto total = content.size();
    content.resize(cut == std::string::npos ? kMaxDocChars : cut);
    absl::StrAppend(&content, "\n\n(Cut at ", content.size(), " of ", total,
                    " characters. Read one of its sections instead:");
    for (const auto& child : docs::Children(db, path)) {
      absl::StrAppend(&content, "\n", child.path, " - ", child.title);
    }
    content.push_back(')');
  }
  if (!content.empty()) {
    absl::StrAppend(&text, "\n", content);
  }
  co_return ToolResult{std::move(text)};
}

// A directory prefix lists one row per page: whole-page docs and the title
// rows of split docs. A prefix naming a page or heading lists the rows under
// it.
yaclib::Task<ToolResult> ListDocs(RequestContext& ctx, const ToolArgs& args) {
  const auto prefix = args.prefix.value_or("");
  const bool within_page = prefix.ends_with(".md") ||
                           prefix.ends_with(".mdx") ||
                           docs::HeadingDepth(prefix) > 0;
  auto result = co_await ctx.RunQuery(
    absl::StrCat(
      "SELECT path, title, breadcrumb FROM sdb_docs.docs WHERE starts_with("
      "path, ",
      SqlLiteral(prefix), ")", within_page ? "" : " AND depth <= 1",
      " ORDER BY path"),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(absl::StrCat("list_docs failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    co_return ToolResult{
      absl::StrCat("No documentation under prefix: ", prefix)};
  }
  std::string text;
  for (size_t row = 0; row < result->RowCount(); ++row) {
    absl::StrAppend(&text, row == 0 ? "" : "\n", Cell(*result, "path", row),
                    " - ", Cell(*result, "title", row));
    if (const auto breadcrumb = Cell(*result, "breadcrumb", row);
        !breadcrumb.empty()) {
      absl::StrAppend(&text, " (", breadcrumb, ")");
    }
  }
  co_return ToolResult{std::move(text)};
}

yaclib::Task<ToolResult> ListObjects(RequestContext& ctx,
                                     const ToolArgs& args) {
  const std::string kind{
    absl::StripAsciiWhitespace(args.kind.value_or(std::string{}))};
  auto result = co_await ctx.RunQuery(
    absl::StrCat(
      "SELECT kind, best.signature AS signature, "
      "left(best.summary, ",
      kSummaryChars,
      ") AS summary FROM ("
      "SELECT kind, name, arg_min({'signature': signature, "
      "'summary': coalesce(summary, '')}, "
      "CASE WHEN nullif(trim(coalesce(summary, '')), '') IS NULL "
      "THEN 1000000 ELSE 0 END + length(signature)) AS best "
      "FROM sdb_docs.objects ",
      kind.empty()
        ? ""
        : absl::StrCat("WHERE lower(kind) = lower(", SqlLiteral(kind), ") "),
      "GROUP BY kind, name) ORDER BY kind, name LIMIT ", kMaxObjects + 1),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(absl::StrCat("list_objects failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    co_return Error(absl::StrCat("No objects of kind: ", kind,
                                 ". Known kinds: ", kKinds, "."));
  }
  const auto shown =
    std::min<size_t>(result->RowCount(), static_cast<size_t>(kMaxObjects));
  std::string text;
  for (size_t row = 0; row < shown; ++row) {
    absl::StrAppend(&text, row == 0 ? "" : "\n",
                    Cell(*result, "signature", row));
    if (kind.empty()) {
      absl::StrAppend(&text, " (", Cell(*result, "kind", row), ")");
    }
    if (const auto summary = Cell(*result, "summary", row); !summary.empty()) {
      absl::StrAppend(&text, " - ", summary);
    }
  }
  if (result->RowCount() > shown) {
    absl::StrAppend(&text, "\n\n(truncated at ", shown,
                    " objects; narrow with kind)");
  }
  co_return ToolResult{std::move(text)};
}

yaclib::Task<ToolResult> DescribeObject(RequestContext& ctx,
                                        const ToolArgs& args) {
  const std::string name{
    absl::StripAsciiWhitespace(args.name.value_or(std::string{}))};
  if (name.empty()) {
    co_return Error("describe_object: name must not be empty");
  }
  const std::string kind{
    absl::StripAsciiWhitespace(args.kind.value_or(std::string{}))};
  const auto literal = SqlLiteral(name);
  const auto kind_literal =
    kind.empty() ? std::string{"NULL"} : SqlLiteral(kind);
  auto result = co_await ctx.RunQuery(
    absl::StrCat(
      "WITH exact AS (SELECT kind, title AS signature, breadcrumb, path "
      "FROM sdb_docs.object(",
      literal, ", ", kind_literal,
      ") WHERE kind IS NOT NULL), "
      "o AS MATERIALIZED (SELECT kind, name, signature FROM sdb_docs.objects",
      kind.empty()
        ? ""
        : absl::StrCat(" WHERE lower(kind) = lower(", kind_literal, ")"),
      "), "
      "candidates AS (SELECT kind, arg_min(signature, length(signature)) AS "
      "signature, min(length(name)) AS width, name FROM o "
      "WHERE NOT EXISTS (SELECT 1 FROM exact) AND name ILIKE '%' || ",
      literal,
      " || '%' GROUP BY kind, name ORDER BY width, name LIMIT 10) "
      "SELECT kind, signature, breadcrumb, path, "
      "row_number() OVER (ORDER BY kind, path) AS position FROM exact "
      "UNION ALL "
      "SELECT kind, signature, NULL, NULL, "
      "row_number() OVER (ORDER BY width, name) FROM candidates "
      "ORDER BY position"),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(
      absl::StrCat("describe_object failed: ", result->GetError()));
  }
  const auto exact = [&](size_t row) {
    return !result->GetValue(3, row).IsNull();
  };
  if (result->RowCount() == 0 || !exact(0)) {
    std::string text = absl::StrCat(
      "No documented ", kind.empty() ? "object" : kind, " named: ", name, ".");
    const bool functions = kind.empty() || kind == "function";
    const bool settings = kind.empty() || kind == "setting";
    duckdb::unique_ptr<duckdb::MaterializedQueryResult> live;
    if (functions || settings) {
      live = co_await ctx.RunQuery(
        absl::StrCat(
          "SELECT kind, signature, description FROM (",
          functions
            ? absl::StrCat(
                "SELECT function_type AS kind, function_name || '(' || "
                "array_to_string(list_transform(generate_series(1, "
                "len(parameters)), i -> parameters[i] || ' ' || "
                "parameter_types[i]), ', ') || CASE WHEN varargs IS NULL THEN "
                "'' ELSE ', ...' END || ')' || coalesce(' -> ' || return_type, "
                "'') AS signature, coalesce(description, '') AS description "
                "FROM duckdb_functions() WHERE lower(function_name) = lower(",
                literal, ")", settings ? " UNION ALL " : "")
            : "",
          settings
            ? absl::StrCat(
                "SELECT 'setting', name || ' ' || input_type || ' = ' || "
                "coalesce(value, 'NULL'), coalesce(description, '') FROM "
                "duckdb_settings() WHERE lower(name) = lower(",
                literal, ")")
            : "",
          ") ORDER BY kind, signature LIMIT 12"),
        /*writes=*/false);
    }
    const bool found_live = live && !live->HasError() && live->RowCount() > 0;
    if (found_live) {
      absl::StrAppend(&text, " The server has it, undocumented:");
      for (size_t row = 0; row < live->RowCount(); ++row) {
        absl::StrAppend(&text, "\n  ", Cell(*live, "signature", row), " (",
                        Cell(*live, "kind", row), ")");
        if (const auto about = Cell(*live, "description", row);
            !about.empty()) {
          absl::StrAppend(&text, " - ", about);
        }
      }
    }
    if (result->RowCount() > 0) {
      absl::StrAppend(&text, "\n\nMaybe you meant:");
      for (size_t row = 0; row < result->RowCount(); ++row) {
        absl::StrAppend(&text, "\n  ", Cell(*result, "signature", row), " (",
                        Cell(*result, "kind", row), ")");
      }
    }
    auto mentions = co_await ctx.RunQuery(
      absl::StrCat("SELECT path, title FROM sdb_docs.search(", literal, ", ",
                   kMentions, ")"),
      /*writes=*/false);
    if (!mentions->HasError() && mentions->RowCount() > 0) {
      absl::StrAppend(&text,
                      "\n\nThe documentation mentions it here (pass a "
                      "path to read_doc):");
      for (size_t row = 0; row < mentions->RowCount(); ++row) {
        absl::StrAppend(&text, "\n  ", Cell(*mentions, "path", row), " - ",
                        Cell(*mentions, "title", row));
      }
    } else if (!found_live && result->RowCount() == 0) {
      absl::StrAppend(&text,
                      " Use list_objects to see what exists, or search_docs to "
                      "search the prose.");
    }
    if (found_live) {
      co_return ToolResult{std::move(text)};
    }
    co_return Error(std::move(text));
  }
  std::vector<std::string> paths;
  for (size_t row = 0; row < result->RowCount(); ++row) {
    paths.push_back(SqlLiteral(Cell(*result, "path", row)));
  }
  auto bodies = co_await ctx.RunQuery(
    absl::StrCat("SELECT path, left(content, ", kContentChars,
                 ") AS content, length(content) > ", kContentChars,
                 " AS truncated FROM sdb_docs.docs WHERE path IN (",
                 absl::StrJoin(paths, ", "), ")"),
    /*writes=*/false);
  if (bodies->HasError()) {
    co_return Error(
      absl::StrCat("describe_object failed: ", bodies->GetError()));
  }
  absl::flat_hash_map<std::string, size_t> body_rows;
  for (size_t row = 0; row < bodies->RowCount(); ++row) {
    body_rows.emplace(Cell(*bodies, "path", row), row);
  }
  std::string text;
  for (size_t row = 0; row < result->RowCount(); ++row) {
    const auto path = Cell(*result, "path", row);
    absl::StrAppend(&text, row == 0 ? "" : "\n\n---\n\n",
                    Cell(*result, "signature", row), " (",
                    Cell(*result, "kind", row), ")\npath: ", path, "\n");
    if (const auto breadcrumb = Cell(*result, "breadcrumb", row);
        !breadcrumb.empty()) {
      absl::StrAppend(&text, "in: ", breadcrumb, "\n");
    }
    const auto body = body_rows.find(path);
    if (body == body_rows.end()) {
      continue;
    }
    if (const auto content = Cell(*bodies, "content", body->second);
        !content.empty()) {
      absl::StrAppend(&text, "\n", connector::AbsoluteLinks(content, path));
      if (bodies->GetValue(2, body->second).GetValue<bool>()) {
        absl::StrAppend(&text,
                        "\n\n(truncated; read the full page with "
                        "read_doc on the path above)");
      }
    }
  }
  co_return ToolResult{std::move(text)};
}

struct Statement {
  bool single = true;
  bool explain = false;
};

size_t SkipQuoted(std::string_view sql, size_t at) {
  const auto quote = sql[at];
  for (auto i = at + 1; i < sql.size(); ++i) {
    if (sql[i] == quote) {
      if (i + 1 < sql.size() && sql[i + 1] == quote) {
        ++i;
        continue;
      }
      return i + 1;
    }
  }
  return sql.size();
}

size_t SkipDollarQuoted(std::string_view sql, size_t at) {
  const auto close = sql.find('$', at + 1);
  if (close == std::string_view::npos) {
    return at + 1;
  }
  const auto tag = sql.substr(at, close - at + 1);
  if (!absl::c_all_of(tag.substr(1, tag.size() - 2), [](char c) {
        return absl::ascii_isalnum(static_cast<unsigned char>(c)) || c == '_';
      })) {
    return at + 1;
  }
  const auto end = sql.find(tag, close + 1);
  return end == std::string_view::npos ? sql.size() : end + tag.size();
}

size_t SkipBlockComment(std::string_view sql, size_t at) {
  size_t depth = 0;
  for (auto i = at; i + 1 < sql.size(); ++i) {
    if (sql[i] == '/' && sql[i + 1] == '*') {
      ++depth;
      ++i;
    } else if (sql[i] == '*' && sql[i + 1] == '/') {
      ++i;
      if (--depth == 0) {
        return i + 1;
      }
    }
  }
  return sql.size();
}

Statement Classify(std::string_view sql) {
  Statement statement;
  bool first = true;
  bool ended = false;
  for (size_t i = 0; i < sql.size();) {
    const auto c = sql[i];
    if (absl::ascii_isspace(static_cast<unsigned char>(c))) {
      ++i;
      continue;
    }
    if (sql.substr(i).starts_with("--")) {
      const auto eol = sql.find('\n', i);
      i = eol == std::string_view::npos ? sql.size() : eol + 1;
      continue;
    }
    if (sql.substr(i).starts_with("/*")) {
      i = SkipBlockComment(sql, i);
      continue;
    }
    if (ended) {
      statement.single = false;
      break;
    }
    if (first) {
      statement.explain = absl::StartsWithIgnoreCase(sql.substr(i), "explain");
      first = false;
    }
    if (c == '\'' || c == '"') {
      i = SkipQuoted(sql, i);
    } else if (c == '$') {
      i = SkipDollarQuoted(sql, i);
    } else {
      ended = c == ';';
      ++i;
    }
  }
  return statement;
}

std::string WithoutExplain(std::string error) {
  constexpr std::string_view kLine = "LINE 1: EXPLAIN ";
  const auto at = error.find(kLine);
  if (at == std::string::npos) {
    return error;
  }
  error.erase(at + kLine.size() - std::string_view{"EXPLAIN "}.size(),
              std::string_view{"EXPLAIN "}.size());
  const auto caret = error.find('\n', at);
  if (caret != std::string::npos &&
      error.compare(caret + 1, 8, std::string(8, ' ')) == 0) {
    error.erase(caret + 1, 8);
  }
  return error;
}

yaclib::Task<ToolResult> CheckSql(RequestContext& ctx, const ToolArgs& args) {
  const std::string sql{
    absl::StripAsciiWhitespace(args.sql.value_or(std::string{}))};
  if (sql.empty()) {
    co_return Error("check_sql: sql must not be empty");
  }
  const auto statement = Classify(sql);
  if (!statement.single) {
    co_return Error("check_sql: pass one statement at a time");
  }
  if (statement.explain) {
    co_return Error(
      "check_sql: pass the statement itself; check_sql plans it with EXPLAIN "
      "and never runs it");
  }
  auto result =
    co_await ctx.RunQuery(absl::StrCat("EXPLAIN ", sql), /*writes=*/false);
  if (result->HasError()) {
    co_return ToolResult{
      absl::StrCat("Invalid: ", WithoutExplain(result->GetError()))};
  }
  std::string text = "Valid. The plan:\n";
  for (size_t row = 0; row < result->RowCount(); ++row) {
    absl::StrAppend(
      &text, result->GetValue(result->ColumnCount() - 1, row).ToString(), "\n");
  }
  co_return ToolResult{std::move(text)};
}

using ToolFn = yaclib::Task<ToolResult> (*)(RequestContext&, const ToolArgs&);

constexpr std::array<std::pair<std::string_view, ToolFn>, 6> kTools{{
  {"search_docs", &SearchDocs},
  {"read_doc", &ReadDoc},
  {"list_docs", &ListDocs},
  {"list_objects", &ListObjects},
  {"describe_object", &DescribeObject},
  {"check_sql", &CheckSql},
}};

}  // namespace

const ToolsList& Tools() {
  static const ToolsList list{
    .tools = {
      {.name = "search_docs",
       .description =
         "Search the SereneDB documentation. Returns numbered hits with "
         "title, location, a path for read_doc and a snippet. Cite the paths "
         "you used and offer read_doc for the full text.",
       .inputSchema =
         {.properties = {{"query",
                          {.type = "string",
                           .description =
                             "Keywords, a question or pasted text such as an "
                             "error message. Lucene syntax is honored when "
                             "used: \"quoted\" for a phrase, prefix*, term~1 "
                             "for a typo, title:, breadcrumb:, content: or "
                             "path: to scope a term, AND/OR/() to combine. "
                             "Exclusion (-term, NOT) is rejected"}},
                         {"limit",
                          {.type = "integer",
                           .description = "Max results (default 5)",
                           .minimum = 1,
                           .maximum = kMaxLimit}}},
          .required = {"query"}}},
      {.name = "read_doc",
       .description =
         "Return a documentation page or section as Markdown. Links in it "
         "are rewritten to paths this tool accepts, so any link can be "
         "followed. A very long page is cut and lists its sections.",
       .inputSchema = {.properties = {{"path",
                                       {.type = "string",
                                        .description =
                                          "A path from search_docs or "
                                          "list_docs, a page such as "
                                          "sql/functions/search/scoring.md, "
                                          "a link found in a page or a "
                                          "serenedb.com/docs URL"}}},
                       .required = {"path"}}},
      {.name = "list_docs",
       .description = "List documentation as 'path - title'. A directory "
                      "prefix lists pages; a page or section path lists the "
                      "sections under it. Omit the prefix for all pages.",
       .inputSchema = {.properties = {{"prefix",
                                       {.type = "string",
                                        .description =
                                          "Path prefix to filter by; omit for "
                                          "all pages"}}}}},
      {.name = "list_objects",
       .description =
         "List everything SereneDB documents, one line per object, as "
         "'signature (kind) - summary'. Consult this before writing "
         "SereneDB-specific SQL: SereneDB is not Postgres full-text search, so "
         "a function you expect may not exist under the name you expect. Pass "
         "a kind to keep the list small.",
       .inputSchema = {.properties = {{"kind",
                                       {.type = "string",
                                        .description = absl::StrCat(
                                          "One of ", kKinds,
                                          "; omit for everything")}}}}},
      {.name = "describe_object",
       .description =
         "Return the full documentation for a named object, found by its name "
         "or one of its aliases, such as INT8 for BIGINT. Reports every object "
         "carrying the name, since one name can be a function and a data type "
         "at once; pass a kind to keep only one. Use it to confirm a function "
         "exists and to read its signature before calling it. A name the "
         "documentation misses is looked up in the server's own catalog of "
         "functions and settings, and the pages mentioning it are listed.",
       .inputSchema =
         {.properties = {{"name",
                          {.type = "string",
                           .description = "Bare object name or alias as "
                                          "returned by list_objects, e.g. "
                                          "'ts_phrase'"}},
                         {"kind",
                          {.type = "string",
                           .description = absl::StrCat(
                             "One of ", kKinds, "; omit for every kind")}}},
          .required = {"name"}}},
      {.name = "check_sql",
       .description =
         "Check one SQL statement against this server without running it. "
         "Returns the server's parse or binder error with its hint, or the "
         "query plan: an IRESEARCH_SCAN with an Index Filter shows the "
         "inverted index serves the predicate. Tables resolve in the database "
         "this endpoint serves. Use it before handing SQL to the user.",
       .inputSchema = {.properties = {{"sql",
                                       {.type = "string",
                                        .description = "One SQL statement"}}},
                       .required = {"sql"}}},
    }};
  return list;
}

bool KnownTool(std::string_view name) {
  return absl::c_any_of(kTools,
                        [&](const auto& tool) { return tool.first == name; });
}

yaclib::Task<ToolResult> CallTool(RequestContext& ctx, std::string_view name,
                                  const ToolArgs& args) {
  const auto it = absl::c_find_if(
    kTools, [&](const auto& tool) { return tool.first == name; });
  if (it == kTools.end()) {
    return yaclib::MakeTask(Error(absl::StrCat("Unknown tool: ", name)));
  }
  return it->second(ctx, args);
}

}  // namespace sdb::network::http::mcp
