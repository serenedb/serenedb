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
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <yaclib/lazy/make.hpp>

#include "network/http/common.h"

namespace sdb::network::http::mcp {
namespace {

constexpr int64_t kDefaultLimit = 5;
constexpr int64_t kMaxLimit = 10;

std::string Cell(duckdb::MaterializedQueryResult& result,
                 std::string_view column, size_t row) {
  const auto it = absl::c_find(result.names, column);
  SDB_ASSERT(it != result.names.end(), "no column ", column);
  return duckdb::StringValue::Get(
    result.GetValue(static_cast<size_t>(it - result.names.begin()), row));
}

// "page.md" is a whole-page doc; "page.md#Title#Heading" a heading row, one
// unescaped '#' per level, with the page title acting as the H1.
size_t Depth(std::string_view path) {
  size_t depth = 0;
  for (size_t i = 0; i < path.size(); ++i) {
    if (path[i] == '#' && (i == 0 || path[i - 1] != '\\')) {
      ++depth;
    }
  }
  return depth;
}

std::string HeadingLine(std::string_view path, std::string_view title) {
  return absl::StrCat(std::string(std::max<size_t>(Depth(path), 1), '#'), " ",
                      title);
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
  auto result = co_await ctx.RunQuery(
    absl::StrCat("SELECT title, breadcrumb, content FROM sdb_docs.docs "
                 "WHERE path = ",
                 SqlLiteral(*args.path)),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(absl::StrCat("read_doc failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    co_return Error(absl::StrCat("No documentation at path: ", *args.path,
                                 ". Use list_docs or search_docs to find "
                                 "valid paths."));
  }
  std::string text = absl::StrCat("path: ", *args.path, "\n");
  if (const auto breadcrumb = Cell(*result, "breadcrumb", 0);
      !breadcrumb.empty()) {
    absl::StrAppend(&text, "in: ", breadcrumb, "\n");
  }
  absl::StrAppend(&text, "\n",
                  HeadingLine(*args.path, Cell(*result, "title", 0)), "\n");
  if (const auto content = Cell(*result, "content", 0); !content.empty()) {
    absl::StrAppend(&text, "\n", content);
  }
  co_return ToolResult{std::move(text)};
}

// A directory prefix lists one row per page: whole-page docs and the title
// rows of split docs. A prefix naming a page or heading lists the rows under
// it.
yaclib::Task<ToolResult> ListDocs(RequestContext& ctx, const ToolArgs& args) {
  const auto prefix = args.prefix.value_or("");
  const bool within_page =
    prefix.ends_with(".md") || prefix.ends_with(".mdx") || Depth(prefix) > 0;
  auto result = co_await ctx.RunQuery(
    absl::StrCat("SELECT path, title, breadcrumb FROM sdb_docs.docs WHERE ",
                 within_page
                   ? ""
                   : "length(path) - length(replace(path, '#', '')) <= 1 AND ",
                 "starts_with(path, ", SqlLiteral(prefix), ") ORDER BY path"),
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

// The catalog keyed by object rather than by page: one line per documented
// function, statement, tokenizer, type, setting or index type.
yaclib::Task<ToolResult> ListObjects(RequestContext& ctx,
                                     const ToolArgs& args) {
  const auto kind = args.kind.value_or("");
  auto result = co_await ctx.RunQuery(
    absl::StrCat(
      "SELECT kind, signature, coalesce(summary, '') AS summary "
      "FROM sdb_docs.objects() ",
      kind.empty() ? "" : absl::StrCat("WHERE kind = ", SqlLiteral(kind), " "),
      "ORDER BY kind, name, path"),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(absl::StrCat("list_objects failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    co_return Error(absl::StrCat(
      "No objects of kind: ", kind,
      ". Known kinds: function, statement, tokenizer, type, setting, "
      "index_type."));
  }
  std::string text;
  for (size_t row = 0; row < result->RowCount(); ++row) {
    absl::StrAppend(&text, row == 0 ? "" : "\n",
                    Cell(*result, "signature", row));
    if (kind.empty()) {
      absl::StrAppend(&text, " (", Cell(*result, "kind", row), ")");
    }
    if (const auto summary = Cell(*result, "summary", row); !summary.empty()) {
      absl::StrAppend(&text, " - ", summary);
    }
  }
  co_return ToolResult{std::move(text)};
}

// Every object carrying the name, because a caller that does not know an
// object's kind cannot disambiguate one.
yaclib::Task<ToolResult> DescribeObject(RequestContext& ctx,
                                        const ToolArgs& args) {
  if (!args.name || absl::StripAsciiWhitespace(*args.name).empty()) {
    co_return Error("describe_object: name must not be empty");
  }
  auto result = co_await ctx.RunQuery(
    absl::StrCat("SELECT o.kind, o.signature, o.breadcrumb, o.path, d.content "
                 "FROM sdb_docs.objects() o JOIN sdb_docs.docs d USING (path) "
                 "WHERE lower(o.name) = lower(",
                 SqlLiteral(*args.name), ") ORDER BY o.kind, o.path"),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(
      absl::StrCat("describe_object failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    auto similar = co_await ctx.RunQuery(
      absl::StrCat("SELECT kind, signature FROM sdb_docs.objects() WHERE name "
                   "ILIKE '%' || ",
                   SqlLiteral(*args.name),
                   " || '%' ORDER BY length(name), name LIMIT 10"),
      /*writes=*/false);
    std::string text =
      absl::StrCat("No documented object named: ", *args.name, ".");
    if (!similar->HasError() && similar->RowCount() > 0) {
      absl::StrAppend(&text, "\n\nMaybe you meant:");
      for (size_t row = 0; row < similar->RowCount(); ++row) {
        absl::StrAppend(&text, "\n  ", Cell(*similar, "signature", row), " (",
                        Cell(*similar, "kind", row), ")");
      }
    } else {
      absl::StrAppend(&text,
                      " Use list_objects to see what exists, or search_docs to "
                      "search the prose.");
    }
    co_return Error(std::move(text));
  }
  std::string text;
  for (size_t row = 0; row < result->RowCount(); ++row) {
    absl::StrAppend(&text, row == 0 ? "" : "\n\n---\n\n",
                    Cell(*result, "signature", row), " (",
                    Cell(*result, "kind", row),
                    ")\npath: ", Cell(*result, "path", row), "\n");
    if (const auto breadcrumb = Cell(*result, "breadcrumb", row);
        !breadcrumb.empty()) {
      absl::StrAppend(&text, "in: ", breadcrumb, "\n");
    }
    if (const auto content = Cell(*result, "content", row); !content.empty()) {
      absl::StrAppend(&text, "\n", content);
    }
  }
  co_return ToolResult{std::move(text)};
}

using ToolFn = yaclib::Task<ToolResult> (*)(RequestContext&, const ToolArgs&);

constexpr std::array<std::pair<std::string_view, ToolFn>, 5> kTools{{
  {"search_docs", &SearchDocs},
  {"read_doc", &ReadDoc},
  {"list_docs", &ListDocs},
  {"list_objects", &ListObjects},
  {"describe_object", &DescribeObject},
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
                           .description = "Keyword query, 2-6 words naming a "
                                          "concrete feature or concept (no "
                                          "pronouns)"}},
                         {"limit",
                          {.type = "integer",
                           .description = "Max results (default 5)",
                           .minimum = 1,
                           .maximum = kMaxLimit}}},
          .required = {"query"}}},
      {.name = "read_doc",
       .description = "Return a documentation page or section as complete "
                      "Markdown. Pass a path exactly as returned by "
                      "search_docs or list_docs.",
       .inputSchema = {.properties = {{"path",
                                       {.type = "string",
                                        .description =
                                          "Path exactly as returned by "
                                          "search_docs or list_docs"}}},
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
       .inputSchema =
         {.properties = {{"kind",
                          {.type = "string",
                           .description =
                             "One of function, statement, tokenizer, type, "
                             "setting, index_type; omit for everything"}}}}},
      {.name = "describe_object",
       .description =
         "Return the full documentation for a named object. Reports every "
         "object carrying the name, since one name can be a function and a "
         "data type at once. Use it to confirm a function exists and to read "
         "its signature before calling it.",
       .inputSchema = {.properties = {{"name",
                                       {.type = "string",
                                        .description =
                                          "Bare object name as returned by "
                                          "list_objects, e.g. 'ts_phrase'"}}},
                       .required = {"name"}}},
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
