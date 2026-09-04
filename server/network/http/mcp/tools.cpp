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

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstddef>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <string>
#include <string_view>

#include "network/http/es/common.h"

namespace sdb::network::http::mcp {
namespace {

constexpr int64_t kDefaultLimit = 5;
constexpr int64_t kMaxLimit = 10;
constexpr size_t kSnippetChars = 400;

constexpr std::string_view kToolsList =
  R"json({"tools":[{"name":"search_docs","description":"Search the SereneDB documentation. Returns numbered sections [n] with the section title, the page title, the page path and a snippet. Follow up with read_doc for the full text of a page or section.","inputSchema":{"type":"object","properties":{"query":{"type":"string","description":"Keyword query, 2-6 words naming a concrete feature or concept (no pronouns)"},"limit":{"type":"integer","minimum":1,"maximum":10,"description":"Max results (default 5)"}},"required":["query"]}},{"name":"read_doc","description":"Read a documentation page as Markdown, or a single section of it. Pass the path exactly as returned by search_docs or list_docs; add section (a section title from search_docs) to get only that section with its code examples.","inputSchema":{"type":"object","properties":{"path":{"type":"string","description":"Page path exactly as returned by search_docs or list_docs"},"section":{"type":"string","description":"Section title exactly as returned by search_docs; omit for the whole page"}},"required":["path"]}},{"name":"list_docs","description":"List documentation pages (path and title), optionally only those under a path prefix such as sql/statements/.","inputSchema":{"type":"object","properties":{"prefix":{"type":"string","description":"Path prefix to filter by; omit for all pages"}}}}]})json";

std::string Cell(duckdb::MaterializedQueryResult& result, size_t column,
                 size_t row) {
  const auto value = result.GetValue(column, row);
  return value.IsNull() ? std::string{} : duckdb::StringValue::Get(value);
}

std::string Snippet(std::string_view text) {
  std::string out;
  out.reserve(std::min(text.size(), kSnippetChars) + 3);
  bool space = false;
  for (const char c : text) {
    if (c == ' ' || c == '\n' || c == '\t' || c == '\r') {
      space = !out.empty();
      continue;
    }
    if (space) {
      out.push_back(' ');
      space = false;
    }
    out.push_back(c);
    if (out.size() >= kSnippetChars) {
      break;
    }
  }
  if (out.size() >= kSnippetChars) {
    const auto cut = out.rfind(' ');
    if (cut != std::string::npos && cut > kSnippetChars / 2) {
      out.resize(cut);
    }
    out.append("...");
  }
  return out;
}

ToolResult Error(std::string text) { return {std::move(text), true}; }

yaclib::Task<ToolResult> SearchDocs(RequestContext& ctx, const ToolArgs& args) {
  if (!args.query ||
      args.query->find_first_not_of(" \t\r\n") == std::string::npos) {
    co_return Error("search_docs: query must not be empty");
  }
  const auto limit =
    std::clamp(args.limit.value_or(kDefaultLimit), int64_t{1}, kMaxLimit);
  const auto query = es::SqlLiteral(*args.query);
  auto result = co_await ctx.RunQuery(
    absl::StrCat("SELECT s.path, s.title, d.title, s.content_text FROM "
                 "sdb_docs.sections_fts s JOIN sdb_docs.docs d ON d.path = "
                 "s.path WHERE s.title @@ ",
                 query, " OR s.content_text @@ ", query,
                 " ORDER BY BM25(s.tableoid) DESC, s.path, s.title LIMIT ",
                 limit),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(absl::StrCat("search_docs failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    co_return ToolResult{"No results."};
  }
  std::string text;
  for (size_t row = 0; row < result->RowCount(); ++row) {
    const auto path = Cell(*result, 0, row);
    const auto title = Cell(*result, 1, row);
    const auto page = Cell(*result, 2, row);
    absl::StrAppend(&text, row == 0 ? "" : "\n\n", "[", row + 1, "] ", title);
    if (page != title) {
      absl::StrAppend(&text, " - ", page);
    }
    absl::StrAppend(&text, "\npath: ", path, "\n",
                    Snippet(Cell(*result, 3, row)));
  }
  co_return ToolResult{std::move(text)};
}

yaclib::Task<ToolResult> ReadDoc(RequestContext& ctx, const ToolArgs& args) {
  if (!args.path || args.path->empty()) {
    co_return Error("read_doc: path is required");
  }
  const auto path = es::SqlLiteral(*args.path);
  if (args.section && !args.section->empty()) {
    auto result = co_await ctx.RunQuery(
      absl::StrCat("SELECT breadcrumb, content FROM sdb_docs.sections WHERE "
                   "path = ",
                   path, " AND title = ", es::SqlLiteral(*args.section),
                   " ORDER BY id"),
      /*writes=*/false);
    if (result->HasError()) {
      co_return Error(absl::StrCat("read_doc failed: ", result->GetError()));
    }
    if (result->RowCount() == 0) {
      co_return Error(absl::StrCat("No section titled \"", *args.section,
                                   "\" in ", *args.path,
                                   ". Use search_docs to find section titles "
                                   "or omit section to read the whole page."));
    }
    std::string text = absl::StrCat(*args.section, "\npath: ", *args.path);
    for (size_t row = 0; row < result->RowCount(); ++row) {
      if (result->RowCount() > 1) {
        absl::StrAppend(&text, "\n\n[", Cell(*result, 0, row), "]");
      }
      absl::StrAppend(&text, "\n\n", Cell(*result, 1, row));
    }
    co_return ToolResult{std::move(text)};
  }
  auto result = co_await ctx.RunQuery(
    absl::StrCat("SELECT title, content FROM sdb_docs.docs WHERE path = ",
                 path),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(absl::StrCat("read_doc failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    co_return Error(absl::StrCat("No documentation page at path: ", *args.path,
                                 ". Use list_docs or search_docs to find "
                                 "valid paths."));
  }
  co_return ToolResult{absl::StrCat("# ", Cell(*result, 0, 0), "\npath: ",
                                    *args.path, "\n\n", Cell(*result, 1, 0))};
}

yaclib::Task<ToolResult> ListDocs(RequestContext& ctx, const ToolArgs& args) {
  const auto prefix = args.prefix.value_or("");
  auto result = co_await ctx.RunQuery(
    absl::StrCat("SELECT path, title FROM sdb_docs.docs WHERE "
                 "starts_with(path, ",
                 es::SqlLiteral(prefix), ") ORDER BY path"),
    /*writes=*/false);
  if (result->HasError()) {
    co_return Error(absl::StrCat("list_docs failed: ", result->GetError()));
  }
  if (result->RowCount() == 0) {
    co_return ToolResult{
      absl::StrCat("No documentation pages under prefix: ", prefix)};
  }
  std::string text;
  for (size_t row = 0; row < result->RowCount(); ++row) {
    absl::StrAppend(&text, row == 0 ? "" : "\n", Cell(*result, 0, row), " - ",
                    Cell(*result, 1, row));
  }
  co_return ToolResult{std::move(text)};
}

}  // namespace

std::string_view ToolsListJson() { return kToolsList; }

bool KnownTool(std::string_view name) {
  return name == "search_docs" || name == "read_doc" || name == "list_docs";
}

yaclib::Task<ToolResult> CallTool(RequestContext& ctx, std::string_view name,
                                  const ToolArgs& args) {
  if (name == "search_docs") {
    co_return co_await SearchDocs(ctx, args);
  }
  if (name == "read_doc") {
    co_return co_await ReadDoc(ctx, args);
  }
  if (name == "list_docs") {
    co_return co_await ListDocs(ctx, args);
  }
  co_return Error(absl::StrCat("Unknown tool: ", name));
}

}  // namespace sdb::network::http::mcp
