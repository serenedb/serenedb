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

#pragma once

#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "docs/docs_index_data.h"

namespace duckdb {

class DatabaseInstance;
}

namespace sdb::docs {

struct Entry {
  std::string path;
  std::string title;
  std::string breadcrumb;
  std::string content;
  double score = 0.0;
};

enum class Content : bool {
  Omit,
  Include,
};

struct Object {
  std::string kind;
  std::string name;
  std::string signature;
  std::string summary;
  std::string aliases;
  std::string path;
  std::string page;
  std::string category;
  std::string breadcrumb;
};

std::size_t HeadingDepth(std::string_view path);

std::string_view CallName(std::string_view term);

std::string SiteRoute(std::string_view link);

std::string Markdown(const Entry& entry);

std::string Snippet(std::string_view text, size_t limit);

void CheckLayout(std::span<const IndexFile> files);

std::optional<Entry> EntryAt(duckdb::DatabaseInstance& db,
                             std::string_view path, Content content);

std::optional<Entry> FindByPath(duckdb::DatabaseInstance& db,
                                std::string_view path);

std::optional<Entry> ResolveLink(duckdb::DatabaseInstance& db,
                                 std::string_view link,
                                 std::string_view base = {},
                                 Content content = Content::Omit);

std::vector<Entry> Lookup(duckdb::DatabaseInstance& db, std::string_view name,
                          Content content = Content::Omit);

std::vector<Object> Objects(duckdb::DatabaseInstance& db,
                            std::string_view kind = {});

std::vector<Object> FindObjects(duckdb::DatabaseInstance& db,
                                std::string_view name, std::string_view kind);

std::vector<std::string> CompleteName(duckdb::DatabaseInstance& db,
                                      std::string_view prefix,
                                      std::string_view kind, size_t limit,
                                      std::span<const std::string> extra = {});

std::vector<Entry> ListPrefix(duckdb::DatabaseInstance& db,
                              std::string_view prefix, bool pages_only,
                              Content content = Content::Omit);

std::vector<Entry> Children(duckdb::DatabaseInstance& db,
                            std::string_view path);

std::vector<std::string> ListPaths(duckdb::DatabaseInstance& db,
                                   std::string_view prefix);

std::vector<std::string> CompletePath(duckdb::DatabaseInstance& db,
                                      std::string_view prefix, size_t limit);

std::vector<Entry> Search(duckdb::DatabaseInstance& db, std::string_view query,
                          size_t limit, Content content, std::string& error);

std::vector<Entry> Candidates(duckdb::DatabaseInstance& db,
                              std::string_view name, size_t limit);

}  // namespace sdb::docs
