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

#include <array>
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
  std::string content_text;
  double score = 0.0;
};

struct Columns {
  bool content = false;
  bool content_text = false;
};

struct Object {
  std::string kind;
  std::string name;
  std::string signature;
  std::optional<std::string> summary;
  std::optional<std::string> aliases;
  std::string path;
  std::string page;
  std::optional<std::string> category;
  std::string breadcrumb;
};

inline constexpr size_t kObjectFields = 9;

std::array<std::optional<std::string_view>, kObjectFields> ObjectFields(
  const Object& object);

using ObjectRow = std::array<std::optional<std::string>, kObjectFields>;

Object ObjectFromFields(ObjectRow fields);
std::string EncodeObjects(std::span<const Object> objects);

std::vector<Object> DecodeObjects(std::string_view text);

std::size_t HeadingDepth(std::string_view path);

std::string_view CallName(std::string_view term);

std::string SiteRoute(std::string_view link);

std::string Markdown(const Entry& entry);

std::string Snippet(std::string_view text, size_t limit);

void Publish(duckdb::DatabaseInstance& db, std::vector<IndexBlob> image);

std::optional<Entry> EntryAt(duckdb::DatabaseInstance& db,
                             std::string_view path, Columns columns);

std::optional<Entry> FindByPath(duckdb::DatabaseInstance& db,
                                std::string_view path);

std::optional<Entry> ResolveLink(duckdb::DatabaseInstance& db,
                                 std::string_view link,
                                 std::string_view base = {},
                                 Columns columns = {});

std::vector<Entry> Lookup(duckdb::DatabaseInstance& db, std::string_view name,
                          Columns columns = {});

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
                              Columns columns = {});

std::vector<Entry> Children(duckdb::DatabaseInstance& db,
                            std::string_view path);

std::vector<std::string> ListPaths(duckdb::DatabaseInstance& db,
                                   std::string_view prefix);

std::vector<std::string> CompletePath(duckdb::DatabaseInstance& db,
                                      std::string_view prefix, size_t limit);

std::vector<Entry> Search(duckdb::DatabaseInstance& db, std::string_view query,
                          size_t limit, Columns columns, std::string& error);

std::vector<Entry> Candidates(duckdb::DatabaseInstance& db,
                              std::string_view name, size_t limit);

}  // namespace sdb::docs
