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
#include <cstdint>
#include <duckdb/main/database.hpp>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::connector {

struct MarkdownLink {
  std::string label;
  std::string page;
  std::string anchor;
  std::string url;
};

struct MarkdownLinks {
  std::string_view site;
  size_t first = 1;
  std::vector<MarkdownLink> links;
};

std::string RenderMarkdown(std::string_view markdown, int32_t width, bool color,
                           std::string_view base_path,
                           MarkdownLinks* links = nullptr);

std::string EscapeMarkdown(std::string_view text);

std::string ResolveHref(std::string_view base_path, std::string_view href);

bool IsExternal(std::string_view href);

std::string AbsoluteLinks(std::string_view markdown,
                          std::string_view base_path);

void RegisterMarkdownRenderFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
