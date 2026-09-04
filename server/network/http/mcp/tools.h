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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <yaclib/coro/task.hpp>

#include "network/http/handler.h"

namespace sdb::network::http::mcp {

struct ToolArgs {
  std::optional<std::string> query;
  std::optional<std::string> path;
  std::optional<std::string> section;
  std::optional<std::string> prefix;
  std::optional<int64_t> limit;
};

struct ToolResult {
  std::string text;
  bool is_error = false;
};

std::string_view ToolsListJson();

bool KnownTool(std::string_view name);

yaclib::Task<ToolResult> CallTool(RequestContext& ctx, std::string_view name,
                                  const ToolArgs& args);

}  // namespace sdb::network::http::mcp
