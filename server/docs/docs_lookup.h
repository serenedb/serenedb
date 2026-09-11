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
#include <string>
#include <string_view>
#include <vector>

#include "docs/docs_data.h"

namespace sdb::docs {

const Doc* FindByPath(std::string_view path);

std::vector<const Doc*> Lookup(std::string_view name);

std::vector<const Doc*> Similar(std::string_view name, size_t limit);

std::vector<const Doc*> ListPrefix(std::string_view prefix, bool pages_only);

std::vector<const Doc*> Children(std::string_view path);

std::vector<std::string> CompletePath(std::string_view prefix,
                                      size_t limit);

std::string ResolveDocLink(std::string_view base_path, std::string_view href);

std::size_t HeadingDepth(std::string_view path);

}  // namespace sdb::docs
