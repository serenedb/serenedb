////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <span>
#include <string>
#include <string_view>

namespace sdb::pg {

struct OptionInfo {
  std::string_view name;
  std::string_view type;           // "string", "boolean", "integer", "character"
  std::string_view description;
  std::string_view default_value;  // empty if none
};

struct OptionGroup {
  std::string_view name;
  std::span<const OptionInfo> options;     // leaf options in this group
  std::span<const OptionGroup> subgroups;  // nested groups
};

std::string FormatHelp(std::span<const OptionGroup> groups);

}  // namespace sdb::pg
