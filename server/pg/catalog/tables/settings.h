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
#include <string_view>

namespace sdb::pg {

struct Guc {
  std::string_view name;
  std::string_view setting;
  std::string_view unit;
  std::string_view category;
  std::string_view short_desc;
  std::string_view extra_desc;
  std::string_view context;
  std::string_view vartype;
  std::string_view min_val;
  std::string_view max_val;
  std::span<const std::string_view> enumvals;
};

#include "pg/catalog/tables/settings.gen.inc"

const Guc* FindGuc(std::string_view name);

}  // namespace sdb::pg
