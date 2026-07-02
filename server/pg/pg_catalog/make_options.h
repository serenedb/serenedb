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

#include <absl/strings/ascii.h>

#include <deque>
#include <string>
#include <string_view>
#include <vector>

#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {

// Build "key=value" option strings, redacting any secret value so credentials
// are not dumped into the catalog (clickhouse accepts both `password` and
// `passwd`). The strings are appended to `storage` (stable addresses) and a
// span over the row's slice is returned. Shared by the pg_foreign_server and
// pg_user_mapping system tables, whose objects both expose GetOptionKeys()/
// GetOptionValues().
template<typename Object>
Array<Text> MakeOptions(const Object& object, std::deque<std::string>& storage,
                        std::deque<std::vector<std::string_view>>& spans) {
  const auto& keys = object.GetOptionKeys();
  const auto& vals = object.GetOptionValues();
  auto& span = spans.emplace_back();
  span.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    std::string entry = keys[i];
    entry += '=';
    const auto lkey = absl::AsciiStrToLower(keys[i]);
    if (lkey != "password" && lkey != "passwd") {
      entry += vals[i];
    }
    storage.push_back(std::move(entry));
    span.push_back(storage.back());
  }
  return Array<Text>{span};
}

}  // namespace sdb::pg
