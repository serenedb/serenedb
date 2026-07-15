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

#include <absl/functional/function_ref.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/assert.h"

namespace sdb::catalog {

// An FDW OPTIONS (...) list (CREATE SERVER / CREATE USER MAPPING): ordered as
// written, keys stored as ingested (the DDL layer lower-cases them). Owned by
// ForeignServer and UserMapping; persisted as the two parallel vectors.
class Options {
 public:
  Options() = default;
  Options(std::vector<std::string> keys, std::vector<std::string> values)
    : _keys{std::move(keys)}, _values{std::move(values)} {
    SDB_ASSERT(_keys.size() == _values.size());
  }

  // The one shared secrecy rule for option values (clickhouse accepts both
  // `password` and `passwd`), matched case-insensitively. Used by the pg
  // catalog views' redaction and any future dump tooling.
  static bool IsSecretKey(std::string_view key) {
    return absl::EqualsIgnoreCase(key, "password") ||
           absl::EqualsIgnoreCase(key, "passwd");
  }

  size_t Size() const noexcept { return _keys.size(); }
  bool Empty() const noexcept { return _keys.empty(); }

  void Visit(
    absl::FunctionRef<void(std::string_view key, std::string_view value)>
      visitor) const {
    for (size_t i = 0; i < _keys.size(); ++i) {
      visitor(_keys[i], _values[i]);
    }
  }

  // "key=value" strings in insertion order (the pg_foreign_server /
  // pg_user_mapping text[] shape). With `redact_secrets`, a secret key's
  // value is omitted -- rendered as `password=` -- keeping the key visible.
  std::vector<std::string> ToStrings(bool redact_secrets) const {
    std::vector<std::string> out;
    out.reserve(_keys.size());
    for (size_t i = 0; i < _keys.size(); ++i) {
      const bool redact = redact_secrets && IsSecretKey(_keys[i]);
      out.push_back(
        absl::StrCat(_keys[i], "=", redact ? std::string_view{} : _values[i]));
    }
    return out;
  }

  // Persistence accessors (ForeignServerData/UserMappingData keep the two
  // parallel vectors as their serialized shape).
  std::span<const std::string> Keys() const noexcept { return _keys; }
  std::span<const std::string> Values() const noexcept { return _values; }

 private:
  std::vector<std::string> _keys;
  std::vector<std::string> _values;
};

}  // namespace sdb::catalog
