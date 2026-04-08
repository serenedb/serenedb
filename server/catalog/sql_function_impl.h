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

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <string>

#include "basics/result.h"

namespace sdb::pg {

// Stores a SQL function/macro as its SQL text representation.
// On demand, DuckDB parses the SQL to build macro entries.
class FunctionImpl {
 public:
  explicit FunctionImpl(std::string sql) : _sql(std::move(sql)) {}

  static Result FromVPack(vpack::Slice slice,
                          std::unique_ptr<FunctionImpl>& out);

  void ToVPack(vpack::Builder& builder) const;

  std::string_view GetSQL() const noexcept { return _sql; }

 private:
  std::string _sql;  // e.g. "CREATE FUNCTION add1(x INT) RETURNS INT ..."
};

}  // namespace sdb::pg
