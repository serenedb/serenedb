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
#include <cstdint>
#include <span>
#include <string_view>

#include "pg/catalog/oids.h"

namespace sdb::pg {

struct BuiltinType {
  int32_t oid;
  std::string_view name;
  duckdb::idx_t nsp;
  int16_t len;
  bool byval;
  char type;
  char category;
  bool preferred;
  char delim;
  int32_t subscript;
  int32_t elem;
  int32_t array;
  int32_t input;
  int32_t output;
  int32_t receive;
  int32_t send;
  int32_t modin;
  int32_t modout;
  int32_t analyze;
  char align;
  char storage;
  int32_t collation;
  int32_t basetype;
  int32_t relid;
  int32_t typmod;
  bool notnull;
  std::string_view default_text;

  constexpr bool IsArray() const noexcept {
    return elem != 0 && subscript == kArraySubscriptHandler && storage != 'p';
  }
};

struct BuiltinProc {
  int32_t oid;
  std::string_view name;
  int16_t nargs;
  int32_t rettype;
  std::array<int32_t, 3> argtypes;
  char kind;
  bool retset;
  char volatility;
  bool strict;
  int32_t lang;
  std::string_view src;
};

struct BuiltinCollation {
  int32_t oid;
  std::string_view name;
  char provider;
  std::string_view locale;
  bool deterministic = true;
};

std::span<const BuiltinType> BuiltinTypes();
const BuiltinType* FindBuiltinType(int32_t oid);
const BuiltinType* FindBuiltinType(duckdb::idx_t nsp, std::string_view name);
const BuiltinType* FindBuiltinRowType(duckdb::idx_t relid);

std::span<const BuiltinProc> BuiltinProcs();
int32_t BuiltinProcOid(std::string_view name);

std::span<const BuiltinCollation> BuiltinCollations();
const BuiltinCollation* FindBuiltinCollation(int64_t oid);
int32_t CollationOid(std::string_view collation);

}  // namespace sdb::pg
