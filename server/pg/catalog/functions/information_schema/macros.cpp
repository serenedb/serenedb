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

#include "pg/catalog/functions/functions.h"

namespace sdb::pg {
namespace {

constexpr SystemMacro kMacros[] = {
  // clang-format off
  // Expand any 1-D array into a set with integers 1..N

  // TODO(mbkkt): rewrite once parser supports PG-style OUT params
  {"information_schema", "_pg_expandarray",
   R"((arr) AS TABLE SELECT unnest AS x, ordinality AS n FROM unnest(arr) WITH ORDINALITY)"},
  {"information_schema", "_pg_truetypid",
   R"((att, typ) AS CASE WHEN typ.typtype = 'd' THEN typ.typbasetype ELSE att.atttypid END)"},
  {"information_schema", "_pg_truetypmod",
   R"((att, typ) AS CASE WHEN typ.typtype = 'd' THEN typ.typtypmod ELSE att.atttypmod END)"},
  // clang-format on
};

}  // namespace

const std::span<const SystemMacro> kInformationSchemaMacros = kMacros;

}  // namespace sdb::pg
