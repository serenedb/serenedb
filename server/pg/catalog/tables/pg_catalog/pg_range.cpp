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

#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct Range {
  duckdb::idx_t type;
  duckdb::idx_t subtype;
  duckdb::idx_t multirange;
  duckdb::idx_t collation;
  duckdb::idx_t opclass;
  duckdb::idx_t canonical;
  duckdb::idx_t subdiff;
};

constexpr Range kRanges[] = {
#include "pg/catalog/generated/builtin_ranges.gen.inc"
};

constexpr auto kRange = Shape<kPgRangeSql, const Range>(
  Col<"rngtypid">(&Range::type), Col<"rngsubtype">(&Range::subtype),
  Col<"rngmultitypid">(&Range::multirange),
  Col<"rngcollation">(&Range::collation), Col<"rngsubopc">(&Range::opclass),
  Col<"rngcanonical">(&Range::canonical), Col<"rngsubdiff">(&Range::subdiff));

}  // namespace

SystemTable gPgRange = SystemTableOf<StaticTable<kRange, kRanges>>();

}  // namespace sdb::pg
