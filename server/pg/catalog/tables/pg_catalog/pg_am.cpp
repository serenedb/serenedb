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

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct Am {
  duckdb::idx_t oid;
  std::string_view name;
  char type;
  duckdb::idx_t handler;
};

constexpr std::array kAms{
  Am{kPgAmHeap, "heap", 't', kPgAmHeapHandler},
  Am{kPgAmInverted, "inverted", 'i', kInvalidOid},
  Am{kPgAmIResearch, "iresearch", 't', kInvalidOid},
  Am{kPgAmSecondary, "secondary", 'i', kInvalidOid},
};

class PgAm final : public SystemTableScan<kPgAmSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{ArraySource<Am>{&LoadStatic<kAms>, {}}};

  static constexpr auto kAm = Shape<kSql, const Am>(
    Col<"oid">(&Am::oid), Col<"amname">(&Am::name),
    Col<"amhandler">(&Am::handler), Col<"amtype">(&Am::type));

  void Row(const Am& am) { Emit<kAm>(am); }
};

}  // namespace

SystemTable gPgAm = SystemTableOf<PgAm>();

}  // namespace sdb::pg
