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

#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr ArrayKey<BuiltinFunction, BuiltinFunctions> kAggregateKeys[] = {
  {kPgAggregateSql["aggfnoid"], kBuiltinsByOid},
};

class PgAggregate final : public SystemTableScan<kPgAggregateSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<BuiltinFunction, BuiltinFunctions>{&LoadBuiltins,
                                                   kAggregateKeys}};

  static constexpr auto kAggregate = Shape<kSql, const BuiltinFunction>(
    Col<"aggfnoid">(&BuiltinFunction::oid),
    Col<"aggtranstype">([](const auto&) { return kInternal; }));

  void Row(const BuiltinFunction& function) {
    if (function.kind == 'a') {
      Emit<kAggregate>(function);
    }
  }
};

}  // namespace

SystemTable gPgAggregate = SystemTableOf<PgAggregate>();

}  // namespace sdb::pg
