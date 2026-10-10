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

#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

SystemRows<BuiltinCollation> LoadCollations(SystemScan&) {
  return BuiltinCollations();
}

template<char Provider>
constexpr auto kLocale = [](const BuiltinCollation& collation) {
  return collation.provider == Provider ? NonEmpty(collation.locale)
                                        : std::nullopt;
};

class PgCollation final : public SystemTableScan<kPgCollationSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<BuiltinCollation>{&LoadCollations, {}}};

  static constexpr auto kCollation = Shape<kSql, const BuiltinCollation>(
    Col<"oid">(&BuiltinCollation::oid),
    Col<"collname">(&BuiltinCollation::name),
    Col<"collnamespace">([](const auto&) { return kPgCatalogSchema; }),
    Col<"collowner">([](const auto&) { return kRootUser; }),
    Col<"collprovider">(&BuiltinCollation::provider),
    Col<"collisdeterministic">(&BuiltinCollation::deterministic),
    Col<"collcollate">(kLocale<'c'>), Col<"collctype">(kLocale<'c'>),
    Col<"colllocale">(kLocale<'i'>));

  void Row(const BuiltinCollation& collation) { Emit<kCollation>(collation); }
};

}  // namespace

SystemTable gPgCollation = SystemTableOf<PgCollation>();

}  // namespace sdb::pg
