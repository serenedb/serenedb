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

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

class PgForeignDataWrapper final
  : public SystemTableScan<kPgForeignDataWrapperSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<ForeignDataWrapper>{&LoadStatic<kForeignDataWrappers>, {}}};

  static constexpr auto kWrapper = Shape<kSql, const ForeignDataWrapper>(
    Col<"oid">(&ForeignDataWrapper::oid),
    Col<"fdwname">(&ForeignDataWrapper::name),
    Col<"fdwowner">([](const auto&) { return kRootUser; }),
    Col<"fdwacl">(
      [](const auto&) -> const auto& { return kForeignDataWrapperAcl; }));

  void Row(const ForeignDataWrapper& wrapper) { Emit<kWrapper>(wrapper); }
};

}  // namespace

SystemTable gPgForeignDataWrapper = SystemTableOf<PgForeignDataWrapper>();

}  // namespace sdb::pg
