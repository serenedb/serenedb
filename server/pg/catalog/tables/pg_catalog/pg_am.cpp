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

constexpr auto kAm = Shape<kPgAmSql, const SystemAccessMethod>(
  Col<"oid">(&SystemAccessMethod::oid),
  Col<"amname">(&SystemAccessMethod::name),
  Col<"amhandler">(&SystemAccessMethod::handler),
  Col<"amtype">(&SystemAccessMethod::type));

}  // namespace

SystemTable gPgAm = SystemTableOf<StaticTable<kAm, kSystemAccessMethods>>();

}  // namespace sdb::pg
