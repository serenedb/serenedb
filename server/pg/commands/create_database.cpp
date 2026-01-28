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

#include <yaclib/async/make.hpp>

#include "catalog/databases.h"
#include "pg/commands.h"

namespace sdb::pg {

yaclib::Future<Result> CreateDatabase(ExecContext& context,
                                      const CreatedbStmt& stmt) {
  auto r = catalog::CreateDatabase(
    context, catalog::DatabaseOptions{.name = stmt.dbname});
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
