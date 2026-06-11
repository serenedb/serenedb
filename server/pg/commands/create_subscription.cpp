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

#include "create_subscription.h"

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {

void CreateSubscription(ConnectionContext& conn_ctx, std::string_view name,
                        std::string_view conninfo,
                        std::vector<std::string>&& publications) {
  auto db_id = conn_ctx.GetDatabaseId();

  auto& catalog = catalog::CatalogFeature::instance().Local();

  catalog::Subscription::Config cfg;
  cfg.conninfo = conninfo;
  cfg.publications = std::move(publications);
  cfg.slot_name = name;

  auto subscription = std::make_shared<catalog::Subscription>(
    db_id, catalog::NextId(), name, cfg);

  auto r = catalog.CreateSubscription(db_id, subscription);

  if (!r.ok()) {
    THROW_SQL_ERROR(ERR_MSG(r.errorMessage()));
  }
}

}  // namespace sdb::pg
