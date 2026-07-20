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

#include "pg/pg_catalog/pg_subscription.h"

#include <span>
#include <string_view>

#include "catalog/catalog.h"
#include "catalog/subscription.h"
#include "connector/duckdb_client_state.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_table.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgSubscription>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();
  auto db_id = GetDatabaseId();

  auto subs = catalog->GetSubscriptions(db_id);

  auto result = CreateColumns<PgSubscription>(subs.size());
  duckdb::idx_t row = 0;
  for (const auto& sub : subs) {
    const auto& cfg = sub->GetConfig();

    std::vector<Text> pubs;
    pubs.reserve(cfg.publications.size());
    for (const auto& pub : cfg.publications) {
      pubs.push_back(Text{pub});
    }

    PgSubscription pg_row{
      .oid = static_cast<Oid>(sub->GetId().id()),
      .subdbid = static_cast<Oid>(db_id.id()),
      .subskiplsn = Empty{},
      .subname = Name{sub->GetName()},
      .subowner = static_cast<Oid>(sub->GetOwner().id()),
      .subenabled = cfg.enabled,
      .subbinary = cfg.binary,
      .substream = PgSubscription::Substream::Disallow,
      .subtwophasestate = PgSubscription::Subtwophasestate::Disabled,
      .subdisableonerr = cfg.disable_on_error,
      .subpasswordrequired = cfg.password_required,
      .subrunasowner = cfg.run_as_owner,
      .subfailover = cfg.failover,
      .subconninfo = Text{cfg.conninfo},
      .subslotname = Name{cfg.slot_name},
      .subsynccommit = Text{cfg.synchronous_commit},
      .subpublications = pubs,
      .suborigin = Text{cfg.origin_name},
    };
    WriteData(result, pg_row, /*null_mask=*/0, row, *catalog);
    ++row;
  }
  return {std::move(result), row};
}

}  // namespace sdb::pg
