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

#include "catalog/catalog.h"
#include "catalog/subscription.h"
#include "connector/duckdb_client_state.h"
#include "pg/system_table.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgSubscription>::GetTableData() {
  auto cat = _config.EnsureCatalogSnapshot();
  auto db_id = GetDatabaseId();

  auto subs = cat->GetSubscriptions(db_id);

  auto result = CreateColumns<PgSubscription>(subs.size());
  duckdb::idx_t row = 0;
  for (const auto& sub : subs) {
    const auto& cfg = sub->GetConfig();

    Array<Text> pubs;
    // for (const auto& p : cfg.publications) {
    //   pubs.push_back(Text{p});
    // }

    PgSubscription pg_row{
      .oid = static_cast<Oid>(sub->GetId().id()),
      .subdbid = static_cast<Oid>(db_id.id()),
      .subskiplsn = Empty{},
      .subname = Name{sub->GetName()},
      .subowner = Oid{10},
      .subenabled = cfg.enabled,
      .subbinary = cfg.binary,
      .substream = PgSubscription::Substream::Disallow,
      .subtwophasestate = PgSubscription::Subtwophasestate::Disabled,
      .subdisableonerr = false,
      .subpasswordrequired = false,
      .subrunasowner = false,
      .subfailover = false,
      .subconninfo = Text{cfg.conninfo},
      .subslotname = Name{cfg.slot_name},
      .subsynccommit = Text{"off"},
      .subpublications = pubs,
      .suborigin = Text{cfg.origin_name},
    };
    WriteData(result, pg_row, /*null_mask=*/0, row);
    ++row;
  }
  return {std::move(result), row};
}

}  // namespace sdb::pg
