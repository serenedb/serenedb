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

#include <deque>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/subscription.h"
#include "pg/commands/create_subscription.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNoSkipLsn = MaskFromNulls({
  GetIndex(&PgSubscription::subskiplsn),
});
constexpr uint64_t kNoSlot = MaskFromNulls({
  GetIndex(&PgSubscription::subslotname),
});

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgSubscription>::GetTableData() {
  std::deque<std::vector<Text>> publications;
  std::deque<std::string> skip_lsns;
  std::vector<PgSubscription> values;
  std::vector<uint64_t> null_masks;

  auto& database = GetDatabase().Cast<catalog::SereneDBCatalog>();
  database.GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
    .Scan(database.GetCatalogTransaction(_context),
          [&](duckdb::CatalogEntry& entry) {
            const auto& subscription =
              entry.Cast<catalog::SubscriptionCatalogEntry>();
            const auto& config = subscription.Config();
            const auto& names = publications.emplace_back(
              config.publications.begin(), config.publications.end());
            const bool skips = config.skip_lsn > subscription.RemoteLsn();
            const auto& skip = skip_lsns.emplace_back(
              skips ? FormatLsn(config.skip_lsn) : std::string{});
            values.push_back(PgSubscription{
              .oid = subscription.oid,
              .subdbid = GetDatabaseId(),
              .subskiplsn = skip,
              .subname = subscription.name.GetIdentifierName(),
              .subowner = subscription.permissions.owner,
              .subenabled = config.enabled,
              .subbinary = config.binary,
              .substream = config.streaming == "parallel"
                             ? PgSubscription::Substream::Parallel
                           : config.streaming == "on"
                             ? PgSubscription::Substream::Spill
                             : PgSubscription::Substream::Disallow,
              .subtwophasestate = PgSubscription::Subtwophasestate::Disabled,
              .subdisableonerr = config.disable_on_error,
              .subpasswordrequired = config.password_required,
              .subrunasowner = config.run_as_owner,
              .subfailover = config.failover,
              .subconninfo = config.conninfo,
              .subslotname = config.slot_name,
              .subsynccommit = config.synchronous_commit,
              .subpublications = names,
              .suborigin = config.origin,
            });
            null_masks.push_back((skips ? 0 : kNoSkipLsn) |
                                 (config.slot_name.empty() ? kNoSlot : 0));
          });

  auto result = CreateColumns<PgSubscription>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], null_masks[row], row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
