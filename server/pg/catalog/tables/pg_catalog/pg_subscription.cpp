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

#include <duckdb/main/attached_database.hpp>

#include "catalog/entry/subscription.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kSubscriptionIndexes[] = {
  {kPgSubscriptionSql["oid"], SystemLookup::Object},
  {kPgSubscriptionSql["subname"], SystemLookup::Object},
};

char Substream(std::string_view streaming) {
  if (streaming == "parallel") {
    return 'p';
  }
  return streaming == "on" ? 't' : 'f';
}

class PgSubscription final : public SystemTableScan<kPgSubscriptionSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Database, duckdb::CatalogType::SUBSCRIPTION_ENTRY,
    kSubscriptionIndexes}};

  static constexpr auto kSubscription =
    Shape<kSql, const catalog::SubscriptionCatalogEntry>(
      Col<"oid">(&duckdb::CatalogEntry::oid),
      Col<"subdbid">([](const auto& subscription) {
        return subscription.ParentCatalog().GetAttached().oid;
      }),
      Col<"subskiplsn">([](const auto& subscription) {
        const auto skip = subscription.Config().skip_lsn;
        return skip > subscription.RemoteLsn() ? skip : uint64_t{0};
      }),
      Col<"subname">(&duckdb::CatalogEntry::name), Col<"subowner">(kOwner),
      Col<"subenabled">(
        [](const auto& subscription) { return subscription.Config().enabled; }),
      Col<"subbinary">(
        [](const auto& subscription) { return subscription.Config().binary; }),
      Col<"substream">([](const auto& subscription) {
        return Substream(subscription.Config().streaming);
      }),
      Col<"subtwophasestate">([](const auto&) { return 'd'; }),
      Col<"subdisableonerr">([](const auto& subscription) {
        return subscription.Config().disable_on_error;
      }),
      Col<"subpasswordrequired">([](const auto& subscription) {
        return subscription.Config().password_required;
      }),
      Col<"subrunasowner">([](const auto& subscription) {
        return subscription.Config().run_as_owner;
      }),
      Col<"subfailover">([](const auto& subscription) {
        return subscription.Config().failover;
      }),
      Col<"subconninfo">([](const auto& subscription) -> std::string_view {
        return subscription.Config().conninfo;
      }),
      Col<"subslotname">(
        [](const auto& subscription) -> std::optional<std::string_view> {
          return NonEmpty(std::string_view{subscription.Config().slot_name});
        }),
      Col<"subsynccommit">([](const auto& subscription) -> std::string_view {
        return subscription.Config().synchronous_commit;
      }),
      Col<"subpublications">([](const auto& subscription) -> const auto& {
        return subscription.Config().publications;
      }),
      Col<"suborigin">([](const auto& subscription) -> std::string_view {
        return subscription.Config().origin;
      }));

  void Row(const catalog::SubscriptionCatalogEntry& subscription) {
    Emit<kSubscription>(subscription);
  }
};

}  // namespace

SystemTable gPgSubscription = SystemTableOf<PgSubscription>();

}  // namespace sdb::pg
