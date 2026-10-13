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

#pragma once

#include <duckdb/catalog/catalog_entry/subscription_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_subscription_info.hpp>
#include <span>
#include <vector>

namespace sdb::catalog {

class SubscriptionCatalogEntry final : public duckdb::SubscriptionCatalogEntry {
 public:
  SubscriptionCatalogEntry(duckdb::Catalog& catalog,
                           duckdb::CreateSubscriptionInfo& info);
  SubscriptionCatalogEntry(
    duckdb::Catalog& catalog, duckdb::CreateSubscriptionInfo& info,
    duckdb::shared_ptr<duckdb::ReplicationLsnState> lsn_state);

  const duckdb::CreateSubscriptionInfo& Config() const noexcept {
    return *_info;
  }
  std::vector<duckdb::SubscriptionRelation> Relations() const;

  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::ClientContext& context, duckdb::AlterInfo& info) final;
  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;
  std::string ToSQL() const final { return GetInfo()->ToString(); }

 private:
  void FoldSynced(std::span<duckdb::SubscriptionRelation> relations) const;

  duckdb::unique_ptr<duckdb::CreateSubscriptionInfo> _info;
};

}  // namespace sdb::catalog
