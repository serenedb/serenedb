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
#include <string>
#include <vector>

namespace sdb::catalog {

struct SubscriptionConfig {
  std::string conninfo;
  std::vector<std::string> publications;
  std::string slot_name;
  bool enabled = true;
  bool binary = false;
  bool copy_data = true;
  bool create_slot = true;
  bool disable_on_error = false;
  bool password_required = true;
  bool run_as_owner = false;
  bool failover = false;
  std::string origin = "any";
  std::string synchronous_commit = "off";
  std::string streaming = "off";
  uint64_t skip_lsn = 0;
  std::vector<duckdb::SubscriptionRelation> relations;
};

class SubscriptionCatalogEntry final : public duckdb::SubscriptionCatalogEntry {
 public:
  SubscriptionCatalogEntry(duckdb::Catalog& catalog,
                           duckdb::CreateSubscriptionInfo& info);
  SubscriptionCatalogEntry(
    duckdb::Catalog& catalog, duckdb::CreateSubscriptionInfo& info,
    duckdb::shared_ptr<duckdb::ReplicationLsnState> lsn_state);

  const SubscriptionConfig& Config() const noexcept { return _config; }
  std::vector<duckdb::SubscriptionRelation> Relations() const;

  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::ClientContext& context, duckdb::AlterInfo& info) final;
  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;
  std::string ToSQL() const final { return GetInfo()->ToString(); }

 private:
  SubscriptionConfig _config;
};

}  // namespace sdb::catalog
