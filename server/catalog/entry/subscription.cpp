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

#include "catalog/entry/subscription.h"

#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <utility>

#include "catalog/transaction.h"

namespace sdb::catalog {

SubscriptionCatalogEntry::SubscriptionCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateSubscriptionInfo& info)
  : SubscriptionCatalogEntry{
      catalog, info, std::make_shared<ReplicationLsnState>(info.remote_lsn)} {}

SubscriptionCatalogEntry::SubscriptionCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateSubscriptionInfo& info,
  std::shared_ptr<ReplicationLsnState> lsn_state)
  : ReplicationLsnEntry{duckdb::CatalogType::SUBSCRIPTION_ENTRY, catalog,
                        info.GetQualifiedName().Name(), info.oid,
                        std::move(lsn_state)},
    _info{
      duckdb::unique_ptr_cast<duckdb::CreateInfo,
                              duckdb::CreateSubscriptionInfo>(info.Copy())} {
  RaiseRemoteLsn(info.remote_lsn);
  comment = info.comment;
  tags = info.tags;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SubscriptionCatalogEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  if (info.type != duckdb::AlterType::REPLACE_DEFINITION) {
    return ReplicationLsnEntry::AlterEntry(context, info);
  }
  auto& definition = info.Cast<duckdb::ReplaceDefinitionInfo>()
                       .definition->Cast<duckdb::CreateSubscriptionInfo>();
  return duckdb::make_uniq<SubscriptionCatalogEntry>(catalog, definition,
                                                     LsnState());
}

void SubscriptionCatalogEntry::FoldSynced(
  std::span<duckdb::SubscriptionRelation> relations,
  duckdb::transaction_t snapshot) const {
  for (auto& relation : relations) {
    if (relation.state == 'r' || relation.sync_id == 0) {
      continue;
    }
    if (const auto lsn = RelationSyncedLsn(relation.sync_id, snapshot)) {
      relation.state = 'r';
      relation.lsn = *lsn;
    }
  }
}

std::vector<duckdb::SubscriptionRelation> SubscriptionCatalogEntry::Relations(
  duckdb::transaction_t snapshot) const {
  std::vector<duckdb::SubscriptionRelation> relations{_info->relations.begin(),
                                                      _info->relations.end()};
  FoldSynced(relations, snapshot);
  return relations;
}

duckdb::unique_ptr<duckdb::CreateInfo> SubscriptionCatalogEntry::GetInfo()
  const {
  auto info = _info->Copy();
  auto& subscription = info->Cast<duckdb::CreateSubscriptionInfo>();
  subscription.SetName(name);
  subscription.permissions = permissions;
  subscription.comment = comment;
  subscription.tags = tags;
  FoldSynced(subscription.relations, Transactions::Get().LastCommit());
  subscription.remote_lsn = RemoteLsn();
  return info;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SubscriptionCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<SubscriptionCatalogEntry>(
    catalog, info->Cast<duckdb::CreateSubscriptionInfo>(), LsnState());
}

}  // namespace sdb::catalog
