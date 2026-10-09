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

namespace sdb::catalog {
namespace {

SubscriptionConfig MakeConfig(const duckdb::CreateSubscriptionInfo& info) {
  return {
    .conninfo = info.conninfo,
    .publications = {info.publications.begin(), info.publications.end()},
    .slot_name = info.slot_name,
    .enabled = info.enabled,
    .binary = info.binary,
    .copy_data = info.copy_data,
    .create_slot = info.create_slot,
    .disable_on_error = info.disable_on_error,
    .password_required = info.password_required,
    .run_as_owner = info.run_as_owner,
    .failover = info.failover,
    .origin = info.origin,
    .synchronous_commit = info.synchronous_commit,
    .streaming = info.streaming,
    .skip_lsn = info.skip_lsn,
    .relations = {info.relations.begin(), info.relations.end()},
  };
}

}  // namespace

SubscriptionCatalogEntry::SubscriptionCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateSubscriptionInfo& info)
  : SubscriptionCatalogEntry{
      catalog, info,
      duckdb::make_shared_ptr<duckdb::ReplicationLsnState>(info.remote_lsn)} {}

SubscriptionCatalogEntry::SubscriptionCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateSubscriptionInfo& info,
  duckdb::shared_ptr<duckdb::ReplicationLsnState> lsn_state)
  : duckdb::SubscriptionCatalogEntry{catalog, info.GetQualifiedName().Name(),
                                     info.oid, std::move(lsn_state)},
    _config{MakeConfig(info)} {
  RaiseRemoteLsn(info.remote_lsn);
  comment = info.comment;
  tags = info.tags;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SubscriptionCatalogEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  if (info.type != duckdb::AlterType::REPLACE_DEFINITION) {
    return duckdb::SubscriptionCatalogEntry::AlterEntry(context, info);
  }
  auto& definition = info.Cast<duckdb::ReplaceDefinitionInfo>()
                       .definition->Cast<duckdb::CreateSubscriptionInfo>();
  return duckdb::make_uniq<SubscriptionCatalogEntry>(catalog, definition,
                                                     LsnState());
}

std::vector<duckdb::SubscriptionRelation> SubscriptionCatalogEntry::Relations()
  const {
  auto relations = _config.relations;
  for (auto& relation : relations) {
    if (relation.state == 'r' || relation.sync_id == 0) {
      continue;
    }
    if (const auto lsn = RelationSyncedLsn(relation.sync_id)) {
      relation.state = 'r';
      relation.lsn = *lsn;
    }
  }
  return relations;
}

duckdb::unique_ptr<duckdb::CreateInfo> SubscriptionCatalogEntry::GetInfo()
  const {
  auto info = duckdb::make_uniq<duckdb::CreateSubscriptionInfo>();
  info->SetName(name);
  info->permissions = permissions;
  info->conninfo = _config.conninfo;
  info->publications = {_config.publications.begin(),
                        _config.publications.end()};
  info->slot_name = _config.slot_name;
  info->enabled = _config.enabled;
  info->binary = _config.binary;
  info->copy_data = _config.copy_data;
  info->create_slot = _config.create_slot;
  info->disable_on_error = _config.disable_on_error;
  info->password_required = _config.password_required;
  info->run_as_owner = _config.run_as_owner;
  info->failover = _config.failover;
  info->origin = _config.origin;
  info->synchronous_commit = _config.synchronous_commit;
  info->streaming = _config.streaming;
  auto relations = Relations();
  info->relations = {std::make_move_iterator(relations.begin()),
                     std::make_move_iterator(relations.end())};
  info->remote_lsn = RemoteLsn();
  info->skip_lsn = _config.skip_lsn;
  info->comment = comment;
  info->tags = tags;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> SubscriptionCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<SubscriptionCatalogEntry>(
    catalog, info->Cast<duckdb::CreateSubscriptionInfo>(), LsnState());
}

}  // namespace sdb::catalog
