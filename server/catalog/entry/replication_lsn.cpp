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

#include "catalog/entry/replication_lsn.h"

#include <iresearch/utils/assert.hpp>
#include <utility>

namespace sdb::catalog {

ReplicationLsnEntry::ReplicationLsnEntry(
  duckdb::CatalogType type, duckdb::Catalog& catalog, duckdb::Identifier name,
  duckdb::idx_t oid, std::shared_ptr<ReplicationLsnState> lsn_state)
  : duckdb::InCatalogEntry{type, catalog, std::move(name), oid},
    _lsn_state{std::move(lsn_state)} {
  SDB_ASSERT(_lsn_state);
}

void ReplicationLsnEntry::RaiseRemoteLsn(uint64_t lsn) {
  auto current = _lsn_state->remote_lsn.load();
  while (lsn > current &&
         !_lsn_state->remote_lsn.compare_exchange_weak(current, lsn)) {
  }
}

void ReplicationLsnEntry::MarkRelationSynced(duckdb::idx_t relation,
                                             uint64_t lsn) {
  absl::MutexLock lock{&_lsn_state->relation_mutex};
  _lsn_state->relation_lsns[relation] = lsn;
}

std::optional<uint64_t> ReplicationLsnEntry::RelationSyncedLsn(
  duckdb::idx_t relation) const {
  absl::MutexLock lock{&_lsn_state->relation_mutex};
  const auto it = _lsn_state->relation_lsns.find(relation);
  if (it == _lsn_state->relation_lsns.end()) {
    return std::nullopt;
  }
  return it->second;
}

ReplicationOriginEntry::ReplicationOriginEntry(
  duckdb::Catalog& catalog, duckdb::CreateReplicationOriginInfo& info)
  : ReplicationLsnEntry{
      duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY, catalog,
      info.GetQualifiedName().Name(), info.oid,
      std::make_shared<ReplicationLsnState>(info.remote_lsn)} {
  comment = info.comment;
  tags = info.tags;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CreateInfo> ReplicationOriginEntry::GetInfo() const {
  auto info = duckdb::make_uniq<duckdb::CreateReplicationOriginInfo>();
  info->SetName(name);
  info->oid = oid;
  info->remote_lsn = RemoteLsn();
  info->permissions = permissions;
  info->comment = comment;
  info->tags = tags;
  return info;
}

duckdb::unique_ptr<duckdb::CatalogEntry> ReplicationOriginEntry::Copy(
  duckdb::ClientContext&) const {
  auto info = GetInfo();
  return duckdb::make_uniq<ReplicationOriginEntry>(
    catalog, info->Cast<duckdb::CreateReplicationOriginInfo>());
}

}  // namespace sdb::catalog
