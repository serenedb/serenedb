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

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_replication_origin_info.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <optional>

namespace sdb::catalog {

struct SyncedRelation {
  uint64_t lsn = 0;
  duckdb::transaction_t commit = 0;
};

struct ReplicationLsnState {
  explicit ReplicationLsnState(uint64_t lsn) : remote_lsn{lsn} {}

  std::atomic<uint64_t> remote_lsn;
  absl::Mutex relation_mutex;
  irs::containers::FlatHashMap<duckdb::idx_t, SyncedRelation> relation_lsns
    ABSL_GUARDED_BY(relation_mutex);
};

class ReplicationLsnEntry : public duckdb::InCatalogEntry {
 public:
  ReplicationLsnEntry(duckdb::CatalogType type, duckdb::Catalog& catalog,
                      duckdb::Identifier name, duckdb::idx_t oid,
                      std::shared_ptr<ReplicationLsnState> lsn_state);

  uint64_t RemoteLsn() const { return _lsn_state->remote_lsn.load(); }
  void RaiseRemoteLsn(uint64_t lsn);
  void AssignRemoteLsn(uint64_t lsn) { _lsn_state->remote_lsn.store(lsn); }
  const std::shared_ptr<ReplicationLsnState>& LsnState() const {
    return _lsn_state;
  }
  void MarkRelationSynced(duckdb::idx_t relation, uint64_t lsn,
                          duckdb::transaction_t commit);
  std::optional<uint64_t> RelationSyncedLsn(
    duckdb::idx_t relation, duckdb::transaction_t snapshot) const;

 private:
  std::shared_ptr<ReplicationLsnState> _lsn_state;
};

class ReplicationOriginEntry final : public ReplicationLsnEntry {
 public:
  static constexpr duckdb::CatalogType Type =
    duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY;
  static constexpr const char* Name = "replication origin";

  ReplicationOriginEntry(duckdb::Catalog& catalog,
                         duckdb::CreateReplicationOriginInfo& info);

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;
  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;
};

}  // namespace sdb::catalog
