////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>

#include "catalog/types.h"

namespace vpack {

class Builder;
class Slice;

}  // namespace vpack
namespace sdb {
namespace transaction {

struct Options {
  Options();

  /// returns default options used in tailing sync replication
  static Options replicationDefaults();

  /// adjust the global default values for transactions
  static void setLimits(uint64_t max_transaction_size,
                        uint64_t intermediate_commit_size,
                        uint64_t intermediate_commit_count);

  /// read the options from a vpack slice
  void fromVPack(vpack::Slice slice);

  /// add the options to an opened vpack builder
  void toVPack(vpack::Builder&) const;

#ifdef SDB_FAULT_INJECTION
  /// patch intermediateCommitCount for testing
  static void adjustIntermediateCommitCount(Options& options);
#endif

  bool isIntermediateCommitEnabled() const noexcept;

  static constexpr double kDefaultLockTimeout = 900.0;
  static uint64_t
    gDefaultMaxTransactionSize;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
  static uint64_t
    gDefaultIntermediateCommitSize;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
  static uint64_t
    gDefaultIntermediateCommitCount;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

  /// time (in seconds) that is spent waiting for a lock
  double lock_timeout = kDefaultLockTimeout;
  uint64_t max_transaction_size = gDefaultMaxTransactionSize;
  uint64_t intermediate_commit_size = gDefaultIntermediateCommitSize;
  uint64_t intermediate_commit_count = gDefaultIntermediateCommitCount;

  /// originating server of this transaction. will be populated
  /// only in the cluster, and with a coordinator id/coordinator reboot id
  /// then. coordinators fill this in when they start a transaction, and
  /// the info is send with the transaction begin requests to DB servers,
  /// which will also store the coordinator's id. this is so they can
  /// abort the transaction should the coordinator die or be rebooted.
  /// the server id and reboot id are intentionally empty in single server
  /// case.
  sdb::PeerState origin;

  bool allow_implicit_collections_for_read = true;
  bool allow_implicit_collections_for_write = false;  // replication only!
  bool wait_for_sync = false;
  bool fill_block_cache = true;
  bool is_follower_transaction = false;
  /// The following flag indicates if a transaction is allowed to perform
  /// dirty reads (aka read-from-followers). This is stored in the
  /// `TransactionState`. The decision is taken when the transaction is
  /// created.
  bool allow_dirty_reads = false;

  /// determines whether this transaction requires the changes to be
  /// replicated. E.g., transactions that _must not_ be replicated are those
  /// that create/drop indexes.
  /// This option should be set to false for read-only transactions, because
  /// it allows us to use a the more efficient SimpleRocksDBTransactionState
  /// on the leader.
  ///
  /// This option is only relevant for replication 2.0
  bool requires_replication = true;

  /// If set to true, the transaction is started without acquiring a
  /// snapshot. The snapshot can be acquired at a later point by calling
  /// `ensureSnapshot`. This allows us to lock the used keys before the
  /// snapshot is acquired in order to avoid write-write conflict.
  bool delay_snapshot = false;

  /// if set to true, skips the fast, unordered lock round and always
  /// uses the sequential, ordered lock round.
  /// if set to false, the fast lock round may be tried, depending on the
  /// context of the transaction.
  bool skip_fast_lock_round = false;
};

}  // namespace transaction
}  // namespace sdb
