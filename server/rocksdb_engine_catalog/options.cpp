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

#include "options.h"

#include <vpack/builder.h>
#include <vpack/slice.h>

#include "basics/debugging.h"
#include "basics/static_strings.h"
#include "general_server/state.h"

using namespace sdb::transaction;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t Options::gDefaultMaxTransactionSize =
  std::numeric_limits<decltype(Options::gDefaultMaxTransactionSize)>::max();
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t Options::gDefaultIntermediateCommitSize =
  uint64_t{512} * 1024 * 1024;  // 1 << 29
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t Options::gDefaultIntermediateCommitCount = 1'000'000;

Options::Options() {
#ifdef SDB_FAULT_INJECTION
  // patch intermediateCommitCount for testing
  adjustIntermediateCommitCount(*this);
#endif
}

Options Options::replicationDefaults() {
  Options options;
  // this is important, because when we get a "transaction begin" marker
  // we don't know which collections will participate in the transaction later.
  options.allow_implicit_collections_for_write = true;
  options.wait_for_sync = false;
  return options;
}

void Options::setLimits(uint64_t max_transaction_size,
                        uint64_t intermediate_commit_size,
                        uint64_t intermediate_commit_count) {
  gDefaultMaxTransactionSize = max_transaction_size;
  gDefaultIntermediateCommitSize = intermediate_commit_size;
  gDefaultIntermediateCommitCount = intermediate_commit_count;
}

bool Options::isIntermediateCommitEnabled() const noexcept {
  return intermediate_commit_size != UINT64_MAX ||
         intermediate_commit_count != UINT64_MAX;
}

void Options::fromVPack(vpack::Slice slice) {
  if (auto value = slice.get("lockTimeout"); value.isNumber()) {
    lock_timeout = value.getNumber<double>();
  }
  if (auto value = slice.get("maxTransactionSize"); value.isNumber()) {
    max_transaction_size = value.getNumber<uint64_t>();
  }
  if (auto value = slice.get("intermediateCommitSize"); value.isNumber()) {
    intermediate_commit_size = value.getNumber<uint64_t>();
  }
  if (auto value = slice.get("intermediateCommitCount"); value.isNumber()) {
    intermediate_commit_count = value.getNumber<uint64_t>();
  }
  // simon: 'allowImplicit' is due to naming in 'db._execute_transaction(...)'
  if (auto value = slice.get("allowImplicit"); value.isBool()) {
    allow_implicit_collections_for_read = value.isTrue();
  }
  if (auto value = slice.get(StaticStrings::kWaitForSyncString);
      value.isBool()) {
    wait_for_sync = value.isTrue();
  }
  if (auto value = slice.get("fillBlockCache"); value.isBool()) {
    fill_block_cache = value.isTrue();
  }
  if (auto value = slice.get("allowDirtyReads"); value.isBool()) {
    allow_dirty_reads = value.isTrue();
  } else {
    SDB_IF_FAILURE("TransactionState::dirtyReadsAreDefault") {
      allow_dirty_reads = true;
    }
  }
  if (auto value = slice.get("skipFastLockRound"); value.isBool()) {
    skip_fast_lock_round = value.isTrue();
  }
  // we are intentionally *not* reading allowImplicitCollectionForWrite here.
  // this is an internal option only used in replication

#ifdef SDB_FAULT_INJECTION
  // patch intermediateCommitCount for testing
  adjustIntermediateCommitCount(*this);
#endif
}

/// add the options to an opened vpack builder
void Options::toVPack(vpack::Builder& builder) const {
  SDB_ASSERT(builder.isOpenObject());

  builder.add("lockTimeout", lock_timeout);
  builder.add("maxTransactionSize", max_transaction_size);
  builder.add("intermediateCommitSize", intermediate_commit_size);
  builder.add("intermediateCommitCount", intermediate_commit_count);
  builder.add("allowImplicit", allow_implicit_collections_for_read);
  builder.add(StaticStrings::kWaitForSyncString, wait_for_sync);
  builder.add("fillBlockCache", fill_block_cache);
  // we are intentionally *not* writing allowImplicitCollectionForWrite here.
  // this is an internal option only used in replication
  builder.add("allowDirtyReads", allow_dirty_reads);

  builder.add("skipFastLockRound", skip_fast_lock_round);
}

#ifdef SDB_FAULT_INJECTION
/// patch intermediateCommitCount for testing
/*static*/ void Options::adjustIntermediateCommitCount(Options& options) {
  SDB_IF_FAILURE("TransactionState::intermediateCommitCount100") {
    options.intermediate_commit_count = 100;
  }
  SDB_IF_FAILURE("TransactionState::intermediateCommitCount1000") {
    options.intermediate_commit_count = 1000;
  }
  SDB_IF_FAILURE("TransactionState::intermediateCommitCount10000") {
    options.intermediate_commit_count = 10000;
  }
}
#endif
