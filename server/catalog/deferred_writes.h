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

#include <absl/functional/function_ref.h>

#include <cstddef>
#include <cstdint>
#include <duckdb/common/shared_ptr.hpp>
#include <memory>
#include <span>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/store/wal_entry.h"

namespace duckdb {

class ClientContext;
class DataTable;

}  // namespace duckdb
namespace sdb::catalog {

// The catalog half of one transaction, parked on its ClientContext.
//
// Four things live here, all with the same lifetime -- from the first catalog
// mutation of the transaction to its commit or rollback:
//
//   - the log frames it has not appended yet, in one queue. They go out from
//     the commit itself, together with the write of what they describe: the
//     catalog is the decision point and commits first, and the position they
//     land at rides the data commit that follows. They are also the description
//     of the batch a commit replays when its base moved, so nothing else has to
//     record what the mutations did.
//   - the rows a reshaping statement produced, waiting for the entry write.
class DeferredCatalogWrites;

// Nothing is recorded without a transaction to charge it to: boot, background
// drop tasks and connection teardown append inline and hold no claim, so they
// need no state and must not create it.
DeferredCatalogWrites* TryGetDeferredCatalogWrites(
  duckdb::ClientContext& context);

// Parks a frame for the transaction's commit to append, and hands back the
// records as the queue now holds them: the mutation performs them from there,
// so the entry write this mutation triggers already finds its own frame
// queued.
std::span<const wal::Entry> QueueDeferredFrame(duckdb::ClientContext& context,
                                               std::vector<wal::Entry> entries);

// Parks the async artifact cleanup for a removal this transaction performed,
// released together with the destructive frames. A drop task closes the wal's
// drop bracket when it finishes, so starting it before the DropPrepare that
// opens the bracket is durable would leave the tombstone open forever. Returns
// false when there is no transaction to park it on, in which case the caller
// schedules it itself -- the append was inline and already ordered.
bool QueueDropTask(duckdb::ClientContext& context,
                   const std::shared_ptr<DropTask>& task);

// Counts one applied mutation and pins the transaction's read view of the
// cluster-global sets. False when there is no transaction to charge the
// mutation to -- boot, background drop tasks, teardown -- whose records were
// appended inline.
bool RecordCatalogDelta(duckdb::ClientContext& context);

// How many catalog mutations this transaction has recorded. Introspection, and
// the signal a cached plan is checked against.
size_t CatalogWriteCount(duckdb::ClientContext& context);

}  // namespace sdb::catalog
