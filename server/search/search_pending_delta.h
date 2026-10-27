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
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <memory>
#include <vector>

#include "search/search_table_changes.h"

namespace sdb::search {

// What one committed transaction leaves behind until a refresh publishes it:
// rows that are durable, in no reader, and therefore invisible to a probe that
// consults only the reader.
//
// Rows arrive in whichever of the two forms their write path produced. A
// transaction whose buffer overran flushed its rows into segments, so those
// travel as readers; one that stayed under the threshold has its rows only in
// an unflushed iresearch transaction, so its keys travel as terms, bounded by
// the threshold that kept them there. `removed` is the rowids the transaction
// deleted: published rows that are no longer live, which a probe must discount
// for exactly as long as the reader still shows them. It carries the ranges the
// change entry recorded rather than their rowids, so a row-range DELETE is not
// expanded here only to be walked once.
struct PendingGeneration {
  uint64_t tick = 0;
  std::vector<irs::SegmentReader> segments;
  std::vector<std::vector<irs::bstring>> keys;
  std::vector<RowRange> removed;

  bool Empty() const noexcept {
    return segments.empty() && keys.empty() && removed.empty();
  }
};

using PendingGenerationPtr = std::shared_ptr<const PendingGeneration>;

// Per search table. Generational: one entry per commit, dropped whole, nothing
// ever removed by key.
class PendingDelta {
 public:
  // Installed under the committing database's commit lock and after the
  // commit's point of no return. Earlier would publish the keys of a
  // transaction that can still abort; later opens a window in which the rows
  // are in neither the reader nor the delta, so a concurrent certifier sees
  // neither and admits a duplicate.
  void Install(PendingGenerationPtr generation);

  // The unpublished generations, with `read_reader` run inside the lock the
  // pruner also takes -- so the reader a certifier holds and the generations
  // beside it can never straddle a prune, which is what would hide keys that
  // by then live only in the reader the certifier did not take.
  std::vector<PendingGenerationPtr> Snapshot(
    absl::FunctionRef<void()> read_reader) const;

  // Drops every generation at or below `tick`. The bound is read before the
  // refresh begins and applied once the new reader is installed, so nothing is
  // dropped whose publish has not landed.
  void Prune(uint64_t tick);

  // Everything this table has pending, for a publication that replaces the
  // index wholesale rather than adding to it.
  void Clear();

  bool Empty() const;

 private:
  mutable absl::Mutex _mutex;
  std::vector<PendingGenerationPtr> _generations ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::search
