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

#include <absl/container/btree_set.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <memory>
#include <optional>
#include <utility>
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
//
// The segments are readers rather than names because the in-lock pass reads
// them while holding DuckDB's commit lock, where a file open does not belong.
// The committer opens them before it takes that lock -- which it has to do
// anyway, to put its own keys to its own pre-lock pass -- so the reader every
// later certifier needs costs the commit nothing it was not already paying.
struct PendingGeneration {
  // What a refresh prunes by: the writer tick this transaction committed at.
  // Non-decreasing in install order but not strictly increasing, since a
  // transaction that reserves no query takes the tick already standing.
  uint64_t tick = 0;
  // What a certifier reads by: the delta's own install order, assigned here
  // because two generations can share a tick and a certifier that skipped the
  // second of them would be certifying against a hole.
  uint64_t sequence = 0;
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
  // A certifier's claim on everything installed above `Mark()`. While one is
  // held a prune drops nothing above that mark, so the tail the certifier has
  // not read yet is still here when it comes back for it.
  //
  // Without it a refresh between a certifier's two passes could publish that
  // tail and prune it, leaving those keys in the new reader alone -- which the
  // certifier is not holding and will not take again, since re-reading the
  // published state is the work the two-pass split exists to avoid. They would
  // then be in none of its sources, and it would admit a duplicate of them.
  class [[nodiscard]] PruneFloor {
   public:
    PruneFloor() noexcept = default;
    PruneFloor(PruneFloor&& other) noexcept
      : _delta{std::exchange(other._delta, nullptr)}, _mark{other._mark} {}
    PruneFloor& operator=(PruneFloor&& other) noexcept {
      PruneFloor{std::move(other)}.Swap(*this);
      return *this;
    }
    ~PruneFloor() { Release(); }

    bool Armed() const noexcept { return _delta != nullptr; }
    uint64_t Mark() const noexcept { return _mark; }
    void Release() noexcept;

   private:
    friend class PendingDelta;

    PruneFloor(PendingDelta& delta, uint64_t mark) noexcept
      : _delta{&delta}, _mark{mark} {}

    void Swap(PruneFloor& other) noexcept {
      std::swap(_delta, other._delta);
      std::swap(_mark, other._mark);
    }

    PendingDelta* _delta = nullptr;
    uint64_t _mark = 0;
  };

  struct Reading {
    std::vector<PendingGenerationPtr> generations;
    // How far into the install order this reading settles the question: the
    // last sequence installed when it was taken, which is not the last among
    // `generations` once a prune has dropped the ones below it.
    uint64_t mark = 0;
  };

  struct Claimed {
    std::vector<PendingGenerationPtr> generations;
    PruneFloor floor;
    // Set when the incremental view is unsound and `generations` is the whole
    // delta rather than the tail: the caller must put the published state to
    // its probe as well. See Claim.
    bool from_scratch = false;
  };

  // Installed under the committing database's commit lock and after the
  // commit's point of no return. Earlier would publish the keys of a
  // transaction that can still abort; later opens a window in which the rows
  // are in neither the reader nor the delta, so a concurrent certifier sees
  // neither and admits a duplicate.
  void Install(std::shared_ptr<PendingGeneration> generation);

  // The unpublished generations, with `read_reader` run inside the lock the
  // pruner also takes -- so the reader a certifier holds and the generations
  // beside it can never straddle a prune, which is what would hide keys that
  // by then live only in the reader the certifier did not take.
  //
  // Arms nothing: this is the statement-end probe, and the transaction it runs
  // in may sit idle for as long as its client likes before it commits. A floor
  // held across that would keep the delta from ever being pruned.
  Reading Read(absl::FunctionRef<void()> read_reader) const;

  // The tail above `certified` -- the mark a previous Read settled, absent
  // when there was none -- with a floor armed so the next pass still finds
  // whatever lands from here on. Takes the delta's lock exclusively, so a
  // caller that holds no other lock must claim before it takes one.
  //
  // `from_scratch` comes back set when a prune has dropped a generation above
  // `certified` in the meantime. The tail is then incomplete and cannot be
  // completed, since what is missing has moved into a reader the caller does
  // not hold, so the claim degrades to the whole delta and the caller re-reads
  // the published state with it. That is the price of not holding a floor
  // across the idle window, and it is paid only by a transaction a refresh
  // overtook.
  Claimed Claim(std::optional<uint64_t> certified,
                absl::FunctionRef<void()> read_reader);

  // The tail above `floor`, with the floor moved up over it. Needs no
  // completeness check: the floor is what guarantees none of it was pruned.
  std::vector<PendingGenerationPtr> Extend(PruneFloor& floor);

  // Drops every generation at or below `tick`, less whatever an armed floor
  // holds back. The bound is read before the refresh begins and applied once
  // the new reader is installed, so nothing is dropped whose publish has not
  // landed.
  //
  // Both orders prune a prefix -- ticks are non-decreasing in install order --
  // so holding back by sequence and dropping by tick agree on where to stop.
  void Prune(uint64_t tick);

  // Everything this table has pending, for a publication that replaces the
  // index wholesale rather than adding to it.
  void Clear();

  bool Empty() const;

 private:
  void ReleaseFloor(uint64_t tick) noexcept;

  mutable absl::Mutex _mutex;
  std::vector<PendingGenerationPtr> _generations ABSL_GUARDED_BY(_mutex);
  // The last sequence installed and the last dropped. A certifier compares the
  // mark it read against the second to learn whether its tail is still whole,
  // and takes the first as the mark for its next pass.
  uint64_t _installed ABSL_GUARDED_BY(_mutex) = 0;
  uint64_t _pruned_through ABSL_GUARDED_BY(_mutex) = 0;
  absl::btree_multiset<uint64_t> _floors ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::search
