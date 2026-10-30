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

#include <iresearch/index/index_reader.hpp>
#include <span>
#include <vector>

#include "connector/search_key_unique.h"
#include "search/search_pending_delta.h"
#include "search/search_table_changes.h"

namespace sdb::search {

// What a certifier must consult besides the published reader and its own
// writes: rows that are durable, in no reader, and therefore invisible to a
// probe that reads only the reader.
//
// This is the shape the probe takes, with nothing in it about where the rows
// were found. Two sides can fill it -- the in-memory delta every commit
// installs, and a read of the WAL tail that installs nothing -- and the point
// of the shape is that everything above the seam is written once.
//
// The pointers borrow. Whatever produced them has to outlive the probe, which
// is why each producer hands back its own owning type with one of these
// inside rather than this alone.
struct UnpublishedTier {
  std::vector<const irs::SubReader*> segments;
  std::vector<const connector::KeyTerms*> keys;
  std::vector<RowRange> removed;

  bool Empty() const noexcept {
    return segments.empty() && keys.empty() && removed.empty();
  }
};

// The delta's side of the seam: the generations as the probe wants them.
inline UnpublishedTier Flatten(
  std::span<const PendingGenerationPtr> generations) {
  UnpublishedTier tier;
  for (const auto& generation : generations) {
    for (const auto& segment : generation->segments) {
      tier.segments.push_back(&segment);
    }
    for (const auto& key : generation->keys) {
      tier.keys.push_back(&key);
    }
    tier.removed.insert(tier.removed.end(), generation->removed.begin(),
                        generation->removed.end());
  }
  return tier;
}

}  // namespace sdb::search
