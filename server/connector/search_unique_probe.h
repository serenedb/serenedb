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

#include <cstdint>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/document_mask.hpp>
#include <span>
#include <vector>

#include "connector/search_key_unique.h"

namespace sdb::connector {

// What a transaction has removed that no reader shows yet: the documents it
// recorded against the reader it holds, and the rows it removed by key. A
// removal reaches a reader only with the publish that applies it, so a probe
// blind to these would answer for rows its own caller has already deleted.
struct PendingRemovals {
  const irs::DocRemoval* docs = nullptr;
  // Sorted, as `irs::RemoveKeys` requires.
  std::span<const int64_t> keys;
};

// "Does this key already exist outside the current transaction?" over the two
// tiers a certifier cannot see from one reader: what the published reader
// holds, and what committed transactions have left unpublished. Rows of the
// second tier arrive as whole segments where their transaction flushed them
// and as bare keys where it did not; `removed` spans both tiers, since a
// removal is invisible in the reader until the same publish applies it.
class UniqueProbe {
 public:
  // `reader` is the published tier, null for a pass that has already settled
  // its keys against it and is only catching up on what has been committed
  // since. Leaving it out is the point of such a pass: adding the published
  // segments is most of the cost of building a probe and all of the cost of
  // consulting one.
  // `unpublished_keys` is taken, not borrowed: the probe sorts it and keeps it
  // for its lifetime, so a caller that already built the vector hands it over
  // instead of having it copied.
  UniqueProbe(const irs::DirectoryReader* reader,
              std::span<const irs::SubReader* const> unpublished_segments,
              std::vector<const KeyTerms*> unpublished_keys,
              std::span<const irs::field_id> key_fields,
              const PendingRemovals& removed);

  bool Exists(std::span<const KeyTerm> key);

  // Whether any segment holds `term` for key column `field`, ignoring deletes.
  // A false rules the term out without reading a posting list; a true still
  // needs `Exists` to settle it.
  bool MayHold(size_t field, const KeyTerm& term);

 private:
  // One key column's term as the unpublished tier holds it: the owning
  // `KeyTerm` belongs to a key, and the index built per column only borrows.
  struct ColumnTerm {
    irs::DictRef dict;
    irs::bytes_view term;

    auto operator<=>(const ColumnTerm&) const = default;
  };

  struct Segment {
    const irs::SubReader* sub;
    irs::DocumentMask::Iterator mask;
    // Per key column, every dictionary the field was written under: which one
    // answers is the key's own, and for a boolean two rows of one column
    // answer from different ones.
    std::vector<std::span<const irs::TermReader* const>> fields;
    irs::DocumentMaskBuilder deleted;
  };

  void AddSegment(const irs::SubReader& sub,
                  std::span<const irs::field_id> key_fields);

  void ApplyRemovals(const PendingRemovals& removed);

  // Unpublished keys indexed by key column, built on the first MayHold for
  // that column: the column the walk leads with is chosen after the probe is
  // constructed, and for a composite key the others are never asked.
  std::span<const ColumnTerm> ColumnTerms(size_t field);

  std::vector<Segment> _segments;
  std::vector<const KeyTerms*> _keys;
  std::vector<std::vector<ColumnTerm>> _column_terms;
  std::vector<bool> _column_indexed;
  std::vector<const irs::TermReader*> _readers;
  std::vector<irs::doc_id_t> _hits;
  std::vector<irs::doc_id_t> _next;
};

}  // namespace sdb::connector
