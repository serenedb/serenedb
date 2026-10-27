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

#include <iresearch/index/directory_reader.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <span>

namespace sdb::connector {

// "Does this key already exist outside the current transaction?" over the two
// tiers a certifier cannot see from one reader: what the published reader
// holds, and what committed transactions have left unpublished. Rows of the
// second tier arrive as whole segments where their transaction flushed them
// and as bare keys where it did not; `deleted_rowids` spans both tiers, since
// a removal is invisible in the reader until the same publish applies it.
class UniqueProbe {
 public:
  UniqueProbe(
    const irs::DirectoryReader& reader,
    std::span<const irs::SubReader* const> unpublished_segments,
    std::span<const std::vector<irs::bstring>* const> unpublished_keys,
    std::span<const irs::field_id> key_fields,
    std::span<const int64_t> deleted_rowids);

  bool Exists(std::span<const irs::bstring> key_terms);

  // Whether any segment holds `term` for key column `field`, ignoring deletes.
  // A false rules the term out without reading a posting list; a true still
  // needs `Exists` to settle it.
  bool MayHold(size_t field, irs::bytes_view term);

 private:
  struct Segment {
    const irs::SubReader* sub;
    irs::DocumentMask::Iterator mask;
    std::vector<const irs::TermReader*> fields;
    irs::DocumentMaskBuilder deleted;
  };

  void AddSegment(const irs::SubReader& sub,
                  std::span<const irs::field_id> key_fields,
                  std::span<const int64_t> deleted_rowids);

  // Unpublished keys indexed by key column, built on the first MayHold for
  // that column: the column the walk leads with is chosen after the probe is
  // constructed, and for a composite key the others are never asked.
  std::span<const irs::bytes_view> ColumnTerms(size_t field);

  std::vector<Segment> _segments;
  std::vector<const std::vector<irs::bstring>*> _keys;
  std::vector<std::vector<irs::bytes_view>> _column_terms;
  std::vector<bool> _column_indexed;
  std::vector<irs::doc_id_t> _hits;
  std::vector<irs::doc_id_t> _next;
};

}  // namespace sdb::connector
