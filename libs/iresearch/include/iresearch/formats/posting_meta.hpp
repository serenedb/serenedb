////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>

#include "iresearch/types.hpp"

namespace irs {

struct PostingMeta {
  void clear() noexcept {
    docs_count = freq = 0;
    pos_start = pay_start = 0;
    pos_offset = 0;
    first_entry = 0;
  }

  uint32_t docs_count = 0;  // How many documents a particular term contains
  uint32_t freq = 0;  // How many times a particular term occur in documents
  uint64_t pos_start = 0;  // where this term's postings start in the .pos file
  uint64_t pay_start = 0;  // where this term's postings start in the .pay file
  // Slot of the term's first position inside the block at `pos_start`, so it
  // is bounded by the position block size.
  uint32_t pos_offset = 0;

  // The record carries exactly one of the three below, and `docs_count` says
  // which:
  //
  //   1 doc         `doc` -- the document itself. Such a term has no `.doc`
  //                 data, and its score bound follows from `freq` and that
  //                 document.
  //   <= 128 docs   `doc_start` -- one doc block, whose score bound leads the
  //                 term's `.doc` data.
  //   more          `first_entry` -- ordinal of the term's first skip entry.
  //                 Entry `j` describes doc block `j`, so entry 0 carries
  //                 `doc_start` and every block's bound is a column.
  //
  // Each is monotone only within itself, so each delta codes against the last
  // term of its own kind -- the running bases live in the decoder, not here.
  //
  // `pos_start`, `pos_offset` and `pay_start` are written for every term, so
  // the position and payload columns can be relative to them.
  union {
    doc_id_t doc;
    uint64_t doc_start;
    uint64_t first_entry = 0;
  };
};

// Running delta bases for `PostingsReader::decode`. The term record's one
// slot takes three meanings and each is monotone only within itself, so each
// needs its own base. Kept out of `PostingMeta`, of which there is one per
// term, and reset alongside it at a block boundary.
struct PostingDecodeState {
  uint64_t doc_start = 0;
  uint64_t first_entry = 0;
};

// What a query over a term this segment does not have stands on.
inline constexpr PostingMeta kNoPosting;

}  // namespace irs
