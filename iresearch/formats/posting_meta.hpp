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
    doc_start = pos_start = pay_start = 0;
    pos_offset = 0;
    doc_delta = 0;
  }

  uint32_t docs_count = 0;  // How many documents a particular term contains
  uint32_t freq = 0;  // How many times a particular term occur in documents
  uint64_t doc_start = 0;  // where this term's postings start in the .doc file
  uint64_t pos_start = 0;  // where this term's postings start in the .pos file
  uint64_t pay_start = 0;  // where this term's postings start in the .pay file
  // Slot of the term's first position inside the block at `pos_start`, so it
  // is bounded by the position block size.
  uint32_t pos_offset = 0;
  // A delta whose base `docs_count` decides, and the only field of this record
  // that means two things. A single-document term has no `.doc` data, so it
  // carries its document as a delta from `doc_limits::min()`; a term long
  // enough to carry skip data carries where that data starts as a delta from
  // `doc_start`, which bounds it by the term's own `.doc` footprint rather than
  // by the file -- `EndTerm` refuses a term that does not fit. For the lengths
  // in between it is neither written nor read.
  uint32_t doc_delta = 0;
};

// What a query over a term this segment does not have stands on.
inline constexpr PostingMeta kNoPosting;

}  // namespace irs
