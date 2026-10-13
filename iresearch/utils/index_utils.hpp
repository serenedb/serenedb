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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_writer.hpp"

namespace irs::index_utils {

// merge segment if:
//   {threshold} > segment_bytes / (all_segment_bytes / #segments)
struct CompactionBytes {
  float threshold = 0;
};

CompactionPolicy MakePolicy(const CompactionBytes& options);

// merge segment if:
//   {threshold} >= (segment_bytes + sum_of_merge_candidate_segment_bytes) /
//   all_segment_bytes
struct CompactionBytesAccum {
  float threshold = 0;
};

CompactionPolicy MakePolicy(const CompactionBytesAccum& options);

// merge first {threshold} segments
struct CompactionCount {
  size_t threshold = std::numeric_limits<size_t>::max();
};

CompactionPolicy MakePolicy(const CompactionCount& options);

// merge segment if:
//   {threshold} >= segment_docs{valid} / (all_segment_docs{valid} / #segments)
struct CompactionDocsLive {
  float threshold = 0;
};

CompactionPolicy MakePolicy(const CompactionDocsLive& options);

// merge segment if:
//   {threshold} > #segment_docs{valid} / (#segment_docs{valid} +
//   #segment_docs{removed})
struct CompactionDocsFill {
  float threshold = 0;
};

CompactionPolicy MakePolicy(const CompactionDocsFill& options);

// Writes segment_meta to the supplied directory
void FlushIndexSegment(Directory& dir, IndexSegment& segment,
                       bool increment_version = true);

void FlushIndexSegmentPatch(Directory& dir, IndexSegment& segment,
                            const DocumentMask& patch);

}  // namespace irs::index_utils
