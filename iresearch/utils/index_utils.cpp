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

#include "index_utils.hpp"

#include "iresearch/formats/segment_meta_writer.hpp"
#include "iresearch/index/index_meta.hpp"

namespace irs::index_utils {
namespace {

void PrepareFlush(IndexSegment& segment, bool increment_version) {
  auto& meta = segment.meta;
  SDB_ASSERT(meta.byte_size);  // Ensure segment size is estimated
  SDB_ASSERT(segment.meta.docs_mask_size <= segment.meta.byte_size);

  SDB_ASSERT(!meta.docs_mask || !meta.docs_mask->Empty());
  meta.live_docs_count = meta.docs_count;
  if (const auto removals = RemovalCount(meta); removals != 0) {
    SDB_ASSERT(removals < meta.docs_count);
    meta.live_docs_count -= removals;
    meta.version += uint64_t{increment_version};
  }
}

}  // namespace

void FlushIndexSegment(Directory& dir, IndexSegment& segment,
                       bool increment_version) {
  PrepareFlush(segment, increment_version);
  segment_meta::Write(dir, segment.filename, segment.meta);
}

}  // namespace irs::index_utils
