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

#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/index/index_reader_options.hpp"

namespace irs {

class IndexFieldOptions;
struct AnnBuildEnv;

struct SegmentWriterOptions {
  ScorerPtr scorer = nullptr;
  // Non-owning. For a segment writer just the fallback (the owning override
  // comes via SetFieldOptions); for a merge writer the whole config.
  const IndexFieldOptions* field_options = nullptr;
  // Non-owning. Null builds the segment's ANN graph on the flushing thread.
  const AnnBuildEnv* ann_env = nullptr;
  HnswBases* hnsw_bases = nullptr;
};

}  // namespace irs
