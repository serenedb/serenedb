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

#include <cstddef>
#include <functional>
#include <iresearch/formats/ann_build_env.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/merge_progress.hpp>
#include <iresearch/index/merge/pipeline_merge_executor.hpp>
#include <limits>

namespace tests {

using CompactionPolicy =
  std::function<void(irs::Compaction& candidates, const irs::IndexReader& index,
                     const irs::CompactingSegments& compacting_segments)>;

struct CompactionBytes {
  float threshold = 0;
};

CompactionPolicy MakePolicy(const CompactionBytes& options);

struct CompactionBytesAccum {
  float threshold = 0;
};

CompactionPolicy MakePolicy(const CompactionBytesAccum& options);

struct CompactionCount {
  size_t threshold = std::numeric_limits<size_t>::max();
};

CompactionPolicy MakePolicy(const CompactionCount& options);

struct CompactionDocsLive {
  float threshold = 0;
};

CompactionPolicy MakePolicy(const CompactionDocsLive& options);

struct CompactionDocsFill {
  float threshold = 0;
};

CompactionPolicy MakePolicy(const CompactionDocsFill& options);

irs::CompactionResult Compact(
  irs::IndexWriter& writer, const CompactionPolicy& policy,
  const irs::IndexFieldOptions* field_options = nullptr,
  const irs::MergeProgress& progress = {});

irs::CompactionResult Compact(irs::IndexWriter& writer,
                              const CompactionPolicy& policy,
                              const irs::IndexFieldOptions* field_options,
                              const irs::MergeProgress& progress,
                              const irs::AnnBuildEnv* env,
                              const irs::PipelineMergeExecutor& executor);

}  // namespace tests
