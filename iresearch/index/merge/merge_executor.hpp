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

#include "iresearch/formats/ann_build_env.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_plan.hpp"
#include "iresearch/index/merge/merge_profile.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/index/segment_writer_options.hpp"
#include "iresearch/store/directory.hpp"

namespace irs {

[[nodiscard]] bool ExecuteMerge(MergePlan& plan);

[[nodiscard]] bool MergeSegments(Directory& dir, MergeDocMap& doc_map,
                                 SegmentMeta& segment,
                                 const SegmentWriterOptions& options,
                                 duckdb::shared_ptr<duckdb::DedicatedPool> pool,
                                 const MergeProgress& progress = {},
                                 const AnnBuildEnv* env = nullptr,
                                 MergeProfile* profile = nullptr);

}  // namespace irs
