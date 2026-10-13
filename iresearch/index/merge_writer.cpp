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

#include "merge_writer.hpp"

#include <yaclib/coro/future.hpp>

#include "iresearch/index/merge/merge_plan.hpp"
#include "iresearch/utils/misc.hpp"

namespace irs {

auto MergeWriter::Flush(SegmentMeta& segment,
                        const FlushProgress& progress /*= {}*/,
                        const AnnBuildEnv* env /*= nullptr*/,
                        MergeExecutor* executor /*= nullptr*/)
  -> yaclib::Future<bool> {
  bool result = false;
  Finally segment_invalidator = [&result, &segment]() noexcept {
    if (!result) [[unlikely]] {
      segment.files.clear();
      static_cast<SegmentInfo&>(segment) = SegmentInfo{};
    }
  };
  MergePlan plan{_dir, _readers,       segment,  _scorer,
                 _db,  _field_options, progress, env};
  result = plan.Prepare(executor != nullptr ? executor->Threads() : 1) &&
           (executor != nullptr ? executor->Run(plan) : RunSerially(plan));
  _profile = plan.Profile();
  co_return result;
}

}  // namespace irs
