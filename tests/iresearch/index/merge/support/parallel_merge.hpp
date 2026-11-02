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

#include <cstddef>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/pipeline_merge_executor.hpp>

namespace tests::merge {

irs::PipelineMergeExecutor MakeExecutor(size_t threads = 1);

bool RunPlan(irs::MergePlan& plan, size_t threads = 1);

class ShuffledClaims {
 public:
  explicit ShuffledClaims(bool shuffled = true);
  ~ShuffledClaims();

  ShuffledClaims(const ShuffledClaims&) = delete;
  ShuffledClaims& operator=(const ShuffledClaims&) = delete;

 private:
  bool _armed;
};

}  // namespace tests::merge
