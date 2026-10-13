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

namespace yaclib {

class IExecutor;

}  // namespace yaclib
namespace sdb::search {

class PoolMergeExecutor final : public irs::MergeExecutor {
 public:
  using AcquireFn = bool (*)() noexcept;
  using ReleaseFn = void (*)() noexcept;

  PoolMergeExecutor(yaclib::IExecutor& pool, size_t helpers, AcquireFn acquire,
                    ReleaseFn release) noexcept
    : _pool{pool}, _helpers{helpers}, _acquire{acquire}, _release{release} {}

  bool Run(irs::MergePlan& plan) final;
  size_t Threads() const noexcept final { return _helpers + 1; }

 private:
  yaclib::IExecutor& _pool;
  size_t _helpers;
  AcquireFn _acquire;
  ReleaseFn _release;
};

}  // namespace sdb::search
