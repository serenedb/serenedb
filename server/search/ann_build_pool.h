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

#include <cstdint>
#include <iresearch/formats/ann_build_env.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/util/intrusive_ptr.hpp>

namespace sdb::search {

class AnnBuildPool final : public irs::AnnBuildEnv {
 public:
  using AcquireFn = uint32_t (*)(uint32_t) noexcept;
  using ReleaseFn = void (*)(uint32_t) noexcept;

  AnnBuildPool(yaclib::IExecutor& pool, AcquireFn acquire,
               ReleaseFn release) noexcept;

  uint32_t Acquire(uint32_t want) const noexcept final {
    return _acquire(want);
  }

  void Release(uint32_t workers) const noexcept final { _release(workers); }

  void Run(uint32_t workers, Step step) const final;

 private:
  yaclib::IntrusivePtr<yaclib::IExecutor> _pool;
  AcquireFn _acquire;
  ReleaseFn _release;
};

}  // namespace sdb::search
