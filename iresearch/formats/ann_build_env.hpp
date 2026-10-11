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

#include <absl/functional/function_ref.h>

#include <cstdint>

namespace irs {

class AnnBuildEnv {
 public:
  using Step = absl::FunctionRef<bool(uint32_t)>;

  virtual ~AnnBuildEnv() = default;

  virtual uint32_t Acquire(uint32_t want) const noexcept = 0;

  virtual void Release(uint32_t workers) const noexcept = 0;

  virtual void Run(uint32_t workers, Step step) const = 0;
};

class AnnWorkers {
 public:
  AnnWorkers(const AnnBuildEnv* env, uint32_t want) noexcept
    : _env{want > 1 ? env : nullptr},
      _count{_env != nullptr ? _env->Acquire(want) : 1} {}

  ~AnnWorkers() {
    if (_env != nullptr) {
      _env->Release(_count);
    }
  }

  AnnWorkers(const AnnWorkers&) = delete;
  AnnWorkers& operator=(const AnnWorkers&) = delete;

  uint32_t Count() const noexcept { return _count; }

  void Run(AnnBuildEnv::Step step) const {
    if (_count > 1) {
      _env->Run(_count, step);
      return;
    }
    while (step(0)) {
    }
  }

 private:
  const AnnBuildEnv* _env;
  uint32_t _count;
};

}  // namespace irs
