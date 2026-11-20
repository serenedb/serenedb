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
#include <duckdb/parallel/task_scheduler.hpp>
#include <tuple>
#include <utility>
#include <vector>
#include <yaclib/async/run.hpp>
#include <yaclib/async/wait.hpp>

#include "iresearch/formats/ann_build_env.hpp"

namespace irs {

class AnnCrew {
 public:
  AnnCrew(const AnnBuildEnv* env, uint32_t want) noexcept
    : _env{env && env->executor && want > 1 ? env : nullptr},
      _want{_env ? want : 1U} {
    if (_env) {
      _held = _env->acquire(_want, 0);
    }
  }

  AnnCrew(const AnnCrew&) = delete;
  AnnCrew& operator=(const AnnCrew&) = delete;

  ~AnnCrew() {
    Wait();
    if (_env) {
      _env->release(_held);
    }
  }

  bool CanGrow() const noexcept { return _env; }
  uint32_t Want() const noexcept { return _want; }

  uint32_t Take() {
    if (_env && _held < _want) {
      _held += _env->acquire(_want - _held, _held);
    }
    return _held;
  }

  template<typename F>
  void Spawn(F& work) {
    for (; _spawned < _held; ++_spawned) {
      _runs.push_back(
        yaclib::Run(*_env->executor, [&work, w = _spawned] { work(w); }));
    }
  }

  template<typename F>
  void Grow(F& work) {
    Take();
    Spawn(work);
  }

  std::vector<yaclib::FutureOn<>>& Runs() noexcept { return _runs; }

  void Join() {
    Wait();
    const auto runs = std::exchange(_runs, {});
    _spawned = 1;
    for (const auto& run : runs) {
      std::ignore = run.Touch().Ok();
    }
  }

 private:
  void Wait() {
    if (!_runs.empty()) {
      duckdb::TaskScheduler::BlockingScope blocking;
      yaclib::Wait(_runs.begin(), _runs.end());
    }
  }

  const AnnBuildEnv* _env;
  uint32_t _want;
  uint32_t _held = 1;
  uint32_t _spawned = 1;
  std::vector<yaclib::FutureOn<>> _runs;
};

}  // namespace irs
