////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <axiom/runner/LocalRunner.h>

#include <yaclib/async/contract.hpp>
#include <yaclib/async/future.hpp>
#include <yaclib/async/make.hpp>

#include "basics/assert.h"
#include "basics/fwd.h"

namespace sdb::query {

// TODO: Axiom Runner is not designed for async execution this should be fixed.
class Runner final {
 public:
  Runner(const Runner&) = delete;
  Runner& operator=(const Runner&) = delete;

  Runner() = default;
  Runner(Runner&&) = default;
  Runner& operator=(Runner&&) = default;

  explicit Runner(
    axiom::runner::MultiFragmentPlanPtr plan,
    axiom::runner::FinishWrite finish_write,
    std::shared_ptr<velox::core::QueryCtx> query_ctx,
    std::shared_ptr<axiom::runner::SplitSourceFactory> split_source_factory,
    std::shared_ptr<velox::memory::MemoryPool> output_pool)
    : _runner{std::make_shared<facebook::axiom::runner::LocalRunner>(
        std::move(plan), std::move(finish_write), std::move(query_ctx),
        std::move(split_source_factory), std::move(output_pool))} {}

  explicit operator bool() const noexcept { return _runner != nullptr; }

  velox::RowVectorPtr Next(yaclib::Future<>& wait) {
    SDB_ASSERT(_runner);
    return _runner->next();
  }

  void RequestCancel() {
    if (_runner) {
      _runner->abort();
    }
  }

  std::string PrintPlanWithStats() {
    SDB_ASSERT(_runner);
    return _runner->printPlanWithStats();
  }

  ~Runner() {
    if (!_runner) {
      return;
    }
    _runner->abort();
    auto f = _runner->wait();
    _runner = nullptr;
    f.wait();
  }

 private:
  std::shared_ptr<axiom::runner::LocalRunner> _runner;
};

}  // namespace sdb::query
