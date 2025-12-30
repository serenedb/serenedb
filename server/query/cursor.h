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

#include <absl/functional/any_invocable.h>
#include <basics/exceptions.h>
#include <velox/exec/Task.h>
#include <velox/vector/ComplexVector.h>

#include <yaclib/util/result.hpp>

#include "query/context.h"
#include "query/runner.h"

namespace sdb::query {

class Query;

class Cursor {
 public:
  enum class Process {
    Wait = 0,
    More,
    Done,
  };
  Process Next(velox::RowVectorPtr& batch);

  void RequestCancel();

  ~Cursor();

 private:
  Process ExecuteVelox(velox::RowVectorPtr& batch);
  Process ExecuteStmt();
  Process ExecuteShow(velox::RowVectorPtr& batch);
  Process ExecuteShowAll(velox::RowVectorPtr& batch);

  friend class Query;
  Cursor(std::function<void()>&& user_task, const Query& query);

  void BuildBatch(velox::RowVectorPtr& batch,
                  std::span<const std::vector<std::string>> data);

  std::shared_ptr<velox::memory::MemoryPool> _data_memory_pool;
  std::function<void()> _user_task;
  Runner _runner;
  const Query& _query;

  yaclib::Result<Result> _stmt_result;
  absl::Mutex _stmt_result_mutex;
};

}  // namespace sdb::query
