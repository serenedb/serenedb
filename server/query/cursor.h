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
#include <absl/functional/function_ref.h>
#include <velox/exec/Task.h>
#include <velox/vector/ComplexVector.h>

#include <span>

#include "query/context.h"
#include "query/executor.h"
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

  yaclib::Future<> RequestCancel();

  ~Cursor();

 private:
  friend class Query;
  Cursor(UserTask&& user_task, Query& query);

  Process NextImpl(velox::RowVectorPtr& batch);
  Process HandleBatch(velox::RowVectorPtr& batch);

  UserTask _user_task;
  Query& _query;
  std::span<const std::unique_ptr<Executor>> _executors;
  absl::AnyInvocable<void()>& _on_error;
  size_t _current = 0;
};

}  // namespace sdb::query
