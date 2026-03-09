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

#include "query/velox_executor.h"

#include <absl/cleanup/cleanup.h>

#include <yaclib/async/make.hpp>

#include "basics/assert.h"
#include "query/query.h"

namespace sdb::query {

void VeloxExecutor::Init(Query& query) {
  _query = &query;
  SDB_ASSERT(!query.GetRunner());
  // if the query is not compiled we expect that preceding executor will do it
  if (query.IsCompiled()) {
    query.MakeRunner();
  }
}

yaclib::Future<> VeloxExecutor::Execute(velox::RowVectorPtr& batch) {
  auto& runner = _query->GetRunner();
  SDB_ASSERT(runner);
  yaclib::Future<> wait;
  batch = runner.Next(wait);
  if (wait.Valid()) {
    SDB_ASSERT(!batch);
    return wait;
  }
  if (_ignore_output) {
    batch = nullptr;
  }
  return {};
}

yaclib::Future<> VeloxExecutor::RequestCancel() {
  _query->GetRunner().RequestCancel();
  return {};
}

yaclib::Future<> RollbackVeloxExecutor::Execute(velox::RowVectorPtr& batch) {
  absl::Cleanup rollback = [&]() noexcept { _on_error(); };
  auto f = VeloxExecutor::Execute(batch);
  std::move(rollback).Cancel();
  return f;
}

}  // namespace sdb::query
