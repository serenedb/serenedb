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

#include <yaclib/async/make.hpp>

#include "basics/assert.h"
#include "query/query.h"

namespace sdb::query {

void VeloxExecutor::Init(Query& query) {
  _query = &query;
  _runner = query.MakeRunner();
}

yaclib::Future<> VeloxExecutor::Execute(velox::RowVectorPtr& batch) {
  SDB_ASSERT(_runner);
  yaclib::Future<> wait;
  batch = _runner.Next(wait);
  if (wait.Valid()) {
    SDB_ASSERT(!batch);
    return wait;
  }
  return {};
}

yaclib::Future<> VeloxExecutor::RequestCancel() {
  _runner.RequestCancel();
  return {};
}

}  // namespace sdb::query
