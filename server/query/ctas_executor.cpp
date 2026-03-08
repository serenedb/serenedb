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

#include "query/ctas_executor.h"

#include <yaclib/async/make.hpp>

#include "basics/assert.h"
#include "basics/system-compiler.h"
#include "query/query.h"

namespace sdb::query {

// CreateTableExecutor

CreateTableExecutor::CreateTableExecutor(
  std::unique_ptr<pg::CTASCommand> ctas_command)
  : _ctas_command{std::move(ctas_command)} {}

yaclib::Future<> CreateTableExecutor::Execute(velox::RowVectorPtr& batch) {
  SDB_ASSERT(_query);
  SDB_ASSERT(_ctas_command);

  if (_fired) {
    return {};
  }
  _fired = true;

  auto f = _ctas_command->CreateTable();
  if (!f.Ready()) {
    return std::move(f).ThenInline([this](Result&& r) {
      if (!r.ok()) {
        SDB_THROW(std::move(r));
      }
      _query->CompileQuery();
    });
  }
  auto r = std::move(f).Touch().Ok();
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  _query->CompileQuery();
  return {};
}

// CTASVeloxExecutor

CTASVeloxExecutor::CTASVeloxExecutor(pg::CTASCommand& ctas_command)
  : _ctas_command{ctas_command} {}

yaclib::Future<> CTASVeloxExecutor::Execute(velox::RowVectorPtr& batch) {
  SDB_ASSERT(_query);
  if (!_runner) {
    _runner = _query->MakeRunner();
  }
  try {
    yaclib::Future<> wait;
    batch = _runner.Next(wait);
    if (wait.Valid()) {
      SDB_ASSERT(!batch);
      return wait;
    }
    if (batch) {
      return yaclib::MakeFuture();
    }
  } catch (...) {
    _ctas_command.Rollback();
    throw;
  }
  return {};
}

yaclib::Future<> CTASVeloxExecutor::RequestCancel() {
  _runner.RequestCancel();
  return {};
}

// RemoveTombstoneExecutor

RemoveTombstoneExecutor::RemoveTombstoneExecutor(pg::CTASCommand& ctas_command)
  : _ctas_command{ctas_command} {}

yaclib::Future<> RemoveTombstoneExecutor::Execute(velox::RowVectorPtr& batch) {
  if (_fired) {
    return {};
  }
  _fired = true;

  SDB_IF_FAILURE("crash_ctas_before_remove_tombstone") {
    SDB_IMMEDIATE_ABORT();
  }
  auto r = _ctas_command.RemoveTombstone();
  if (!r.ok()) {
    _ctas_command.Rollback();
    SDB_THROW(std::move(r));
  }
  return {};
}

}  // namespace sdb::query
