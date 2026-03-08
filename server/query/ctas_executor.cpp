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

CTASExecutor::CTASExecutor(std::unique_ptr<pg::CTASCommand> ctas_command)
  : _ctas_command{std::move(ctas_command)} {}

yaclib::Future<velox::RowVectorPtr> CTASExecutor::Execute() {
  SDB_ASSERT(_ctas_command);
  SDB_ASSERT(_query);

  for (;;) {
    switch (_ctas_command->GetStage()) {
      using enum pg::CTASCommand::Stage;
      case None: {
        auto f = _ctas_command->CreateTable();
        if (!f.Ready()) {
          _ctas_command->SetStage(VeloxRunning);
          return std::move(f).ThenInline(
            [this](Result&& r) -> velox::RowVectorPtr {
              if (!r.ok()) {
                SDB_THROW(std::move(r));
              }
              _query->CompileQuery();
              _runner = _query->MakeRunner();
              return nullptr;
            });
        }
        auto r = std::move(f).Touch().Ok();
        if (!r.ok()) {
          SDB_THROW(std::move(r));
        }
        _ctas_command->SetStage(VeloxRunning);
        _query->CompileQuery();
        _runner = _query->MakeRunner();
        continue;
      }
      case CreateTableWaiting: {
        SDB_UNREACHABLE();
      }
      case VeloxRunning: {
        try {
          SDB_ASSERT(_runner);
          yaclib::Future<> wait;
          auto batch = _runner.Next(wait);
          if (wait.Valid()) {
            SDB_ASSERT(!batch);
            return std::move(wait).ThenInline(
              [](auto&&) -> velox::RowVectorPtr { return nullptr; });
          }
          if (batch) {
            return yaclib::MakeFuture<velox::RowVectorPtr>(std::move(batch));
          }
        } catch (...) {
          _ctas_command->Rollback();
          throw;
        }
        SDB_IF_FAILURE("crash_ctas_before_remove_tombstone") {
          SDB_IMMEDIATE_ABORT();
        }
        auto r = _ctas_command->RemoveTombstone();
        if (!r.ok()) {
          _ctas_command->Rollback();
          SDB_THROW(std::move(r));
        }
        return {};
      }
    }
    SDB_UNREACHABLE();
  }
}

void CTASExecutor::RequestCancel() { _runner.RequestCancel(); }

}  // namespace sdb::query
