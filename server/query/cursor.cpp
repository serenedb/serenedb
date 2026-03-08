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

#include "query/cursor.h"

#include "query/query.h"

namespace sdb::query {

void Cursor::RequestCancel() {
  if (_batch_executor) {
    _batch_executor->RequestCancel();
  } else if (_query.HasExternal()) {
    _query.GetExternalExecutor().RequestCancel().Detach();
  }
}

Cursor::Process Cursor::Next(velox::RowVectorPtr& batch) {
  if (_query.GetContext().command_type.Has(CommandType::External)) {
    return ExecuteStmt();
  }

  if (_batch_executor) {
    for (;;) {
      auto f = _batch_executor->Execute();
      if (!f.Valid()) {
        return Process::Done;
      }
      if (f.Ready()) {
        batch = std::move(f).Touch().Ok();
        if (batch) {
          return Process::More;
        }
        continue;
      }
      std::move(f).DetachInline(
        [user_task = _user_task](yaclib::Result<velox::RowVectorPtr>&&) {
          user_task();
        });
      return Process::Wait;
    }
  }

  return Process::Done;
}

Cursor::Process Cursor::ExecuteStmt() {
  {
    std::lock_guard lock{_stmt_result_mutex};
    if (_stmt_result.State() != yaclib::ResultState::Empty) {
      auto r = std::move(_stmt_result).Ok();
      if (!r.ok()) {
        SDB_THROW(std::move(r));
      }
      return Process::Done;
    }
  }
  auto f = _query.GetExternalExecutor().Execute();
  if (!f.Valid()) {
    return Process::Done;
  }
  if (f.Ready()) {
    auto r = std::move(f).Touch().Ok();
    if (!r.ok()) {
      SDB_THROW(std::move(r));
    }
    return Process::Done;
  }

  std::move(f).DetachInline(
    [&, user_task = _user_task](yaclib::Result<Result>&& r) {
      {
        std::lock_guard lock{_stmt_result_mutex};
        _stmt_result = std::move(r);
      }
      user_task();
    });
  return Process::Wait;
}

Cursor::Cursor(std::function<void()>&& user_task, Query& query)
  : _user_task{std::move(user_task)},
    _query{query},
    _batch_executor{query.TakeBatchExecutor()} {}

Cursor::~Cursor() {
  if (_query.HasExternal()) {
    std::ignore = _query.GetExternalExecutor().RequestCancel().Get();
  }
  // TODO: it doesn't look as correct place to do this
  if (!_query.GetContext().transaction->HasTransactionBegin()) {
    _query.GetContext().transaction->Destroy();
  }
}

}  // namespace sdb::query
