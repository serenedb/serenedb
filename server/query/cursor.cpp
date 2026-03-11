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

yaclib::Future<> Cursor::RequestCancel() {
  if (_current < _executors.size()) {
    return _executors[_current]->RequestCancel();
  }
  return {};
}

Cursor::Process Cursor::Next(velox::RowVectorPtr& batch) {
  for (; _current < _executors.size(); ++_current) {
    auto& executor = _executors[_current];
    auto f = executor->Execute(batch);
    if (batch) {
      SDB_ASSERT(!f.Valid());
      return Process::More;
    }

    if (f.Valid()) {
      if (f.Ready()) {
        std::ignore = std::move(f).Touch().Ok();
        continue;
      }

      std::move(f).DetachInline([user_task = _user_task](yaclib::Result<> r) {
        user_task(std::move(r));
      });
      return Process::Wait;
    }
  }

  return Process::Done;
}

Cursor::Cursor(UserTask&& user_task, Query& query)
  : _user_task{std::move(user_task)},
    _query{query},
    _executors{query.StealExecutors()} {}

Cursor::~Cursor() {
  if (_current < _executors.size()) {
    auto f = _executors[_current]->RequestCancel();
    if (f.Valid()) {
      std::ignore = std::move(f).Get();
    }
  }
  // TODO: it doesn't look as correct place to do this
  if (!_query.GetContext().transaction->HasTransactionBegin()) {
    _query.GetContext().transaction->Destroy();
  }
}

}  // namespace sdb::query
