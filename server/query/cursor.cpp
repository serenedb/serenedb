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
  for (;;) {
    if (_current >= _executors.size()) {
      return Process::Done;
    }
    auto& executor = _executors[_current];
    auto f = executor->Execute(batch);
    if (f.Valid()) {
      std::move(f).DetachInline(
        [user_task = _user_task](auto&&) { user_task(); });
      return Process::Wait;
    }

    if (batch) {
      return Process::More;
    }

    ++_current;
  }
}

Cursor::Cursor(std::function<void()>&& user_task, Query& query)
  : _user_task{std::move(user_task)},
    _query{query},
    _executors{query.TakeExecutors()} {}

Cursor::~Cursor() {
  if (_current < _executors.size()) {
    std::ignore = _executors[_current]->RequestCancel().Get();
  }
  // TODO: it doesn't look as correct place to do this
  if (!_query.GetContext().transaction->HasTransactionBegin()) {
    _query.GetContext().transaction->Destroy();
  }
}

}  // namespace sdb::query
