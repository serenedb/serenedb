
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

#include <absl/synchronization/mutex.h>
#include <axiom/logical_plan/LogicalPlanNode.h>
#include <yaclib/util/result.hpp>

#include "basics/fwd.h"
#include "basics/result.h"
#include "catalog/identifiers/object_id.h"
#include "utils/exec_context.h"
#include "yaclib/async/future.hpp"

struct CreateTableAsStmt;

namespace sdb::query {
class Transaction;
}

namespace sdb::pg {

// create table as stmt command
class CTASCommand {
 public:
  enum class Stage {
    None,
    CreateTableWaiting,
    VeloxRunning,
    PersistWaiting,
  };

  CTASCommand(const ExecContext& context, query::Transaction& transaction,
              axiom::logical_plan::TableWriteNode& write,
              const CreateTableAsStmt& stmt)
    : _context{context},
      _transaction{transaction},
      _write{write},
      _stmt{stmt} {}

  yaclib::Future<Result> CreateTable();

  yaclib::Future<Result> PersistTableDefinition();

  bool IsTableCreated() const { return _table_created; }
  bool IsPersisted() const { return _is_persisted; }

  Stage GetStage() const { return _stage; }
  void SetStage(Stage s) { _stage = s; }

  absl::Mutex& AsyncResultMutex() { return _async_result_mutex; }

  bool IsAsyncResultReady() const {
    return _async_result.State() != yaclib::ResultState::Empty;
  }

  yaclib::Result<Result> TakeAsyncResult() { return std::move(_async_result); }

  void StoreAsyncResult(yaclib::Result<Result>&& r) {
    _async_result = std::move(r);
  }

 private:
  const ExecContext& _context;
  query::Transaction& _transaction;
  axiom::logical_plan::TableWriteNode& _write;
  const CreateTableAsStmt& _stmt;

  ObjectId _created_table_id;
  bool _table_created = false;
  bool _is_persisted = false;

  Stage _stage = Stage::None;
  yaclib::Result<Result> _async_result;
  absl::Mutex _async_result_mutex;
};

}  // namespace sdb::pg
