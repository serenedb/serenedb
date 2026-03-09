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

#include <axiom/logical_plan/LogicalPlanNode.h>

#include <memory>

#include "catalog/identifiers/object_id.h"
#include "query/executor.h"
#include "query/velox_executor.h"
#include "utils/exec_context.h"
#include "yaclib/async/future.hpp"

struct IntoClause;

namespace sdb::query {

class Query;
class Transaction;

class CTASCommand {
 public:
  CTASCommand(const ExecContext& context, Transaction& transaction,
              axiom::logical_plan::TableWriteNode& write,
              const IntoClause& into, bool if_not_exists)
    : _context{context},
      _transaction{transaction},
      _write{write},
      _into{into},
      _if_not_exists{if_not_exists} {}

  yaclib::Future<> CreateTable();

  void Rollback();

 private:
  const ExecContext& _context;
  Transaction& _transaction;
  axiom::logical_plan::TableWriteNode& _write;
  const IntoClause& _into;
  bool _if_not_exists;

  std::string _schema;
  std::string_view _table_name;
  ObjectId _db;
};

class CreateTableExecutor final : public Executor {
 public:
  explicit CreateTableExecutor(std::unique_ptr<CTASCommand> ctas_command);

  void Init(Query& query) final { _query = &query; }

  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;
  yaclib::Future<> RequestCancel() final { return {}; }

  CTASCommand& GetCommand() { return *_ctas_command; }

 private:
  std::unique_ptr<CTASCommand> _ctas_command;
  Query* _query = nullptr;
  bool _fired = false;
};

class CTASVeloxExecutor final : public VeloxExecutor {
 public:
  explicit CTASVeloxExecutor(CTASCommand& ctas_command);

  void Init(Query& query) final { _query = &query; }

  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;

 private:
  CTASCommand& _ctas_command;
};

}  // namespace sdb::query
