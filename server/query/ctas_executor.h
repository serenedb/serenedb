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

#include <memory>

#include "pg/commands/ctas.h"
#include "query/executor.h"
#include "query/velox_executor.h"

namespace sdb::query {

class Query;

class CreateTableExecutor final : public Executor {
 public:
  explicit CreateTableExecutor(std::unique_ptr<pg::CTASCommand> ctas_command);

  void SetQuery(Query& query) final { _query = &query; }

  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;
  yaclib::Future<> RequestCancel() final { return {}; }

  pg::CTASCommand& GetCommand() { return *_ctas_command; }

 private:
  std::unique_ptr<pg::CTASCommand> _ctas_command;
  Query* _query = nullptr;
  bool _fired = false;
};

class CTASVeloxExecutor final : public VeloxExecutor {
 public:
  explicit CTASVeloxExecutor(pg::CTASCommand& ctas_command);

  void SetQuery(Query& query) final { _query = &query; }

  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;

 private:
  pg::CTASCommand& _ctas_command;
};

class RemoveTombstoneExecutor final : public Executor {
 public:
  explicit RemoveTombstoneExecutor(pg::CTASCommand& ctas_command);

  void SetQuery(Query&) final {}

  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;
  yaclib::Future<> RequestCancel() final { return {}; }

 private:
  pg::CTASCommand& _ctas_command;
  bool _fired = false;
};

}  // namespace sdb::query
