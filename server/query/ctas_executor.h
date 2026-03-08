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
#include "query/batch_executor.h"
#include "query/runner.h"

namespace sdb::query {

class Query;

class CTASExecutor final : public BatchExecutor {
 public:
  explicit CTASExecutor(std::unique_ptr<pg::CTASCommand> ctas_command);

  void SetQuery(Query& query) final { _query = &query; }

  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;
  void RequestCancel() final;

 private:
  std::unique_ptr<pg::CTASCommand> _ctas_command;
  Query* _query = nullptr;
  Runner _runner;
};

}  // namespace sdb::query
