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

#include <optional>

#include "query/batch_executor.h"

namespace sdb::query {

class VeloxBatchExecutor;

class ExplainBatchExecutor final : public BatchExecutor {
 public:
  explicit ExplainBatchExecutor(VeloxBatchExecutor* velox = nullptr);

  void SetQuery(Query& query) final;

  yaclib::Future<velox::RowVectorPtr> Execute() final;
  void RequestCancel() final {}

 private:
  velox::RowVectorPtr BuildExplainBatch();

  VeloxBatchExecutor* _velox = nullptr;
  Query* _query = nullptr;
  std::optional<velox::RowVectorPtr> _result;
};

}  // namespace sdb::query
