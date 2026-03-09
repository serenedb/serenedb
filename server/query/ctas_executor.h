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

#include "query/velox_executor.h"
#include "utils/exec_context.h"

struct IntoClause;

namespace sdb::query {

class Query;

class CTASVeloxExecutor final : public VeloxExecutor {
 public:
  CTASVeloxExecutor(const ExecContext& context, const IntoClause& into)
    : _context{context}, _into{into} {}

  void Init(Query& query) final { _query = &query; }

  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;

 private:
  void Rollback();

  const ExecContext& _context;
  const IntoClause& _into;
};

}  // namespace sdb::query
