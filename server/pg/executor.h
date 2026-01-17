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

#include "pg/executor_request.h"
#include "query/external_executor.h"
#include "utils/exec_context.h"

struct Node;

namespace sdb::pg {

class Executor final : public query::ExternalExecutor {
 public:
  explicit Executor(std::shared_ptr<ExecContext> context,
                    std::shared_ptr<ExecutorRequest> req);

  yaclib::Future<Result> Execute() final;
  yaclib::Future<> RequestCancel() final;

 private:
  yaclib::Future<Result> ExecuteGenericDDLRequest();

  std::shared_ptr<ExecContext> _context;
  std::shared_ptr<ExecutorRequest> _req;
};

}  // namespace sdb::pg
