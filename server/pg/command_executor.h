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

#include "query/executor.h"
#include "utils/exec_context.h"

struct Node;
struct RangeVar;

namespace sdb::pg {

enum class CommandRequestType { DDL, RemoveTombstone };

struct CommandExecutorRequest {
  CommandRequestType type;
  virtual ~CommandExecutorRequest() = default;

 protected:
  explicit CommandExecutorRequest(CommandRequestType type) : type{type} {}
};

struct DDLRequest final : CommandExecutorRequest {
  const Node& node;
  explicit DDLRequest(const Node& node)
    : CommandExecutorRequest{CommandRequestType::DDL}, node{node} {}
};

struct RemoveTombstoneRequest final : CommandExecutorRequest {
  const RangeVar& relation;
  explicit RemoveTombstoneRequest(const RangeVar& relation)
    : CommandExecutorRequest{CommandRequestType::RemoveTombstone},
      relation{relation} {}
};

class CommandExecutor final : public query::Executor {
 public:
  explicit CommandExecutor(std::shared_ptr<ExecContext> context,
                           std::unique_ptr<CommandExecutorRequest> request);

  void Init(query::Query&) final {}
  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;
  yaclib::Future<> RequestCancel() final;

 private:
  yaclib::Future<> ExecuteImpl();

  bool _fired = false;
  std::shared_ptr<ExecContext> _context;
  std::unique_ptr<CommandExecutorRequest> _request;
};

}  // namespace sdb::pg
