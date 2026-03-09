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

struct IntoClause;
struct Node;
struct RangeVar;

namespace sdb::pg {

enum class CommandRequestType { DDL, RemoveTombstone, CTASCreateTable };

struct CommandExecutorRequest {
  CommandRequestType type;
  virtual ~CommandExecutorRequest() = default;

 protected:
  explicit CommandExecutorRequest(CommandRequestType type) : type{type} {}
};

struct DDLRequest final : CommandExecutorRequest {
  explicit DDLRequest(const Node& node)
    : CommandExecutorRequest{CommandRequestType::DDL}, node{node} {}
  const Node& node;
};

struct RemoveTombstoneRequest final : CommandExecutorRequest {
  explicit RemoveTombstoneRequest(const RangeVar& relation)
    : CommandExecutorRequest{CommandRequestType::RemoveTombstone},
      relation{relation} {}
  const RangeVar& relation;
};

struct CTASCreateTableRequest final : CommandExecutorRequest {
  const IntoClause& into;
  bool if_not_exists;
  CTASCreateTableRequest(const IntoClause& into, bool if_not_exists)
    : CommandExecutorRequest{CommandRequestType::CTASCreateTable},
      into{into},
      if_not_exists{if_not_exists} {}
};

class CommandExecutor final : public query::Executor {
 public:
  explicit CommandExecutor(std::shared_ptr<ExecContext> context,
                           std::unique_ptr<CommandExecutorRequest> request);

  void Init(query::Query& query) final { _query = &query; }
  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;
  yaclib::Future<> RequestCancel() final;

 private:
  yaclib::Future<> ExecuteImpl();

  std::shared_ptr<ExecContext> _context;
  std::unique_ptr<CommandExecutorRequest> _request;
  query::Query* _query = nullptr;
};

}  // namespace sdb::pg
