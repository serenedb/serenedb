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
#include <string>
#include <string_view>

#include "query/executor.h"
#include "utils/exec_context.h"

struct IndexStmt;
struct IntoClause;
struct Node;

namespace sdb::pg {

class CommandExecutor : public query::Executor {
 public:
  explicit CommandExecutor(std::shared_ptr<ExecContext> context);

  void Init(query::Query& query) final { _query = &query; }
  yaclib::Future<> Execute(velox::RowVectorPtr& batch) final;
  yaclib::Future<> RequestCancel() final;

 protected:
  virtual yaclib::Future<> ExecuteImpl() = 0;

  std::shared_ptr<ExecContext> _context;
  query::Query* _query = nullptr;
};

class DDLExecutor final : public CommandExecutor {
 public:
  DDLExecutor(std::shared_ptr<ExecContext> context, const Node& node);

 protected:
  yaclib::Future<> ExecuteImpl() override;

 private:
  const Node& _node;
};

class CTASCreateTableExecutor final : public CommandExecutor {
 public:
  CTASCreateTableExecutor(std::shared_ptr<ExecContext> context,
                          const IntoClause& into, bool if_not_exists);

 protected:
  yaclib::Future<> ExecuteImpl() override;

 private:
  const IntoClause& _into;
  bool _if_not_exists;
};

class CreateIndexExecutor final : public CommandExecutor {
 public:
  CreateIndexExecutor(std::shared_ptr<ExecContext> context,
                      const IndexStmt& stmt);

 protected:
  yaclib::Future<> ExecuteImpl() override;

 private:
  const IndexStmt& _stmt;
};

class UpdateIndexesExecutor final : public CommandExecutor {
 public:
  UpdateIndexesExecutor(std::shared_ptr<ExecContext> context,
                        std::string_view schemaname,
                        std::string_view relation_name);

 protected:
  yaclib::Future<> ExecuteImpl() override;

 private:
  std::string _schemaname;
  std::string _relation_name;
};

class RemoveTombstoneExecutor final : public CommandExecutor {
 public:
  RemoveTombstoneExecutor(std::shared_ptr<ExecContext> context,
                          std::string_view schemaname, std::string_view name);

 protected:
  yaclib::Future<> ExecuteImpl() override;

 private:
  std::string _schemaname;
  std::string _name;
};

}  // namespace sdb::pg
