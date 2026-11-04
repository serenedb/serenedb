////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/main/engine_host.hpp>

namespace sdb::server::query {

enum class EngineMode : uint8_t {
  kServer,
  kShell,
};

class EngineHost final : public duckdb::EngineHost {
 public:
  explicit EngineHost(EngineMode mode) : _mode{mode} {}

  void LoadExtensions(duckdb::DuckDB& db) final;
  std::span<const duckdb::DefaultType> GetTypes() final;

  void OnConnectionOpened(duckdb::ClientContext& context) final;
  void OnConnectionClosed(duckdb::ClientContext& context) final {}
  void OnBeginExtensionLoad(duckdb::DatabaseInstance& db,
                            const duckdb::string& name) final {}
  void OnExtensionLoaded(duckdb::DatabaseInstance& db,
                         const duckdb::string& name) final {}
  void OnExtensionLoadFail(duckdb::DatabaseInstance& db,
                           const duckdb::string& name,
                           const duckdb::ErrorData& error) final {}

  duckdb::BoundStatement BindStatement(duckdb::ClientContext& context,
                                       duckdb::Binder& binder,
                                       duckdb::SQLStatement& statement) final;
  void PostBind(duckdb::ClientContext& context, duckdb::Binder& binder,
                duckdb::BoundStatement& statement) final;
  duckdb::GetSQLValueFunctionReturnType BindSQLValueFunction(
    duckdb::ClientContext& context, duckdb::Binder& binder,
    const duckdb::string& name,
    duckdb::unique_ptr<duckdb::ParsedExpression>& result) final;
  duckdb::unique_ptr<duckdb::LogicalExtensionOperator> DeserializeOperator(
    duckdb::ClientContext& context, const duckdb::string& extension,
    duckdb::Deserializer& deserializer) final;

  void PreOptimize(duckdb::Optimizer& optimizer,
                   duckdb::unique_ptr<duckdb::LogicalOperator>& plan) final;
  void BeforeOptimizer(duckdb::OptimizerType type, duckdb::Optimizer& optimizer,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) final;
  void AfterOptimizer(duckdb::OptimizerType type, duckdb::Optimizer& optimizer,
                      duckdb::unique_ptr<duckdb::LogicalOperator>& plan) final;
  void PostOptimize(duckdb::Optimizer& optimizer,
                    duckdb::unique_ptr<duckdb::LogicalOperator>& plan) final;
  bool IsRewriteBarrier(duckdb::ClientContext& context,
                        const duckdb::Expression& parent,
                        bool parent_frozen) final;

 private:
  EngineMode _mode;
};

}  // namespace sdb::server::query
