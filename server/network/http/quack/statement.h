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

#include <duckdb/common/error_data.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/main/statement_iterator.hpp>
#include <duckdb/parser/sql_statement.hpp>
#include <functional>
#include <string>
#include <vector>

#include "quack_fetch_collector.hpp"

namespace sdb::network::http::quack {

enum class StepResult : uint8_t {
  Ready,
  Blocked,
  Finished,
};

// The SQL of one PREPARE, run on a session's context into the PREPARE's
// result stream. The session drives it with Step, so no thread waits on it.
class StatementRunner final {
 public:
  StatementRunner(duckdb::ClientContext& context,
                  duckdb::shared_ptr<duckdb::QuackResultStream> stream,
                  std::string sql, bool describe_only);
  ~StatementRunner();

  // `on_reschedule` fires, from any thread, once a Blocked statement can go on.
  StepResult Step(const std::function<void()>& on_reschedule);
  // A later PREPARE, a cancel or the end of the session stopped it.
  bool Stopped() const { return _stream->buffer.Finished(); }

 private:
  void Start();
  bool Describe();
  StepResult Submit(duckdb::unique_ptr<duckdb::SQLStatement> statement);
  StepResult Execute(const std::function<void()>& on_reschedule);
  StepResult FinishStream();
  void SendLastResult();
  StepResult Done();
  StepResult Fail(duckdb::ErrorData error);

  duckdb::ClientContext& _context;
  duckdb::shared_ptr<duckdb::QuackResultStream> _stream;
  std::string _sql;
  const bool _describe_only;
  bool _started = false;
  bool _finished = false;
  std::vector<duckdb::unique_ptr<duckdb::SQLStatement>> _statements;
  size_t _next_statement = 0;
  duckdb::unique_ptr<duckdb::QueryResult> _running;
  duckdb::unique_ptr<duckdb::QueryResult> _last_result;
};

}  // namespace sdb::network::http::quack
