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

#include "network/http/quack/statement.h"

#include <absl/cleanup/cleanup.h>

#include <duckdb/main/client_config.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "quack_message.hpp"
#include "quack_session_state.hpp"
#include "query/driven_query.h"

namespace sdb::network::http::quack {

StatementRunner::StatementRunner(
  duckdb::ClientContext& context,
  duckdb::shared_ptr<duckdb::QuackResultStream> stream, std::string sql,
  bool describe_only)
  : _context{context},
    _stream{std::move(stream)},
    _sql{std::move(sql)},
    _describe_only{describe_only} {}

StatementRunner::~StatementRunner() {
  if (_finished) {
    return;
  }
  duckdb::ErrorData error{duckdb::ExceptionType::INTERRUPT,
                          "Interrupted: the statement was abandoned"};
  if (_started) {
    Fail(std::move(error));
  } else {
    _stream->buffer.SetError(std::move(error));
  }
}

StepResult StatementRunner::Step(const std::function<void()>& on_reschedule) {
  if (_finished) {
    return StepResult::Finished;
  }
  try {
    if (_running) {
      return Execute(on_reschedule);
    }
    if (Stopped()) {
      return Done();
    }
    if (!_started) {
      Start();
      if (_describe_only && Describe()) {
        return Done();
      }
    }
    if (_next_statement == _statements.size()) {
      return FinishStream();
    }
    return Submit(std::move(_statements[_next_statement++]));
  } catch (const std::exception& e) {
    return Fail(duckdb::ErrorData{e});
  }
}

void StatementRunner::Start() {
  _started = true;
  if (auto session_state = duckdb::QuackSessionState::Get(_context)) {
    session_state->SetStatement(_stream);
  }
  auto iterator = _context.IterateStatements(_sql);
  size_t commands = 0;
  while (iterator.Peek()) {
    if (auto statement = iterator.GetStatementForExecution()) {
      _statements.push_back(std::move(statement));
    }
    if (iterator.PeelDone() && ++commands > 1) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("cannot insert multiple commands into a prepared statement"));
    }
  }
}

bool StatementRunner::Describe() {
  if (_statements.size() != 1) {
    return false;
  }
  auto signature = _context.BindStatement(std::move(_statements[0]));
  _next_statement = 1;
  _stream->SignalBound(std::move(signature.types), signature.names, true);
  _stream->announced_total = 0;
  _stream->buffer.Finish(_stream->announced_total);
  return true;
}

StepResult StatementRunner::Submit(
  duckdb::unique_ptr<duckdb::SQLStatement> statement) {
  auto& config = duckdb::ClientConfig::GetConfig(_context);
  config.get_result_collector = [stream = _stream](
                                  duckdb::ClientContext& context,
                                  duckdb::PreparedStatementData& data) {
    return duckdb::MakeQuackFetchCollector(context, data, stream);
  };
  absl::Cleanup reset_collector = [&config] {
    config.get_result_collector = nullptr;
  };
  _running = _context.Submit(std::move(statement), query::DrivenQuery());
  std::move(reset_collector).Invoke();
  if (_running->HasError()) {
    return Fail(_running->GetErrorObject());
  }
  return StepResult::Ready;
}

StepResult StatementRunner::Execute(
  const std::function<void()>& on_reschedule) {
  if (_stream->buffer.HasError()) {
    _context.Interrupt();
  }
  switch (_running->ExecuteTask(on_reschedule)) {
    case duckdb::QueryResultState::NOT_READY:
      return StepResult::Ready;
    case duckdb::QueryResultState::BLOCKED:
    case duckdb::QueryResultState::NO_TASKS_AVAILABLE:
      return StepResult::Blocked;
    case duckdb::QueryResultState::READY:
    case duckdb::QueryResultState::FINISHED:
    case duckdb::QueryResultState::EXECUTION_ERROR:
      break;
  }
  if (_running->HasError()) {
    return Fail(_running->GetErrorObject());
  }
  auto result = query::TakeDrivenResult(*_running);
  if (!result) {
    result = std::move(_running);
  }
  _running.reset();
  if (result->HasError()) {
    return Fail(result->GetErrorObject());
  }
  _last_result = std::move(result);
  return StepResult::Ready;
}

StepResult StatementRunner::FinishStream() {
  if (!_stream->Bound()) {
    SendLastResult();
  }
  _stream->buffer.Finish(_stream->announced_total);
  return Done();
}

void StatementRunner::SendLastResult() {
  if (!_last_result || _last_result->GetNames().empty()) {
    _stream->buffer.SetError(
      duckdb::ErrorData{duckdb::ExceptionType::INVALID_INPUT,
                        "Query did not return any columns"});
    return;
  }
  _stream->SignalBound(_last_result->GetTypes(), _last_result->GetNames(),
                       false);
  duckdb::unique_ptr<duckdb::QuackChunkPayloadWriter> writer;
  duckdb::idx_t rows = 0;
  while (auto chunk = _last_result->Fetch()) {
    if (chunk->size() == 0) {
      continue;
    }
    if (!writer) {
      writer = duckdb::make_uniq<duckdb::QuackChunkPayloadWriter>(
        0, _stream->wire_version);
    }
    writer->AppendChunk(*chunk);
    rows += chunk->size();
  }
  if (!writer) {
    _stream->announced_total = 0;
    return;
  }
  auto sealed = writer->Seal();
  duckdb::QuackFetchPayload entry;
  entry.payload = std::move(sealed.payload);
  entry.payload_size = sealed.payload_size;
  entry.chunk_count = sealed.chunk_count;
  entry.rows = rows;
  const auto bytes = entry.payload_size;
  _stream->buffer.PushBatch(1, std::move(entry), bytes);
  _stream->announced_total = 1;
}

StepResult StatementRunner::Done() {
  _finished = true;
  return StepResult::Finished;
}

StepResult StatementRunner::Fail(duckdb::ErrorData error) {
  _running.reset();
  _stream->buffer.SetError(error);
  if (auto session_state = duckdb::QuackSessionState::Get(_context)) {
    session_state->Streams().Fail(error);
  }
  return Done();
}

}  // namespace sdb::network::http::quack
