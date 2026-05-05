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

#include "connector/functions/sequence.h"

#include <duckdb/common/exception.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/ternary_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/qualified_name.hpp>

#include <memory>
#include <string>
#include <string_view>

#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/sequence.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

// Resolve a (possibly schema-qualified) sequence name to a Sequence in the
// current connection's catalog snapshot. Throws CatalogException if the
// schema or sequence does not exist or the resolved object is not a Sequence.
std::shared_ptr<catalog::Sequence> ResolveSequence(
  duckdb::ClientContext& context, std::string_view qualified) {
  auto qname = duckdb::QualifiedName::Parse(std::string{qualified});
  std::string schema_name = qname.schema.empty()
                              ? std::string{StaticStrings::kPublic}
                              : qname.schema;

  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto database_id = conn_ctx.GetDatabaseId();
  auto schema = snapshot->GetSchema(database_id, schema_name);
  if (!schema) {
    throw duckdb::CatalogException("schema \"%s\" does not exist", schema_name);
  }
  auto seq = snapshot->GetSequence(database_id, schema->GetId(), qname.name);
  if (!seq) {
    throw duckdb::CatalogException(
      "relation \"%s\" does not exist", std::string{qualified});
  }
  return seq;
}

// nextval('seq'): atomically advance the persistent counter by `increment`
// and return the new value. We seed the counter at CREATE SEQUENCE time so
// that the first nextval emits start_value (counter = start - increment).
int64_t Nextval(duckdb::ClientContext& context, std::string_view qualified) {
  auto seq = ResolveSequence(context, qualified);
  const auto& opts = seq->Options();
  auto inc_u = static_cast<uint64_t>(opts.increment);
  uint64_t base = seq->Reserve(inc_u);
  uint64_t high_water = base + inc_u - 1;
  auto value = static_cast<int64_t>(high_water);

  if (value > opts.max_value) {
    if (!opts.cycle) {
      throw duckdb::Exception(
        duckdb::ExceptionType::SEQUENCE,
        "nextval: reached maximum value of sequence \"" +
          std::string{qualified} + "\" (" +
          std::to_string(opts.max_value) + ")");
    }
    // Cycle: reset counter so the next call starts from min_value.
    // Best-effort under concurrent calls -- the current caller still gets
    // `value` (possibly > max). Strict atomic CYCLE is out of MVP scope.
    seq->Write(static_cast<uint64_t>(opts.min_value) - inc_u);
    value = opts.min_value;
  }
  return value;
}

// currval('seq'): the most-recently-emitted value across all callers.
// Throws if nextval has not been called for this sequence yet.
// (PG's spec is per-session; that level of fidelity is out of MVP scope.)
int64_t Currval(duckdb::ClientContext& context, std::string_view qualified) {
  auto seq = ResolveSequence(context, qualified);
  const auto& opts = seq->Options();
  auto persisted = static_cast<int64_t>(seq->Read());
  // Counter sentinel: at sequence creation we seed counter to start - inc, so
  // currval before any nextval should error.
  if (persisted == opts.start_value - opts.increment) {
    throw duckdb::Exception(duckdb::ExceptionType::SEQUENCE,
                            "currval: sequence \"" + std::string{qualified} +
                              "\" is not yet defined in this session");
  }
  return persisted;
}

// setval('seq', value [, is_called]): overwrite the persistent counter.
//   is_called=true (default): the next nextval returns value + increment.
//   is_called=false:          the next nextval returns value.
int64_t Setval(duckdb::ClientContext& context, std::string_view qualified,
               int64_t value, bool is_called) {
  auto seq = ResolveSequence(context, qualified);
  const auto& opts = seq->Options();
  if (value < opts.min_value || value > opts.max_value) {
    throw duckdb::Exception(duckdb::ExceptionType::SEQUENCE,
                            "setval: value out of bounds for sequence \"" +
                              std::string{qualified} + "\"");
  }
  auto to_persist = is_called
                      ? static_cast<uint64_t>(value)
                      : static_cast<uint64_t>(value - opts.increment);
  seq->Write(to_persist);
  return value;
}

// nextval/currval: emit one row at a time. We deliberately do NOT use
// UnaryExecutor here because the input (sequence name) is typically a
// constant vector, which UnaryExecutor would fast-path to a single call --
// nextval must advance the counter once per output row. Match upstream
// DuckDB's nextval impl: force FLAT_VECTOR and loop manually.
void NextvalFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  args.data[0].Flatten(args.size());
  auto* names = duckdb::FlatVector::GetData<duckdb::string_t>(args.data[0]);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto out = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (duckdb::idx_t i = 0; i < args.size(); ++i) {
    out[i] =
      Nextval(context,
              std::string_view{names[i].GetData(), names[i].GetSize()});
  }
}

void CurrvalFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  args.data[0].Flatten(args.size());
  auto* names = duckdb::FlatVector::GetData<duckdb::string_t>(args.data[0]);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto out = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (duckdb::idx_t i = 0; i < args.size(); ++i) {
    out[i] =
      Currval(context,
              std::string_view{names[i].GetData(), names[i].GetSize()});
  }
}

void Setval2Function(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int64_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t name, int64_t value) -> int64_t {
      return Setval(context, std::string_view{name.GetData(), name.GetSize()},
                    value, /*is_called=*/true);
    });
}

void Setval3Function(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::TernaryExecutor::Execute<duckdb::string_t, int64_t, bool, int64_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t name, int64_t value, bool is_called) -> int64_t {
      return Setval(context, std::string_view{name.GetData(), name.GetSize()},
                    value, is_called);
    });
}

}  // namespace

void RegisterSequenceFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  {
    duckdb::ScalarFunction func{
      "nextval",
      {duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BIGINT,
      NextvalFunction,
    };
    func.stability = duckdb::FunctionStability::VOLATILE;
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "currval",
      {duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BIGINT,
      CurrvalFunction,
    };
    func.stability = duckdb::FunctionStability::VOLATILE;
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "setval",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BIGINT,
      Setval2Function,
    };
    func.stability = duckdb::FunctionStability::VOLATILE;
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "setval",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
       duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::BIGINT,
      Setval3Function,
    };
    func.stability = duckdb::FunctionStability::VOLATILE;
    loader.RegisterFunction(func);
  }
}

}  // namespace sdb::connector
