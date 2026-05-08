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

std::shared_ptr<catalog::Sequence> ResolveSequence(
  duckdb::ClientContext& context, std::string_view qualified) {
  auto qname = duckdb::QualifiedName::Parse(std::string{qualified});
  std::string_view schema_name =
    qname.schema.empty() ? StaticStrings::kPublic : qname.schema;

  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto database_id = conn_ctx.GetDatabaseId();
  auto schema = snapshot->GetSchema(database_id, schema_name);
  if (!schema) {
    throw duckdb::CatalogException("schema \"%s\" does not exist", schema_name);
  }
  auto seq = snapshot->GetSequence(database_id, schema->GetId(), qname.name);
  if (!seq) {
    throw duckdb::CatalogException("relation \"%s\" does not exist",
                                   std::string{qualified});
  }
  return seq;
}

// CREATE SEQUENCE seeds the counter to `start - increment`, so the first
// Reserve(increment) post-merge yields `start`. The same sentinel is what
// Currval inspects to detect "not yet called".
int64_t Nextval(catalog::Sequence& seq, std::string_view qualified) {
  const auto& opts = seq.Options();
  auto inc_u = static_cast<uint64_t>(opts.increment);
  uint64_t base = seq.Reserve(inc_u);
  uint64_t high_water = base + inc_u - 1;
  auto value = static_cast<int64_t>(high_water);

  if (value > opts.max_value) {
    if (!opts.cycle) {
      throw duckdb::Exception(duckdb::ExceptionType::SEQUENCE,
                              "nextval: reached maximum value of sequence \"" +
                                std::string{qualified} + "\" (" +
                                std::to_string(opts.max_value) + ")");
    }
    // CYCLE: best-effort -- this caller still gets `value` (possibly > max);
    // strict atomic cycle is out of scope.
    seq.Write(static_cast<uint64_t>(opts.min_value) - inc_u);
    value = opts.min_value;
  }
  return value;
}

int64_t Nextval(duckdb::ClientContext& context, std::string_view qualified) {
  return Nextval(*ResolveSequence(context, qualified), qualified);
}

// PG's currval is per-session; we report the global last-emitted value.
int64_t Currval(duckdb::ClientContext& context, std::string_view qualified) {
  auto seq = ResolveSequence(context, qualified);
  const auto& opts = seq->Options();
  auto persisted = static_cast<int64_t>(seq->Read());
  if (persisted == opts.start_value - opts.increment) {
    throw duckdb::Exception(duckdb::ExceptionType::SEQUENCE,
                            "currval: sequence \"" + std::string{qualified} +
                              "\" is not yet defined in this session");
  }
  return persisted;
}

int64_t Setval(duckdb::ClientContext& context, std::string_view qualified,
               int64_t value, bool is_called) {
  auto seq = ResolveSequence(context, qualified);
  const auto& opts = seq->Options();
  if (value < opts.min_value || value > opts.max_value) {
    throw duckdb::Exception(duckdb::ExceptionType::SEQUENCE,
                            "setval: value out of bounds for sequence \"" +
                              std::string{qualified} + "\"");
  }
  auto to_persist = is_called ? static_cast<uint64_t>(value)
                              : static_cast<uint64_t>(value - opts.increment);
  seq->Write(to_persist);
  return value;
}

// Vectorised: when the sequence-name argument is a constant vector (the
// usual `DEFAULT nextval('schema.t_id_seq')` case), reserve the whole chunk's
// worth of ticks in one Sequence::Reserve call. Per-row resolution falls back
// to the scalar Nextval helper.
void NextvalFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto num_rows = args.size();
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto out = duckdb::FlatVector::Writer<int64_t>(result, num_rows);

  if (args.data[0].GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR &&
      !duckdb::ConstantVector::IsNull(args.data[0])) {
    auto* name_data =
      duckdb::ConstantVector::GetData<duckdb::string_t>(args.data[0]);
    auto qualified =
      std::string_view{name_data->GetData(), name_data->GetSize()};
    auto seq = ResolveSequence(context, qualified);
    const auto& opts = seq->Options();
    auto inc_u = static_cast<uint64_t>(opts.increment);

    // Cycle / max are conditional on the current value -- splitting a batch
    // at the wrap point would interleave with concurrent setvals. Fall back
    // to the scalar path when this batch could touch max_value.
    uint64_t cur = seq->Read();
    uint64_t total = static_cast<uint64_t>(num_rows) * inc_u;
    bool unsafe_for_batch =
      opts.cycle ||
      (cur != 0 && static_cast<uint64_t>(opts.max_value) - cur < total);

    if (!unsafe_for_batch) {
      uint64_t base = seq->Reserve(total);
      for (duckdb::idx_t i = 0; i < num_rows; ++i) {
        out[i] = static_cast<int64_t>(base + i * inc_u + (inc_u - 1));
      }
      return;
    }

    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      out[i] = Nextval(*seq, qualified);
    }
    return;
  }

  args.data[0].Flatten(num_rows);
  auto* names = duckdb::FlatVector::GetData<duckdb::string_t>(args.data[0]);
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    out[i] = Nextval(context,
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
    out[i] = Currval(context,
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
