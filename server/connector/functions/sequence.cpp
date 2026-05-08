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
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

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
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema_name, "\" does not exist"));
  }
  auto seq = snapshot->GetSequence(database_id, schema->GetId(), qname.name);
  if (!seq) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("relation \"", qualified, "\" does not exist"));
  }
  return seq;
}

int64_t NextVal(catalog::Sequence& seq, int64_t value,
                std::string_view qualified) {
  const auto& opts = seq.Options();
  if (value <= opts.max_value) [[likely]] {
    return value;
  }
  if (!opts.cycle) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("nextval: reached maximum value of sequence \"",
                            qualified, "\" (", opts.max_value, ")"));
  }
  auto new_cycle = static_cast<uint64_t>(opts.min_value) -
                   static_cast<uint64_t>(opts.increment);
  seq.Write(new_cycle);
  return opts.min_value;
}

int64_t Nextval(catalog::Sequence& seq, std::string_view qualified) {
  auto inc_u = static_cast<uint64_t>(seq.Options().increment);
  uint64_t base = seq.Reserve(inc_u);
  return NextVal(seq, static_cast<int64_t>(base + inc_u - 1), qualified);
}

int64_t Nextval(duckdb::ClientContext& context, std::string_view qualified) {
  return Nextval(*ResolveSequence(context, qualified), qualified);
}

int64_t Currval(duckdb::ClientContext& context, std::string_view qualified) {
  auto seq = ResolveSequence(context, qualified);
  const auto& opts = seq->Options();
  auto persisted = static_cast<int64_t>(seq->Read());
  if (persisted == opts.start_value - opts.increment) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("currval: sequence \"", qualified,
                            "\" is not yet defined in this session"));
  }
  return persisted;
}

int64_t Setval(duckdb::ClientContext& context, std::string_view qualified,
               int64_t value, bool is_called) {
  auto seq = ResolveSequence(context, qualified);
  const auto& opts = seq->Options();
  if (value < opts.min_value || value > opts.max_value) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("setval: value out of bounds for sequence \"", qualified, "\""));
  }
  uint64_t to_persist = is_called ? value : value - opts.increment;
  seq->Write(to_persist);
  return value;
}

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
    std::string_view qualified{name_data->GetData(), name_data->GetSize()};
    auto seq = ResolveSequence(context, qualified);
    const auto& opts = seq->Options();
    uint64_t inc_u = opts.increment;
    uint64_t base = seq->Reserve(num_rows * inc_u);

    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      out[i] = NextVal(*seq, base + i * inc_u + (inc_u - 1), qualified);
    }
    return;
  }

  duckdb::UnifiedVectorFormat fmt;
  args.data[0].ToUnifiedFormat(num_rows, fmt);
  auto* names = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    auto idx = fmt.sel->get_index(i);
    std::string_view seq_name{names[idx].GetData(), names[idx].GetSize()};
    out[i] = Nextval(context, seq_name);
  }
}

void CurrvalFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto num_rows = args.size();

  if (args.data[0].GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR &&
      !duckdb::ConstantVector::IsNull(args.data[0])) {
    auto* name_data =
      duckdb::ConstantVector::GetData<duckdb::string_t>(args.data[0]);
    std::string_view qualified{name_data->GetData(), name_data->GetSize()};
    result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
    *duckdb::ConstantVector::GetData<int64_t>(result) =
      Currval(context, qualified);
    return;
  }

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto out = duckdb::FlatVector::Writer<int64_t>(result, num_rows);
  duckdb::UnifiedVectorFormat fmt;
  args.data[0].ToUnifiedFormat(num_rows, fmt);
  auto* names = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    auto idx = fmt.sel->get_index(i);
    std::string_view seq_name{names[idx].GetData(), names[idx].GetSize()};
    out[i] = Currval(context, seq_name);
  }
}

void Setval2Function(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int64_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t name, int64_t value) -> int64_t {
      std::string_view seq_name{name.GetData(), name.GetSize()};
      return Setval(context, seq_name, value, /*is_called=*/true);
    });
}

void Setval3Function(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::TernaryExecutor::Execute<duckdb::string_t, int64_t, bool, int64_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t name, int64_t value, bool is_called) -> int64_t {
      std::string_view seq_name{name.GetData(), name.GetSize()};
      return Setval(context, seq_name, value, is_called);
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
