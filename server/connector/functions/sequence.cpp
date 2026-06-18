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
#include "catalog/object.h"
#include "catalog/sequence.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

std::shared_ptr<catalog::Sequence> ResolveSequence(
  duckdb::ClientContext& context, std::string_view qualified,
  catalog::AclMode need) {
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
  auto seq =
    snapshot->GetSequence(catalog::RequireAccess(conn_ctx.GetRoleId(), need),
                          database_id, schema->GetId(), qname.name);
  if (!seq) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("relation \"", qualified, "\" does not exist"));
  }
  return seq;
}

uint64_t GetValue(const catalog::SequenceOptions& opts, uint64_t raw) {
  if (!opts.cycle) [[likely]] {
    return raw > opts.max_value ? opts.max_value : raw;
  }
  auto range_raw =
    ((opts.max_value - opts.min_value) / opts.increment + 1) * opts.increment;
  return opts.min_value + (raw - opts.min_value) % range_raw;
}

[[noreturn]] void ThrowMaxReached(const catalog::SequenceOptions& opts,
                                  std::string_view qualified) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                  ERR_MSG("nextval: reached maximum value of sequence \"",
                          qualified, "\" (", opts.max_value, ")"));
}

[[noreturn]] void ThrowSetvalOutOfBounds(std::string_view qualified) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("setval: value out of bounds for sequence \"", qualified, "\""));
}

uint64_t Nextval(catalog::Sequence& seq, std::string_view qualified) {
  const auto& opts = seq.Options();
  uint64_t raw = seq.Reserve(opts.increment) + opts.increment - 1;
  if (!opts.cycle && raw > opts.max_value) [[unlikely]] {
    ThrowMaxReached(opts, qualified);
  }
  return GetValue(opts, raw);
}

uint64_t Nextval(duckdb::ClientContext& context, std::string_view qualified) {
  return Nextval(
    *ResolveSequence(context, qualified,
                     catalog::AclMode::Usage | catalog::AclMode::Update),
    qualified);
}

uint64_t Currval(duckdb::ClientContext& context, std::string_view qualified) {
  auto seq = ResolveSequence(
    context, qualified, catalog::AclMode::Usage | catalog::AclMode::Select);
  uint64_t raw = seq->Read();
  if (raw == seq->Options().Seed()) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("currval of sequence \"", qualified,
                            "\" is not yet defined in this session"));
  }
  return GetValue(seq->Options(), raw);
}

uint64_t Setval(duckdb::ClientContext& context, std::string_view qualified,
                uint64_t value, bool is_called) {
  auto seq = ResolveSequence(context, qualified, catalog::AclMode::Update);
  const auto& opts = seq->Options();
  if (value < opts.min_value || value > opts.max_value) {
    ThrowSetvalOutOfBounds(qualified);
  }
  // Snap to the inc-stepped lattice so _cnt stays on-lattice. GetValue's
  // cycle wrap assumes (raw - min) is a multiple of inc; an off-lattice
  // _cnt would let it emit values past max. Returning the un-snapped input
  // matches PG's setval contract (the return value echoes the argument).
  uint64_t snapped =
    opts.min_value +
    ((value - opts.min_value) / opts.increment) * opts.increment;
  uint64_t to_persist = is_called ? snapped : snapped - opts.increment;
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
    auto seq = ResolveSequence(
      context, qualified, catalog::AclMode::Usage | catalog::AclMode::Update);
    const auto& opts = seq->Options();
    uint64_t batch_span = static_cast<uint64_t>(num_rows) * opts.increment;

    uint64_t base = seq->Reserve(batch_span);
    uint64_t r0 = base + opts.increment - 1;  // raw of row 0

    if (!opts.cycle && base + batch_span - 1 > opts.max_value) [[unlikely]] {
      ThrowMaxReached(opts, qualified);
    }
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      out.WriteValue(GetValue(opts, r0 + i * opts.increment));
    }
    return;
  }

  duckdb::UnifiedVectorFormat fmt;
  args.data[0].ToUnifiedFormat(num_rows, fmt);
  auto* names = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    auto idx = fmt.sel->get_index(i);
    std::string_view seq_name{names[idx].GetData(), names[idx].GetSize()};
    out.WriteValue(Nextval(context, seq_name));
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
    out.WriteValue(Currval(context, seq_name));
  }
}

void Setval2Function(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int64_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t name, int64_t value) -> int64_t {
      std::string_view seq_name{name.GetData(), name.GetSize()};
      if (value < 0) {
        ThrowSetvalOutOfBounds(seq_name);
      }
      return Setval(context, seq_name, static_cast<uint64_t>(value),
                    /*is_called=*/true);
    });
}

void Setval3Function(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::TernaryExecutor::Execute<duckdb::string_t, int64_t, bool, int64_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t name, int64_t value, bool is_called) -> int64_t {
      std::string_view seq_name{name.GetData(), name.GetSize()};
      if (value < 0) {
        ThrowSetvalOutOfBounds(seq_name);
      }
      return Setval(context, seq_name, static_cast<uint64_t>(value), is_called);
    });
}

}  // namespace

void RegisterSequenceFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  {
    duckdb::ScalarFunction func{
      std::string{kNextval},
      {duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BIGINT,
      NextvalFunction,
    };
    func.SetVolatile();
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      std::string{kCurrval},
      {duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BIGINT,
      CurrvalFunction,
    };
    func.SetVolatile();
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      std::string{kSetval},
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BIGINT,
      Setval2Function,
    };
    func.SetVolatile();
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      std::string{kSetval},
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
       duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::BIGINT,
      Setval3Function,
    };
    func.SetVolatile();
    loader.RegisterFunction(func);
  }
}

}  // namespace sdb::connector
