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

#include "connector/functions/hash.h"

#include <absl/hash/internal/city.h>

#include <bit>
#include <cstdint>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string_view>
#include <vector>

namespace sdb::connector {
namespace {

static_assert(std::endian::native == std::endian::little);

bool IsHashable(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::HUGEINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
    case duckdb::LogicalTypeId::UHUGEINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
      return true;
    default:
      return false;
  }
}

duckdb::unique_ptr<duckdb::FunctionData> HashCity64Bind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  for (auto& arg : input.GetArguments()) {
    const auto& type = arg->GetReturnType();
    auto normalized = duckdb::LogicalType::NormalizeType(type);
    if (!IsHashable(normalized)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
        ERR_MSG("hash_city64 does not support type ", type.ToString()));
    }
    if (normalized != type) {
      arg = duckdb::BoundCastExpression::AddCastToType(context, std::move(arg),
                                                       normalized);
    }
  }
  return nullptr;
}

std::string_view ValueBytes(duckdb::PhysicalType type,
                            const duckdb::data_t* data, duckdb::idx_t idx) {
  if (type == duckdb::PhysicalType::VARCHAR) {
    const auto& value = reinterpret_cast<const duckdb::string_t*>(data)[idx];
    return {value.GetData(), value.GetSize()};
  }
  const auto width = duckdb::GetTypeIdSize(type);
  return {reinterpret_cast<const char*>(data + idx * width), width};
}

void HashCity64Function(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  const auto count = args.size();
  const auto column_count = args.ColumnCount();
  std::vector<duckdb::UnifiedVectorFormat> formats(column_count);
  std::vector<duckdb::PhysicalType> types(column_count);
  for (duckdb::idx_t c = 0; c < column_count; ++c) {
    args.data[c].ToUnifiedFormat(count, formats[c]);
    types[c] = args.data[c].GetType().InternalType();
  }
  auto hash_row = [&](duckdb::idx_t row, uint64_t& hash) {
    for (duckdb::idx_t c = 0; c < column_count; ++c) {
      const auto& format = formats[c];
      const auto idx = format.sel->get_index(row);
      if (!format.validity.RowIsValid(idx)) {
        return false;
      }
      const auto bytes = ValueBytes(types[c], format.data, idx);
      hash = c == 0
               ? absl::hash_internal::CityHash64(bytes.data(), bytes.size())
               : absl::hash_internal::CityHash64WithSeed(bytes.data(),
                                                         bytes.size(), hash);
    }
    return true;
  };
  uint64_t hash = 0;
  if (args.AllConstant()) {
    if (hash_row(0, hash)) {
      duckdb::ConstantVector::Reference(result, duckdb::Value::UBIGINT(hash),
                                        duckdb::count_t(count));
    } else {
      duckdb::ConstantVector::SetNull(result, duckdb::count_t(count));
    }
    return;
  }
  auto writer = duckdb::FlatVector::Writer<uint64_t>(result, count);
  for (duckdb::idx_t row = 0; row < count; ++row) {
    if (hash_row(row, hash)) {
      writer.WriteValue(hash);
    } else {
      writer.WriteNull();
    }
  }
}

}  // namespace

void RegisterHashFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  duckdb::ScalarFunction func{"hash_city64",
                              {duckdb::LogicalType::ANY},
                              duckdb::LogicalType::UBIGINT,
                              HashCity64Function,
                              HashCity64Bind};
  func.GetSignature().AddArgs("values", duckdb::LogicalType::ANY);
  loader.RegisterFunction(func);
}

}  // namespace sdb::connector
