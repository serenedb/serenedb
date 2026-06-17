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

#include "connector/functions/encode_key.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <string>
#include <vector>

#include "connector/key_encoding.h"

namespace sdb::connector {
namespace {

void EncodeKeyFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  const auto count = args.size();
  std::vector<duckdb::RecursiveUnifiedVectorFormat> formats(args.ColumnCount());
  for (duckdb::idx_t c = 0; c < args.ColumnCount(); ++c) {
    duckdb::Vector::RecursiveToUnifiedFormat(args.data[c], formats[c]);
  }
  std::string buf;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    bool any_null = false;
    for (const auto& fmt : formats) {
      if (!fmt.unified.validity.RowIsValid(fmt.unified.sel->get_index(row))) {
        any_null = true;
        break;
      }
    }
    if (any_null) {
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    buf.clear();
    for (const auto& fmt : formats) {
      key_encoding::AppendValue(buf, fmt, row);
    }
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result)[row] =
      duckdb::StringVector::AddStringOrBlob(result, buf.data(), buf.size());
  }
  if (count == 1) {
    result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  }
}

}  // namespace

void RegisterKeyEncodingFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  duckdb::ScalarFunction func{"sdb_encode_key",
                              {duckdb::LogicalType::ANY},
                              duckdb::LogicalType::BLOB,
                              EncodeKeyFunction};
  func.SetVarArgs(duckdb::LogicalType::ANY);
  func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  loader.RegisterFunction(func);
}

}  // namespace sdb::connector
