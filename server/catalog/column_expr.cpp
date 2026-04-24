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

#include "catalog/column_expr.h"

#include <vpack/vpack_helper.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>

namespace sdb {

ColumnExpr::ColumnExpr(duckdb::unique_ptr<duckdb::ParsedExpression> expr)
  : _expr(std::move(expr)) {}

Result ColumnExpr::FromVPack(vpack::Slice slice, ColumnExpr& column_expr) {
  auto blob = basics::VPackHelper::getString(slice, "duckdb_expr", {});
  if (blob.empty()) {
    return {ERROR_BAD_PARAMETER, "column expression must contain duckdb_expr"};
  }
  duckdb::MemoryStream stream(
    reinterpret_cast<duckdb::data_ptr_t>(const_cast<char*>(blob.data())),
    blob.size());
  duckdb::BinaryDeserializer deserializer(stream);
  deserializer.Begin();
  column_expr._expr = duckdb::ParsedExpression::Deserialize(deserializer);
  deserializer.End();
  return {};
}

void ColumnExpr::ToVPack(vpack::Builder& builder) const {
  SDB_ASSERT(_expr);
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer(stream);
  serializer.Begin();
  _expr->Serialize(serializer);
  serializer.End();
  builder.openObject();
  builder.add("duckdb_expr",
              std::string_view(reinterpret_cast<const char*>(stream.GetData()),
                               stream.GetPosition()));
  builder.close();
}

}  // namespace sdb
