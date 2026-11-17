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
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include "connector/constraint_keys.h"

namespace sdb::connector {
namespace {

void EncodeKeyFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  KeyEncoder{duckdb::Allocator::Get(state.GetContext()), args.GetTypes()}
    .Encode(args, result);
}

}  // namespace

void RegisterKeyEncodingFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  duckdb::ScalarFunction func{"sdb_encode_key",
                              {duckdb::LogicalType::ANY},
                              duckdb::LogicalType::BLOB,
                              EncodeKeyFunction};
  func.GetSignature()
    .AddArgs("args", duckdb::LogicalType::ANY)
    .AddKwargs("kwargs", duckdb::LogicalType::ANY);
  func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  loader.RegisterFunction(func);
}

}  // namespace sdb::connector
