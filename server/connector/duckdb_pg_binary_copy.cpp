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

#include "connector/duckdb_pg_binary_copy.h"

#include <absl/base/internal/endian.h>

#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/formats/copy_options.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "connector/copy_from_batches.h"
#include "connector/copy_to_file.h"
#include "connector/duckdb_pg_text_copy.h"
#include "pg/connection_context.h"
#include "pg/copy_binary.h"
#include "pg/serialize.h"
#include "pg/types/logical.h"
#include "server/utils/message_buffer.h"

namespace sdb::connector {
namespace {

void CheckBinaryOptions(const duckdb::CopyInfo& info, bool from) {
  duckdb::CheckCopyFormatOptions(
    duckdb::ParseCopyFormatOptions(duckdb::CopyFormat::BINARY, from,
                                   info.options),
    from);
}

sdb::pg::SerializationContext BinaryCopyContext(
  duckdb::ClientContext& context) {
  sdb::pg::SerializationContext ctx;
  sdb::pg::FillContext(ConnectionContext::Get(context), ctx);
  return ctx;
}

class PgBinaryCopyEncoder final : public CopyChunkEncoder {
 public:
  PgBinaryCopyEncoder(duckdb::ClientContext& context,
                      std::span<const duckdb::LogicalType> types)
    : _ctx{BinaryCopyContext(context)},
      _serializer{types,
                  std::vector<sdb::pg::VarFormat>(types.size(),
                                                  sdb::pg::VarFormat::Binary),
                  _ctx} {}

  void Encode(const duckdb::DataChunk& chunk, message::Buffer& out) final {
    _serializer.Write(out, _ctx, chunk, 0, chunk.size(), {.field_count = true});
  }

 private:
  sdb::pg::SerializationContext _ctx;
  sdb::pg::ChunkSerializer _serializer;
};

class PgBinaryCopyBindData final : public CopyToFileBindData {
 public:
  explicit PgBinaryCopyBindData(duckdb::vector<duckdb::LogicalType> types)
    : _sql_types{std::move(types)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<PgBinaryCopyBindData>(_sql_types);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    return _sql_types == other.Cast<PgBinaryCopyBindData>()._sql_types;
  }

  std::unique_ptr<CopyChunkEncoder> MakeEncoder(
    duckdb::ClientContext& context) const final {
    return std::make_unique<PgBinaryCopyEncoder>(context, _sql_types);
  }

  void WriteHeader(duckdb::ClientContext&, message::Buffer& out) const final {
    message::Writer writer{out};
    writer.Write(sdb::pg::kPgCopyHeader);
    writer.Commit(false);
  }

  void WriteTrailer(message::Buffer& out) const final {
    message::Writer writer{out};
    absl::big_endian::Store16(writer.Alloc(sizeof(sdb::pg::kPgCopyTrailer)),
                              sdb::pg::kPgCopyTrailer);
    writer.Commit(false);
  }

 private:
  duckdb::vector<duckdb::LogicalType> _sql_types;
};

duckdb::unique_ptr<duckdb::FunctionData> BindCopyTo(
  duckdb::ClientContext&, duckdb::CopyFunctionBindInput& input,
  const duckdb::vector<duckdb::Identifier>&,
  const duckdb::vector<duckdb::LogicalType>& sql_types) {
  CheckBinaryOptions(input.info, false);
  return duckdb::make_uniq<PgBinaryCopyBindData>(sql_types);
}

struct PgBinaryCopyFromBindData final : public duckdb::TableFunctionData {
  PgBinaryCopyFromBindData(std::string table_p,
                           duckdb::vector<duckdb::LogicalType> types,
                           std::vector<std::string> names_p, std::string path)
    : table{std::move(table_p)},
      sql_types{std::move(types)},
      names{std::move(names_p)},
      file_path{std::move(path)} {}
  std::string table;
  duckdb::vector<duckdb::LogicalType> sql_types;
  std::vector<std::string> names;
  std::string file_path;
  bool ordered = false;
};

duckdb::unique_ptr<duckdb::FunctionData> BindFrom(
  duckdb::ClientContext& context, duckdb::CopyFromFunctionBindInput& input,
  duckdb::vector<duckdb::Identifier>& expected_names,
  duckdb::vector<duckdb::LogicalType>& expected_types) {
  CheckBinaryOptions(input.info, true);
  auto result = duckdb::make_uniq<PgBinaryCopyFromBindData>(
    input.info.GetQualifiedName().Name().GetIdentifierName(), expected_types,
    CopyNames(expected_names), input.info.file_path);
  result->ordered = BindCopyFrom(context, input);
  return result;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobalFrom(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind = input.bind_data->Cast<PgBinaryCopyFromBindData>();
  return InitCopyFromGlobal(
    context, bind.file_path,
    pg::CopyInTarget{
      .table = bind.table,
      .types = {bind.sql_types.begin(), bind.sql_types.end()},
      .names = bind.names,
    },
    bind.ordered);
}

}  // namespace

void RegisterPgBinaryCopyFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  duckdb::CopyFunction func("binary");
  func.extension = "binary";
  func.copy_to_bind = BindCopyTo;
  func.copy_to_select = [](duckdb::CopyToSelectInput& input) {
    return CopyToSelect(input, {}, {}, duckdb::ValueRendering::POSTGRES);
  };
  SetCopyToFileCallbacks(func);
  func.copy_from_bind = BindFrom;
  func.copy_from_function =
    duckdb::TableFunction("pg_binary_copy_from", {}, ScanCopyFrom,
                          /*bind=*/nullptr, InitGlobalFrom, InitCopyFromLocal);
  loader.RegisterFunction(std::move(func));
}

}  // namespace sdb::connector
