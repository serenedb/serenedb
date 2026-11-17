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

#include <array>
#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "connector/copy_from_batches.h"
#include "connector/copy_to_file.h"
#include "pg/connection_context.h"
#include "pg/serialize.h"
#include "pg/types/logical.h"
#include "server/utils/message_buffer.h"

namespace sdb::connector {
namespace {

// PGCOPY header: 11-byte signature, int32 flags (0), int32 header-extension
// (0).
constexpr std::array<uint8_t, 19> kPgCopyHeader{
  'P',  'G', 'C', 'O', 'P', 'Y', '\n', 0xFF, '\r', '\n',
  0x00, 0,   0,   0,   0,   0,   0,    0,    0};

[[noreturn]] void RejectBinaryCopyOption(std::string_view key, bool from) {
  if (key == "delimiter" || key == "delim" || key == "sep" ||
      key == "separator") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("cannot specify DELIMITER in BINARY mode"));
  }
  if (key == "null" || key == "nullstr") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("cannot specify NULL in BINARY mode"));
  }
  if (key == "header") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot specify HEADER in BINARY mode"));
  }
  if (key == "ignore_errors" || key == "on_error") {
    if (!from) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("COPY ON_ERROR cannot be used with COPY TO"));
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("only ON_ERROR STOP is allowed in BINARY mode"));
  }
  if (key == "rejects_limit" || key == "reject_limit") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE"));
  }
  if (key == "quote" || key == "escape" || key == "force_quote" ||
      key == "force_not_null" || key == "force_null") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("COPY option \"", key, "\" is only available in CSV mode"));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                  ERR_MSG("COPY option \"", key, "\" not recognized"));
}

class PgBinaryCopyEncoder final : public CopyChunkEncoder {
 public:
  PgBinaryCopyEncoder(duckdb::ClientContext& context,
                      std::span<const duckdb::LogicalType> types) {
    sdb::pg::FillContext(ConnectionContext::Get(context), _ctx);
    _serializers.reserve(types.size());
    for (const auto& type : types) {
      _serializers.push_back(
        sdb::pg::GetSerialization(type, sdb::pg::VarFormat::Binary, _ctx));
    }
  }

  void Encode(const duckdb::DataChunk& chunk, message::Buffer& out) final {
    const auto rows = chunk.size();
    if (rows == 0) {
      return;
    }
    const auto columns = static_cast<uint16_t>(chunk.ColumnCount());
    auto& decoded = _ctx.decoded;
    decoded.clear();
    decoded.resize(columns);
    for (uint16_t column = 0; column < columns; ++column) {
      duckdb::Vector::RecursiveToUnifiedFormat(chunk.data[column], rows,
                                               decoded[column]);
    }
    for (duckdb::idx_t row = 0; row < rows; ++row) {
      message::Writer writer{out};
      _ctx.writer = &writer;
      absl::big_endian::Store16(writer.Alloc(2), columns);
      for (uint16_t column = 0; column < columns; ++column) {
        _serializers[column](_ctx, decoded[column], row);
      }
      writer.Commit(false);
    }
  }

 private:
  sdb::pg::SerializationContext _ctx;
  std::vector<sdb::pg::SerializationFunction> _serializers;
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
    writer.Write({reinterpret_cast<const char*>(kPgCopyHeader.data()),
                  kPgCopyHeader.size()});
    writer.Commit(false);
  }

  void WriteTrailer(message::Buffer& out) const final {
    message::Writer writer{out};
    absl::big_endian::Store16(writer.Alloc(2), static_cast<int16_t>(-1));
    writer.Commit(false);
  }

 private:
  duckdb::vector<duckdb::LogicalType> _sql_types;
};

duckdb::unique_ptr<duckdb::FunctionData> BindCopyTo(
  duckdb::ClientContext&, duckdb::CopyFunctionBindInput& input,
  const duckdb::vector<duckdb::Identifier>&,
  const duckdb::vector<duckdb::LogicalType>& sql_types) {
  CheckBinaryCopyOptions(input.info, false);
  return duckdb::make_uniq<PgBinaryCopyBindData>(sql_types);
}

struct PgBinaryCopyFromBindData final : public duckdb::TableFunctionData {
  PgBinaryCopyFromBindData(duckdb::vector<duckdb::LogicalType> types,
                           std::string path)
    : sql_types{std::move(types)}, file_path{std::move(path)} {}
  duckdb::vector<duckdb::LogicalType> sql_types;
  std::string file_path;
  bool ordered = false;
};

duckdb::unique_ptr<duckdb::FunctionData> BindFrom(
  duckdb::ClientContext& context, duckdb::CopyFromFunctionBindInput& input,
  duckdb::vector<duckdb::Identifier>&,
  duckdb::vector<duckdb::LogicalType>& expected_types) {
  CheckBinaryCopyOptions(input.info, true);
  auto result = duckdb::make_uniq<PgBinaryCopyFromBindData>(
    expected_types, input.info.file_path);
  result->ordered = BindCopyFrom(context, input);
  return result;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobalFrom(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind = input.bind_data->Cast<PgBinaryCopyFromBindData>();
  return InitCopyFromGlobal(
    context, bind.file_path,
    pg::CopyInTarget{
      .binary = true,
      .types = {bind.sql_types.begin(), bind.sql_types.end()},
    },
    bind.ordered);
}

}  // namespace

void CheckBinaryCopyOptions(const duckdb::CopyInfo& info, bool from) {
  if (!info.options.empty()) {
    RejectBinaryCopyOption(info.options.begin()->first.GetIdentifierName(),
                           from);
  }
  if (!info.parsed_options.empty()) {
    RejectBinaryCopyOption(
      info.parsed_options.begin()->first.GetIdentifierName(), from);
  }
}

void RegisterPgBinaryCopyFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  duckdb::CopyFunction func("binary");
  func.extension = "binary";
  func.copy_to_bind = BindCopyTo;
  SetCopyToFileCallbacks(func);
  func.copy_from_bind = BindFrom;
  func.copy_from_function =
    duckdb::TableFunction("pg_binary_copy_from", {}, ScanCopyFrom,
                          /*bind=*/nullptr, InitGlobalFrom, InitCopyFromLocal);
  loader.RegisterFunction(std::move(func));
}

}  // namespace sdb::connector
