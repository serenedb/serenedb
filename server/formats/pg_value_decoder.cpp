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

#include "formats/pg_value_decoder.h"

#include <bit>
#include <cstdint>
#include <duckdb/common/vector/flat_vector.hpp>
#include <exception>
#include <iresearch/utils/pg/error_mapping.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "pg/deserialize.h"
#include "pg/input_error.h"
#include "pg/sql_utils.h"

namespace sdb::formats {
namespace {

duckdb::ErrorData DecodeError(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& e) {
    const auto sql = irs::pg::ToSqlError(e);
    std::string sqlstate(pg::kSqlStateSize, '\0');
    pg::UnpackSqlState(sqlstate, sql.errcode);
    duckdb::unordered_map<std::string, std::string> extra_info{
      {"sqlstate", std::move(sqlstate)}};
    if (!sql.errdetail.empty()) {
      extra_info.emplace("detail", sql.errdetail);
    }
    if (!sql.errhint.empty()) {
      extra_info.emplace("hint", sql.errhint);
    }
    return duckdb::ErrorData{
      duckdb::InvalidInputException{extra_info, sql.errmsg}};
  }
}

class PostgresDecoder final : public duckdb::ValueDecoder {
 public:
  PostgresDecoder(duckdb::ClientContext& context,
                  const duckdb::LogicalType& type)
    : _context{context},
      _type{type},
      _decode{
        pg::GetDeserialization<pg::ColumnSink>(type, pg::VarFormat::Text)} {
    pg::FillDeserializeContext(context, _dctx);
  }

  bool Decode(duckdb::Vector& text, duckdb::Vector& out, duckdb::idx_t count,
              duckdb::ErrorData& error) final {
    duckdb::UnifiedVectorFormat format;
    text.ToUnifiedFormat(count, format);
    const auto* values =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(format);
    _fields.resize(count);
    for (duckdb::idx_t row = 0; row < count; ++row) {
      const auto index = format.sel->get_index(row);
      _fields[row] =
        format.validity.RowIsValid(index)
          ? std::string_view{values[index].GetData(), values[index].GetSize()}
          : std::string_view{};
    }
    _failed.assign((count + 63) / 64, 0);
    _dctx.column_error = nullptr;
    if (_decode(_dctx, _fields, out, 0, _failed.data()) == 0) {
      return true;
    }
    bool first = true;
    for (size_t word = 0; word < _failed.size(); ++word) {
      for (auto bits = _failed[word]; bits != 0; bits &= bits - 1) {
        const auto row =
          word * 64 + static_cast<size_t>(std::countr_zero(bits));
        duckdb::FlatVector::SetNull(out, row, true);
        if (std::exchange(first, false)) {
          error = DecodeError(
            _dctx.column_error ? _dctx.column_error
                               : pg::InputError(_context, _type, _fields[row]));
        }
      }
    }
    return false;
  }

 private:
  duckdb::ClientContext& _context;
  duckdb::LogicalType _type;
  pg::DeserializationFunction<pg::ColumnSink> _decode;
  pg::DeserializeContext _dctx;
  std::vector<std::string_view> _fields;
  std::vector<uint64_t> _failed;
};

}  // namespace

duckdb::unique_ptr<duckdb::ValueDecoder> MakePostgresDecoder(
  duckdb::ClientContext& context, const duckdb::LogicalType& type) {
  return duckdb::make_uniq<PostgresDecoder>(context, type);
}

}  // namespace sdb::formats
