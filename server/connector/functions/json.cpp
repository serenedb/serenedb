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

// PG-compatible JSON functions for the DuckDB connector.
// Ported from server/pg/functions/json.cpp + json.tpp (Velox path).
// Uses simdjson to preserve original whitespace (PG json semantics).

#include "connector/functions/json.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <simdjson.h>

#include <duckdb/common/optional.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/common.h"
#include "connector/functions/duckdb_aliases.h"

namespace sdb::connector {
namespace {

// ---------------------------------------------------------------------------
// JsonParser -- ported from server/pg/functions/json.tpp
// ---------------------------------------------------------------------------

enum class JsonOutputType {
  JSON,
  TEXT,
};

[[noreturn]] void ThrowInvalidJson(simdjson::error_code error) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                  ERR_MSG("invalid input syntax for type json"),
                  ERR_DETAIL(simdjson::error_message(error)));
}

template<typename T>
T Unwrap(simdjson::simdjson_result<T>&& result) {
  T value;
  if (const auto error = std::move(result).get(value);
      error != simdjson::SUCCESS) {
    ThrowInvalidJson(error);
  }
  return value;
}

class JsonParser {
 public:
  void PrepareJson(std::string_view json) {
    _padded_input = simdjson::padded_string{json};
  }

  simdjson::ondemand::document GetJsonDocument() {
    simdjson::ondemand::document doc;
    auto ec = _parser.iterate(_padded_input).get(doc);
    if (ec != simdjson::SUCCESS) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ERR_MSG("Invalid JSON input: ", simdjson::error_message(ec)));
    }
    if (doc.type().error()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                      ERR_MSG("Invalid JSON input: tape error"));
    }
    return doc;
  }

  static simdjson::simdjson_result<simdjson::ondemand::value> GetByIndex(
    simdjson::ondemand::array arr, int64_t relative_index) {
    size_t size, index;
    auto ec = arr.count_elements().get(size);
    SDB_ASSERT(ec == simdjson::SUCCESS);

    if (relative_index < 0) {
      if (static_cast<size_t>(-relative_index) > size) {
        return simdjson::simdjson_result<simdjson::ondemand::value>(
          simdjson::OUT_OF_BOUNDS);
      }
      index = size + relative_index;
    } else {
      index = static_cast<size_t>(relative_index);
    }
    return arr.at(index);
  }

  template<JsonOutputType Output>
  std::string_view ExtractByIndex(int64_t index) {
    simdjson::ondemand::value value;
    auto doc = GetJsonDocument();
    if (doc.get_value().get(value)) {
      return {};
    }
    if (value.type() != simdjson::ondemand::json_type::array) {
      return {};
    }
    simdjson::ondemand::array arr;
    if (value.get_array().get(arr)) {
      return {};
    }
    if (GetByIndex(arr, index).get(value)) {
      return {};
    }
    return ProcessOutput<Output>(value);
  }

  template<JsonOutputType Output>
  std::string_view ExtractByField(std::string_view field) {
    simdjson::ondemand::value value;
    auto doc = GetJsonDocument();
    if (doc.get_value().get(value)) {
      return {};
    }
    if (value.type() != simdjson::ondemand::json_type::object) {
      return {};
    }
    auto res = value.find_field(field);
    if (res.get(value)) {
      return {};
    }
    return ProcessOutput<Output>(value);
  }

  template<JsonOutputType Output>
  std::string_view ExtractByPath(const std::vector<std::string>& path) {
    simdjson::ondemand::value value;
    auto doc = GetJsonDocument();
    if (doc.get_value().get(value)) {
      return {};
    }
    for (const auto& key : path) {
      if (value.type() == simdjson::ondemand::json_type::array) {
        int64_t index;
        if (!absl::SimpleAtoi(key, &index)) {
          return {};
        }
        simdjson::ondemand::array arr;
        if (value.get_array().get(arr)) {
          return {};
        }
        if (GetByIndex(arr, index).get(value)) {
          return {};
        }
      } else if (value.type() == simdjson::ondemand::json_type::object) {
        auto res = value.find_field(key);
        if (res.get(value)) {
          return {};
        }
      } else {
        return {};
      }
    }
    return ProcessOutput<Output>(value);
  }

 private:
  template<JsonOutputType Output>
  std::string_view ProcessOutput(simdjson::ondemand::value& value) {
    if constexpr (Output == JsonOutputType::TEXT) {
      if (value.type() == simdjson::ondemand::json_type::string) {
        return value.get_string().value();
      }
      if (value.type() == simdjson::ondemand::json_type::null) {
        return {};
      }
    }
    std::string_view str;
    if (simdjson::to_json_string(value).get(str)) {
      return {};
    }
    return str;
  }

  simdjson::ondemand::parser _parser;
  simdjson::padded_string _padded_input;
};

// ---------------------------------------------------------------------------
// Validation (::json cast) -- ported from PgJsonInFunction
// ---------------------------------------------------------------------------

template<typename T>
bool ValidateJson(T& value) {
  switch (value.type()) {
    case simdjson::ondemand::json_type::array: {
      simdjson::ondemand::array arr;
      if (value.get_array().get(arr)) {
        return false;
      }
      for (auto element : arr) {
        simdjson::ondemand::value element_value;
        if (element.get(element_value)) {
          return false;
        }
        if (!ValidateJson(element_value)) {
          return false;
        }
      }
      return true;
    }
    case simdjson::ondemand::json_type::object: {
      simdjson::ondemand::object obj;
      if (value.get_object().get(obj)) {
        return false;
      }
      for (auto field : obj) {
        simdjson::ondemand::value field_value;
        if (field.value().get(field_value)) {
          return false;
        }
        if (!ValidateJson(field_value)) {
          return false;
        }
      }
      return true;
    }
    case simdjson::ondemand::json_type::string:
      return value.get_string().error() == simdjson::SUCCESS;
    case simdjson::ondemand::json_type::number: {
      auto num = value.get_number();
      return num.error() == simdjson::SUCCESS ||
             num.error() == simdjson::BIGINT_ERROR;
    }
    case simdjson::ondemand::json_type::boolean:
      return value.get_bool().error() == simdjson::SUCCESS;
    case simdjson::ondemand::json_type::null:
      return value.is_null();
    default:
      return false;
  }
}

void JsonInFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> duckdb::string_t {
      std::string_view sv{input.GetData(), input.GetSize()};
      simdjson::ondemand::parser parser;
      simdjson::padded_string padded{sv};
      simdjson::ondemand::document doc;
      auto ec = parser.iterate(padded).get(doc);
      if (ec != simdjson::SUCCESS || !ValidateJson(doc)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        ERR_MSG("Invalid input syntax for type json: ", sv));
      }
      return duckdb::StringVector::AddString(result, input);
    });
}

// ---------------------------------------------------------------------------
// -> operator (index / field) -- ported from PgJsonExtractIndex/Field
// ---------------------------------------------------------------------------

// json -> int  (returns JSON)
void JsonExtractIndexFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int64_t, duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t json,
        int64_t index) -> duckdb::optional<duckdb::string_t> {
      JsonParser parser;
      parser.PrepareJson({json.GetData(), json.GetSize()});
      auto str = parser.ExtractByIndex<JsonOutputType::JSON>(index);
      if (str.empty() && !str.data()) {
        return duckdb::nullopt;
      }
      return duckdb::StringVector::AddString(result, str.data(), str.size());
    });
}

// json -> text  (returns JSON)
void JsonExtractFieldFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t json,
        duckdb::string_t field) -> duckdb::optional<duckdb::string_t> {
      JsonParser parser;
      parser.PrepareJson({json.GetData(), json.GetSize()});
      auto str = parser.ExtractByField<JsonOutputType::JSON>(
        {field.GetData(), field.GetSize()});
      if (str.empty() && !str.data()) {
        return duckdb::nullopt;
      }
      return duckdb::StringVector::AddString(result, str.data(), str.size());
    });
}

// ---------------------------------------------------------------------------
// ->> operator (index / field) -- ported from PgJsonExtractIndexText/FieldText
// ---------------------------------------------------------------------------

// json ->> int  (returns text)
void JsonExtractIndexTextFunction(duckdb::DataChunk& args,
                                  duckdb::ExpressionState&,
                                  duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int64_t, duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t json,
        int64_t index) -> duckdb::optional<duckdb::string_t> {
      JsonParser parser;
      parser.PrepareJson({json.GetData(), json.GetSize()});
      auto str = parser.ExtractByIndex<JsonOutputType::TEXT>(index);
      if (str.empty() && !str.data()) {
        return duckdb::nullopt;
      }
      return duckdb::StringVector::AddString(result, str.data(), str.size());
    });
}

// json ->> text  (returns text)
void JsonExtractFieldTextFunction(duckdb::DataChunk& args,
                                  duckdb::ExpressionState&,
                                  duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t json,
        duckdb::string_t field) -> duckdb::optional<duckdb::string_t> {
      JsonParser parser;
      parser.PrepareJson({json.GetData(), json.GetSize()});
      auto str = parser.ExtractByField<JsonOutputType::TEXT>(
        {field.GetData(), field.GetSize()});
      if (str.empty() && !str.data()) {
        return duckdb::nullopt;
      }
      return duckdb::StringVector::AddString(result, str.data(), str.size());
    });
}

// ---------------------------------------------------------------------------
// #> / #>> operators and json_extract_path / json_extract_path_text
// Ported from PgJsonExtractPath/Text
// ---------------------------------------------------------------------------

template<JsonOutputType Output>
void JsonExtractPathImpl(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  auto count = args.size();
  auto ncols = args.ColumnCount();

  auto vdata = args.ToUnifiedFormat();

  auto result_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  const bool list_path =
    ncols == 2 && args.data[1].GetType().id() == duckdb::LogicalTypeId::LIST;
  duckdb::UnifiedVectorFormat child_data;
  if (list_path) {
    duckdb::ListVector::GetEntry(args.data[1])
      .ToUnifiedFormat(duckdb::ListVector::GetListSize(args.data[1]),
                       child_data);
  }

  for (duckdb::idx_t row = 0; row < count; row++) {
    auto json_idx = vdata[0].sel->get_index(row);
    if (!vdata[0].validity.RowIsValid(json_idx)) {
      result_validity.SetInvalid(row);
      continue;
    }
    auto json = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
      vdata[0])[json_idx];

    // For #> / #>> the 2nd arg is text[]; for json_extract_path it's variadic
    // Collect path segments
    std::vector<std::string> path;
    if (list_path) {
      // #> / #>> with text[] argument
      auto list_idx = vdata[1].sel->get_index(row);
      if (!vdata[1].validity.RowIsValid(list_idx)) {
        result_validity.SetInvalid(row);
        continue;
      }
      auto& list_entry =
        duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
          vdata[1])[list_idx];
      for (duckdb::idx_t i = 0; i < list_entry.length; i++) {
        auto child_idx = child_data.sel->get_index(list_entry.offset + i);
        if (!child_data.validity.RowIsValid(child_idx)) {
          result_validity.SetInvalid(row);
          goto next_row;
        }
        auto seg = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
          child_data)[child_idx];
        path.emplace_back(seg.GetData(), seg.GetSize());
      }
    } else {
      // Variadic text arguments (json_extract_path / json_extract_path_text)
      for (duckdb::idx_t c = 1; c < ncols; c++) {
        auto seg_idx = vdata[c].sel->get_index(row);
        if (!vdata[c].validity.RowIsValid(seg_idx)) {
          result_validity.SetInvalid(row);
          goto next_row;
        }
        auto seg = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
          vdata[c])[seg_idx];
        path.emplace_back(seg.GetData(), seg.GetSize());
      }
    }

    {
      JsonParser parser;
      parser.PrepareJson({json.GetData(), json.GetSize()});
      auto str = parser.ExtractByPath<Output>(path);
      if (str.empty() && !str.data()) {
        result_validity.SetInvalid(row);
      } else {
        result_data[row] =
          duckdb::StringVector::AddString(result, str.data(), str.size());
      }
    }
  next_row:;
  }
}

void JsonExtractPathFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  JsonExtractPathImpl<JsonOutputType::JSON>(args, state, result);
}

void JsonExtractPathTextFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  JsonExtractPathImpl<JsonOutputType::TEXT>(args, state, result);
}

// ---------------------------------------------------------------------------
// json_typeof -- ported from PgJsonTypeof
// ---------------------------------------------------------------------------

void JsonTypeofFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> duckdb::string_t {
      std::string_view sv(input.GetData(), input.GetSize());
      simdjson::ondemand::parser parser;
      simdjson::padded_string padded(sv);
      auto doc = parser.iterate(padded);
      auto tp = doc.type();
      const char* name = "null";
      if (!tp.error()) {
        switch (tp.value()) {
          case simdjson::ondemand::json_type::object:
            name = "object";
            break;
          case simdjson::ondemand::json_type::array:
            name = "array";
            break;
          case simdjson::ondemand::json_type::string:
            name = "string";
            break;
          case simdjson::ondemand::json_type::number:
            name = "number";
            break;
          case simdjson::ondemand::json_type::boolean:
            name = "boolean";
            break;
          default:
            break;
        }
      }
      return duckdb::StringVector::AddString(result, name);
    });
}

// ---------------------------------------------------------------------------
// json_strip_nulls -- ported from PgJsonStripNulls
// ---------------------------------------------------------------------------

void WriteValue(simdjson::ondemand::value val, std::string& out);

void WriteObject(simdjson::ondemand::object obj, std::string& out) {
  out += '{';
  bool first = true;
  for (auto field : obj) {
    simdjson::ondemand::value val = field.value();
    if (val.type().value() == simdjson::ondemand::json_type::null) {
      continue;
    }
    if (!first) {
      out += ',';
    }
    first = false;
    out += '"';
    out += field.escaped_key().value();
    out += '"';
    out += ':';
    WriteValue(val, out);
  }
  out += '}';
}

void WriteArray(simdjson::ondemand::array arr, std::string& out) {
  out += '[';
  bool first = true;
  for (simdjson::ondemand::value elem : arr) {
    if (!first) {
      out += ',';
    }
    first = false;
    WriteValue(elem, out);
  }
  out += ']';
}

void WriteValue(simdjson::ondemand::value val, std::string& out) {
  switch (val.type().value()) {
    case simdjson::ondemand::json_type::object:
      WriteObject(val.get_object().value(), out);
      break;
    case simdjson::ondemand::json_type::array:
      WriteArray(val.get_array().value(), out);
      break;
    default:
      out += val.raw_json_token();
      break;
  }
}

void JsonStripNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> duckdb::string_t {
      std::string_view sv(input.GetData(), input.GetSize());
      simdjson::ondemand::parser parser;
      simdjson::padded_string padded(sv);
      simdjson::ondemand::document doc;
      if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        ERR_MSG("invalid JSON"));
      }
      std::string out;
      // Document-level handling
      switch (doc.type().value()) {
        case simdjson::ondemand::json_type::object:
          WriteObject(doc.get_object().value(), out);
          break;
        case simdjson::ondemand::json_type::array:
          WriteArray(doc.get_array().value(), out);
          break;
        default:
          out += doc.raw_json_token().value();
          break;
      }
      return duckdb::StringVector::AddString(result, out);
    });
}

enum class JsonKind : uint8_t {
  Null,
  String,
  Number,
  Boolean,
  Array,
  Object,
};

struct JsonValue {
  JsonKind kind;
  std::string_view text;
};

JsonKind KindOf(simdjson::ondemand::json_type type) {
  switch (type) {
    case simdjson::ondemand::json_type::array:
      return JsonKind::Array;
    case simdjson::ondemand::json_type::object:
      return JsonKind::Object;
    case simdjson::ondemand::json_type::number:
      return JsonKind::Number;
    case simdjson::ondemand::json_type::string:
      return JsonKind::String;
    case simdjson::ondemand::json_type::boolean:
      return JsonKind::Boolean;
    case simdjson::ondemand::json_type::null:
      return JsonKind::Null;
    case simdjson::ondemand::json_type::unknown:
      ThrowInvalidJson(simdjson::TAPE_ERROR);
  }
  SDB_UNREACHABLE();
}

std::string_view CheckText(std::string_view text) {
  if (text.contains('\0')) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNTRANSLATABLE_CHARACTER),
                    ERR_MSG("unsupported Unicode escape sequence"),
                    ERR_DETAIL("\\u0000 cannot be converted to text."));
  }
  return text;
}

JsonValue ReadValue(simdjson::ondemand::value value, bool unescape) {
  const auto kind = KindOf(Unwrap(value.type()));
  if (unescape && kind == JsonKind::String) {
    return {kind, CheckText(Unwrap(value.get_string()))};
  }
  return {kind, absl::StripTrailingAsciiWhitespace(Unwrap(value.raw_json()))};
}

void SetString(duckdb::Vector& vector, duckdb::idx_t row,
               std::string_view text) {
  duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vector)[row] =
    duckdb::StringVector::AddString(vector, text);
}

void SetText(duckdb::Vector& vector, duckdb::idx_t row,
             const JsonValue& value) {
  if (value.kind == JsonKind::Null) {
    duckdb::FlatVector::SetNull(vector, row, true);
  } else {
    SetString(vector, row, value.text);
  }
}

enum class JsonSetFunction : uint8_t {
  ArrayElements,
  ArrayElementsText,
  EachText,
  ObjectKeys,
};

struct JsonItem {
  std::string_view key;
  JsonValue value;
};

template<JsonSetFunction Function>
void ExtractItems(simdjson::ondemand::document& doc,
                  std::vector<JsonItem>& items) {
  const auto kind = KindOf(Unwrap(doc.type()));
  if constexpr (Function == JsonSetFunction::ArrayElements ||
                Function == JsonSetFunction::ArrayElementsText) {
    constexpr std::string_view kName =
      Function == JsonSetFunction::ArrayElements ? "json_array_elements"
                                                 : "json_array_elements_text";
    if (kind == JsonKind::Object) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("cannot call ", kName, " on a non-array"));
    }
    if (kind != JsonKind::Array) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("cannot call ", kName, " on a scalar"));
    }
    for (auto element : Unwrap(doc.get_array())) {
      items.emplace_back(
        std::string_view{},
        ReadValue(Unwrap(std::move(element)),
                  Function == JsonSetFunction::ArrayElementsText));
    }
  } else {
    if (kind == JsonKind::Array) {
      if constexpr (Function == JsonSetFunction::EachText) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("cannot deconstruct an array as an object"));
      } else {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("cannot call json_object_keys on an array"));
      }
    }
    if (kind != JsonKind::Object) {
      if constexpr (Function == JsonSetFunction::EachText) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("cannot deconstruct a scalar"));
      } else {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("cannot call json_object_keys on a scalar"));
      }
    }
    for (auto field_result : Unwrap(doc.get_object())) {
      auto field = Unwrap(std::move(field_result));
      const auto key = CheckText(Unwrap(field.unescaped_key()));
      if constexpr (Function == JsonSetFunction::EachText) {
        items.emplace_back(key, ReadValue(field.value(), true));
      } else {
        items.emplace_back(key, JsonValue{});
      }
    }
  }
}

template<JsonSetFunction Function>
void EmitItem(const JsonItem& item, duckdb::DataChunk& output,
              duckdb::idx_t row) {
  if constexpr (Function == JsonSetFunction::ArrayElements) {
    SetString(output.data[0], row, item.value.text);
  } else if constexpr (Function == JsonSetFunction::ArrayElementsText) {
    SetText(output.data[0], row, item.value);
  } else if constexpr (Function == JsonSetFunction::EachText) {
    SetString(output.data[0], row, item.key);
    SetText(output.data[1], row, item.value);
  } else {
    SetString(output.data[0], row, item.key);
  }
}

template<JsonSetFunction Function>
duckdb::unique_ptr<duckdb::FunctionData> JsonSetBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  if constexpr (Function == JsonSetFunction::ArrayElements) {
    names.emplace_back("value");
    return_types.push_back(duckdb::LogicalType::JSON());
  } else if constexpr (Function == JsonSetFunction::ArrayElementsText) {
    names.emplace_back("value");
    return_types.push_back(duckdb::LogicalType::VARCHAR);
  } else if constexpr (Function == JsonSetFunction::EachText) {
    names.emplace_back("key");
    return_types.push_back(duckdb::LogicalType::VARCHAR);
    names.emplace_back("value");
    return_types.push_back(duckdb::LogicalType::VARCHAR);
  } else {
    names.emplace_back("json_object_keys");
    return_types.push_back(duckdb::LogicalType::VARCHAR);
  }
  return nullptr;
}

struct JsonSetState final : duckdb::LocalTableFunctionState {
  JsonParser parser;
  std::vector<JsonItem> items;
  size_t next_item = 0;
  duckdb::idx_t next_row = 0;
};

duckdb::unique_ptr<duckdb::LocalTableFunctionState> InitJsonSetState(
  duckdb::ExecutionContext&, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState*) {
  return duckdb::make_uniq<JsonSetState>();
}

template<JsonSetFunction Function>
duckdb::OperatorResultType JsonSetFunctionImpl(duckdb::ExecutionContext&,
                                               duckdb::TableFunctionInput& data,
                                               duckdb::DataChunk& input,
                                               duckdb::DataChunk& output) {
  auto& state = data.local_state->Cast<JsonSetState>();
  duckdb::UnifiedVectorFormat json;
  input.data[0].ToUnifiedFormat(json);
  const auto* jsons =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(json);
  duckdb::idx_t count = 0;
  while (count < STANDARD_VECTOR_SIZE) {
    if (state.next_item < state.items.size()) {
      EmitItem<Function>(state.items[state.next_item++], output, count++);
      continue;
    }
    if (state.next_row == input.size()) {
      break;
    }
    const auto row = json.sel->get_index(state.next_row++);
    state.items.clear();
    state.next_item = 0;
    if (!json.validity.RowIsValid(row)) {
      continue;
    }
    state.parser.PrepareJson(AsView(jsons[row]));
    auto doc = state.parser.GetJsonDocument();
    ExtractItems<Function>(doc, state.items);
  }
  output.SetChildCardinality(count);
  if (count == 0) {
    state.next_row = 0;
    return duckdb::OperatorResultType::NEED_MORE_INPUT;
  }
  return duckdb::OperatorResultType::HAVE_MORE_OUTPUT;
}

template<JsonSetFunction Function>
duckdb::TableFunction MakeJsonSetFunction(
  std::string_view name, duckdb::TableFunctionReturnType return_type) {
  duckdb::TableFunction function{duckdb::Identifier{name},
                                 {duckdb::LogicalType::JSON()},
                                 nullptr,
                                 JsonSetBind<Function>,
                                 nullptr,
                                 InitJsonSetState};
  function.in_out_function = JsonSetFunctionImpl<Function>;
  function.return_type = return_type;
  return function;
}

enum class FieldKind : uint8_t {
  Text,
  Json,
  Cast,
  List,
  Struct,
};

FieldKind KindOfField(const duckdb::LogicalType& type) {
  if (type.IsJSONType()) {
    return FieldKind::Json;
  }
  if (type == duckdb::LogicalType::VARCHAR) {
    return FieldKind::Text;
  }
  if (type.id() == duckdb::LogicalTypeId::LIST) {
    return FieldKind::List;
  }
  if (type.id() == duckdb::LogicalTypeId::STRUCT) {
    return FieldKind::Struct;
  }
  return FieldKind::Cast;
}

struct RecordField {
  std::string name;
  duckdb::LogicalType type;
  FieldKind kind;

  bool operator==(const RecordField&) const = default;
};

class RecordLayout {
 public:
  explicit RecordLayout(
    const duckdb::child_list_t<duckdb::LogicalType>& columns) {
    _fields.reserve(columns.size());
    for (const auto& [name, type] : columns) {
      _positions.try_emplace(name.GetIdentifierName(), _fields.size());
      _fields.emplace_back(name.GetIdentifierName(), type, KindOfField(type));
    }
  }

  std::span<const RecordField> Fields() const { return _fields; }

  void Scan(simdjson::ondemand::object object,
            std::span<std::optional<JsonValue>> record) const {
    for (auto field_result : object) {
      auto field = Unwrap(std::move(field_result));
      const auto key = CheckText(Unwrap(field.unescaped_key()));
      if (const auto it = _positions.find(key); it != _positions.end()) {
        record[it->second] = ReadValue(field.value(), true);
      }
    }
  }

  bool operator==(const RecordLayout& other) const {
    return _fields == other._fields;
  }

 private:
  std::vector<RecordField> _fields;
  irs::containers::FlatHashMap<std::string, size_t> _positions;
};

struct JsonRecordBindData final : duckdb::FunctionData {
  explicit JsonRecordBindData(RecordLayout layout)
    : layout{std::move(layout)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<JsonRecordBindData>(layout);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return layout == other.Cast<JsonRecordBindData>().layout;
  }

  RecordLayout layout;
};

[[noreturn]] void ThrowCompositeMismatch(JsonKind kind) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("cannot call populate_composite on ",
                          kind == JsonKind::Array ? "an array" : "a scalar"));
}

[[noreturn]] void ThrowExpectedArray(std::string_view key) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                  ERR_MSG("expected JSON array"),
                  ERR_HINT("See the value of key \"", key, "\"."));
}

struct DeferredField {
  duckdb::idx_t row;
  size_t field;
  duckdb::Value value;
};

class RecordWriter {
 public:
  RecordWriter(duckdb::ClientContext& context, const RecordLayout& layout)
    : _context{context}, _layout{layout} {
    const auto fields = _layout.Fields();
    std::vector<duckdb::LogicalType> types(fields.size(),
                                           duckdb::LogicalType::VARCHAR);
    duckdb::vector<bool> staged;
    for (const auto& field : fields) {
      staged.push_back(field.kind == FieldKind::Cast);
    }
    _staging.Initialize(context, types, staged);
  }

  void Reset() {
    _staging.Reset();
    _deferred.clear();
  }

  void Write(std::span<const std::optional<JsonValue>> record,
             const duckdb::Value& base, std::span<duckdb::Vector> targets,
             duckdb::idx_t row) {
    const auto fields = _layout.Fields();
    for (size_t i = 0; i < fields.size(); ++i) {
      const auto& field = fields[i];
      const auto& value = record[i];
      auto& target =
        field.kind == FieldKind::Cast ? _staging.data[i] : targets[i];
      if (!value || value->kind == JsonKind::Null) {
        duckdb::FlatVector::SetNull(target, row, true);
        if (!value && !base.IsNull()) {
          _deferred.emplace_back(row, i,
                                 duckdb::StructValue::GetChildren(base)[i]);
        }
        continue;
      }
      switch (field.kind) {
        case FieldKind::Text:
        case FieldKind::Cast:
          SetString(target, row, value->text);
          break;
        case FieldKind::Json:
          SetString(target, row, JsonText(*value));
          break;
        case FieldKind::List:
        case FieldKind::Struct:
          target.SetValue(row, FieldValue(*value, field));
          break;
      }
    }
  }

  void Flush(std::span<duckdb::Vector> targets, duckdb::idx_t count) {
    const auto fields = _layout.Fields();
    _staging.SetChildCardinality(count);
    for (size_t i = 0; i < fields.size(); ++i) {
      if (fields[i].kind == FieldKind::Cast) {
        duckdb::VectorOperations::Cast(_context, _staging.data[i], targets[i],
                                       count);
      }
    }
    for (const auto& deferred : _deferred) {
      targets[deferred.field].SetValue(deferred.row, deferred.value);
    }
  }

 private:
  std::string_view JsonText(const JsonValue& value) {
    if (value.kind != JsonKind::String) {
      return value.text;
    }
    _escaped.clear();
    _escaped.escape_and_append_with_quotes(value.text);
    return Unwrap(_escaped.view());
  }

  duckdb::Value TextValue(std::string_view text,
                          const duckdb::LogicalType& type) {
    duckdb::Value value{std::string{text}};
    if (type == duckdb::LogicalType::VARCHAR) {
      return value;
    }
    return value.CastAs(_context, type);
  }

  duckdb::Value FieldValue(const JsonValue& value, const RecordField& field) {
    if (value.kind == JsonKind::String) {
      return TextValue(value.text, field.type);
    }
    if (field.kind == FieldKind::List && value.kind != JsonKind::Array) {
      ThrowExpectedArray(field.name);
    }
    if (field.kind == FieldKind::Struct && value.kind != JsonKind::Object) {
      ThrowCompositeMismatch(value.kind);
    }
    _nested.PrepareJson(value.text);
    auto doc = _nested.GetJsonDocument();
    if (field.kind == FieldKind::List) {
      return ListValue(Unwrap(doc.get_array()), field.type, field.name);
    }
    return StructValue(Unwrap(doc.get_object()), field.type);
  }

  duckdb::Value ListValue(simdjson::ondemand::array array,
                          const duckdb::LogicalType& type,
                          std::string_view key) {
    const auto& child = duckdb::ListType::GetChildType(type);
    duckdb::vector<duckdb::Value> values;
    for (auto element : array) {
      values.push_back(ElementValue(Unwrap(std::move(element)), child, key));
    }
    return duckdb::Value::LIST(child, std::move(values));
  }

  duckdb::Value StructValue(simdjson::ondemand::object object,
                            const duckdb::LogicalType& type) {
    const auto& children = duckdb::StructType::GetChildTypes(type);
    duckdb::vector<duckdb::Value> values;
    for (const auto& child : children) {
      values.emplace_back(child.second);
    }
    for (auto field_result : object) {
      auto field = Unwrap(std::move(field_result));
      const auto key = CheckText(Unwrap(field.unescaped_key()));
      const auto it = absl::c_find_if(children, [&](const auto& child) {
        return child.first.GetIdentifierName() == key;
      });
      if (it != children.end()) {
        values[it - children.begin()] =
          ElementValue(field.value(), it->second, key);
      }
    }
    return duckdb::Value::STRUCT(type, std::move(values));
  }

  duckdb::Value ElementValue(simdjson::ondemand::value element,
                             const duckdb::LogicalType& type,
                             std::string_view key) {
    const auto value = KindOf(Unwrap(element.type()));
    if (value == JsonKind::Null) {
      return duckdb::Value{type};
    }
    if (type.IsJSONType()) {
      return TextValue(JsonText(ReadValue(element, true)), type);
    }
    if (value == JsonKind::String) {
      return TextValue(CheckText(Unwrap(element.get_string())), type);
    }
    if (type.id() == duckdb::LogicalTypeId::LIST) {
      if (value != JsonKind::Array) {
        ThrowExpectedArray(key);
      }
      return ListValue(Unwrap(element.get_array()), type, key);
    }
    if (type.id() == duckdb::LogicalTypeId::STRUCT) {
      if (value != JsonKind::Object) {
        ThrowCompositeMismatch(value);
      }
      return StructValue(Unwrap(element.get_object()), type);
    }
    return TextValue(
      absl::StripTrailingAsciiWhitespace(Unwrap(element.raw_json())), type);
  }

  duckdb::ClientContext& _context;
  const RecordLayout& _layout;
  duckdb::DataChunk _staging;
  JsonParser _nested;
  simdjson::builder::string_builder _escaped;
  std::vector<DeferredField> _deferred;
};

struct JsonRecordState final : duckdb::LocalTableFunctionState {
  JsonRecordState(duckdb::ClientContext& context, const RecordLayout& layout)
    : writer{context, layout}, width{layout.Fields().size()} {}

  std::span<std::optional<JsonValue>> AddRecord() {
    fields.resize(fields.size() + width);
    ++records;
    return std::span{fields}.last(width);
  }

  std::span<const std::optional<JsonValue>> Record(size_t record) const {
    return std::span{fields}.subspan(record * width, width);
  }

  RecordWriter writer;
  JsonParser parser;
  size_t width;
  std::vector<std::optional<JsonValue>> fields;
  duckdb::Value base;
  size_t records = 0;
  size_t next_record = 0;
  duckdb::idx_t next_row = 0;
};

duckdb::unique_ptr<duckdb::LocalTableFunctionState> InitJsonRecordState(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState*) {
  return duckdb::make_uniq<JsonRecordState>(
    context.client, input.bind_data->Cast<JsonRecordBindData>().layout);
}

void ParseRecords(simdjson::ondemand::document& doc, const RecordLayout& layout,
                  JsonRecordState& state) {
  const auto kind = KindOf(Unwrap(doc.type()));
  if (kind == JsonKind::Object) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("cannot call json_populate_recordset on an object"));
  }
  if (kind != JsonKind::Array) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("cannot call json_populate_recordset on a scalar"));
  }
  for (auto element_result : Unwrap(doc.get_array())) {
    auto element = Unwrap(std::move(element_result));
    if (KindOf(Unwrap(element.type())) != JsonKind::Object) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(
          "argument of json_populate_recordset must be an array of objects"));
    }
    layout.Scan(Unwrap(element.get_object()), state.AddRecord());
  }
}

duckdb::OperatorResultType JsonPopulateRecordsetFunction(
  duckdb::ExecutionContext&, duckdb::TableFunctionInput& data,
  duckdb::DataChunk& input, duckdb::DataChunk& output) {
  auto& state = data.local_state->Cast<JsonRecordState>();
  const auto& layout = data.bind_data->Cast<JsonRecordBindData>().layout;
  duckdb::UnifiedVectorFormat json;
  input.data[1].ToUnifiedFormat(json);
  const auto* jsons =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(json);
  state.writer.Reset();
  duckdb::idx_t count = 0;
  while (count < STANDARD_VECTOR_SIZE) {
    if (state.next_record < state.records) {
      state.writer.Write(state.Record(state.next_record++), state.base,
                         output.data, count++);
      continue;
    }
    if (state.next_row == input.size()) {
      break;
    }
    const auto row = state.next_row++;
    state.fields.clear();
    state.records = 0;
    state.next_record = 0;
    state.base = input.data[0].GetValue(row);
    const auto index = json.sel->get_index(row);
    if (!json.validity.RowIsValid(index)) {
      continue;
    }
    state.parser.PrepareJson(AsView(jsons[index]));
    auto doc = state.parser.GetJsonDocument();
    ParseRecords(doc, layout, state);
  }
  if (count == 0) {
    output.SetChildCardinality(0);
    state.next_row = 0;
    return duckdb::OperatorResultType::NEED_MORE_INPUT;
  }
  state.writer.Flush(output.data, count);
  output.SetChildCardinality(count);
  return duckdb::OperatorResultType::HAVE_MORE_OUTPUT;
}

const duckdb::LogicalType& RowType(const duckdb::LogicalType& type,
                                   std::string_view function) {
  if (type.id() != duckdb::LogicalTypeId::STRUCT) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
      ERR_MSG("first argument of ", function, " must be a row type"));
  }
  return type;
}

duckdb::unique_ptr<duckdb::FunctionData> JsonPopulateRecordsetBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  const auto& children = duckdb::StructType::GetChildTypes(
    RowType(input.input_table_types[0], "json_populate_recordset"));
  for (const auto& [name, type] : children) {
    names.push_back(name);
    return_types.push_back(type);
  }
  return duckdb::make_uniq<JsonRecordBindData>(RecordLayout{children});
}

duckdb::TableFunction MakeJsonPopulateRecordsetFunction(
  const duckdb::vector<duckdb::LogicalType>& arguments) {
  duckdb::TableFunction function{duckdb::Identifier{"json_populate_recordset"},
                                 arguments,
                                 nullptr,
                                 JsonPopulateRecordsetBind,
                                 nullptr,
                                 InitJsonRecordState};
  function.in_out_function = JsonPopulateRecordsetFunction;
  function.return_type =
    duckdb::TableFunctionReturnType::TABLE_RETURNING_FUNCTION;
  return function;
}

struct JsonPopulateRecordState final : duckdb::FunctionLocalState {
  JsonPopulateRecordState(duckdb::ClientContext& context,
                          const RecordLayout& layout)
    : writer{context, layout}, record(layout.Fields().size()) {}

  RecordWriter writer;
  JsonParser parser;
  std::vector<std::optional<JsonValue>> record;
};

duckdb::unique_ptr<duckdb::FunctionData> JsonPopulateRecordBind(
  duckdb::BindScalarFunctionInput& input) {
  const auto& type =
    RowType(input.GetArguments()[0]->GetReturnType(), "json_populate_record");
  input.GetBoundFunction().SetReturnType(type);
  return duckdb::make_uniq<JsonRecordBindData>(
    RecordLayout{duckdb::StructType::GetChildTypes(type)});
}

duckdb::unique_ptr<duckdb::FunctionLocalState> InitJsonPopulateRecordState(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<JsonPopulateRecordState>(
    state.GetContext(), bind_data->Cast<JsonRecordBindData>().layout);
}

void JsonPopulateRecordFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& local = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<JsonPopulateRecordState>();
  const auto& layout = state.expr.Cast<duckdb::BoundFunctionExpression>()
                         .BindInfo()
                         ->Cast<JsonRecordBindData>()
                         .layout;
  duckdb::UnifiedVectorFormat base;
  args.data[0].ToUnifiedFormat(base);
  duckdb::UnifiedVectorFormat json;
  args.data[1].ToUnifiedFormat(json);
  const auto* jsons =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(json);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto& targets = duckdb::StructVector::GetEntries(result);
  const auto count = args.size();
  local.writer.Reset();
  for (duckdb::idx_t row = 0; row < count; ++row) {
    absl::c_fill(local.record, std::nullopt);
    const auto record_base = base.validity.RowIsValid(base.sel->get_index(row))
                               ? args.data[0].GetValue(row)
                               : duckdb::Value{};
    const auto index = json.sel->get_index(row);
    const bool has_json = json.validity.RowIsValid(index);
    if (has_json) {
      local.parser.PrepareJson(AsView(jsons[index]));
      auto doc = local.parser.GetJsonDocument();
      const auto kind = KindOf(Unwrap(doc.type()));
      if (kind != JsonKind::Object) {
        ThrowCompositeMismatch(kind);
      }
      layout.Scan(Unwrap(doc.get_object()), local.record);
    }
    local.writer.Write(local.record, record_base, targets, row);
    if (!has_json && record_base.IsNull()) {
      duckdb::FlatVector::SetNull(result, row, true);
    }
  }
  local.writer.Flush(targets, count);
}

template<typename Members>
std::string_view WriteJsonObject(simdjson::builder::string_builder& out,
                                 duckdb::idx_t count, Members members) {
  out.clear();
  out.append('{');
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto [key, value] = members(i);
    if (!key.IsValid()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                      ERR_MSG("null value not allowed for object key"));
    }
    if (i != 0) {
      out.append_raw(", ");
    }
    out.escape_and_append_with_quotes(AsView(key.GetValue()));
    out.append_raw(" : ");
    if (value.IsValid()) {
      out.escape_and_append_with_quotes(AsView(value.GetValue()));
    } else {
      out.append_null();
    }
  }
  out.append('}');
  return Unwrap(out.view());
}

void WriteJsonObjectValue(duckdb::VectorWriter<duckdb::string_t>& writer,
                          std::string_view object) {
  writer.WriteValue(
    duckdb::string_t{object.data(), static_cast<uint32_t>(object.size())});
}

void JsonObjectFromPairsFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState&,
                                 duckdb::Vector& result) {
  const auto lists =
    args.data[0].Values<duckdb::VectorListType<duckdb::string_t>>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  simdjson::builder::string_builder out;
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto list = lists[row];
    if (!list.IsValid()) {
      writer.WriteNull();
      continue;
    }
    if (list.GetListLength() % 2 != 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                      ERR_MSG("array must have even number of elements"));
    }
    WriteJsonObjectValue(
      writer, WriteJsonObject(out, list.GetListLength() / 2, [&](auto i) {
        return std::pair{list.GetChildValue(2 * i),
                         list.GetChildValue(2 * i + 1)};
      }));
  }
}

void JsonObjectFromRowsFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState&,
                                duckdb::Vector& result) {
  const auto lists =
    args.data[0]
      .Values<
        duckdb::VectorListType<duckdb::VectorListType<duckdb::string_t>>>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  simdjson::builder::string_builder out;
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto list = lists[row];
    if (!list.IsValid()) {
      writer.WriteNull();
      continue;
    }
    for (const auto pair : list.GetChildValues()) {
      if (!pair.IsValid() || pair.GetListLength() != 2) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                        ERR_MSG("array must have two columns"));
      }
    }
    WriteJsonObjectValue(
      writer, WriteJsonObject(out, list.GetListLength(), [&](auto i) {
        const auto pair = list.GetChildValue(i);
        return std::pair{pair.GetChildValue(0), pair.GetChildValue(1)};
      }));
  }
}

duckdb::unique_ptr<duckdb::FunctionData> JsonObjectFromTextBind(
  duckdb::BindScalarFunctionInput& input) {
  const auto text = input.TryGetConstant(0);
  bool rows = false;
  if (text) {
    const auto value = text->DefaultCastAs(duckdb::LogicalType::VARCHAR);
    const auto literal =
      absl::StripLeadingAsciiWhitespace(duckdb::StringValue::Get(value));
    rows =
      literal.starts_with('{') &&
      absl::StripLeadingAsciiWhitespace(literal.substr(1)).starts_with('{');
  }
  const auto texts = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  auto& function = input.GetBoundFunction();
  function.GetArguments()[0] = rows ? duckdb::LogicalType::LIST(texts) : texts;
  function.SetFunctionCallback(rows ? JsonObjectFromRowsFunction
                                    : JsonObjectFromPairsFunction);
  return nullptr;
}

void JsonObjectFromKeysFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState&,
                                duckdb::Vector& result) {
  const auto keys =
    args.data[0].Values<duckdb::VectorListType<duckdb::string_t>>();
  const auto values =
    args.data[1].Values<duckdb::VectorListType<duckdb::string_t>>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  simdjson::builder::string_builder out;
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto key_list = keys[row];
    const auto value_list = values[row];
    if (!key_list.IsValid() || !value_list.IsValid()) {
      writer.WriteNull();
      continue;
    }
    if (key_list.GetListLength() != value_list.GetListLength()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                      ERR_MSG("mismatched array dimensions"));
    }
    WriteJsonObjectValue(
      writer, WriteJsonObject(out, key_list.GetListLength(), [&](auto i) {
        return std::pair{key_list.GetChildValue(i),
                         value_list.GetChildValue(i)};
      }));
  }
}

}  // namespace

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

void RegisterPgJsonFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  auto JSON = duckdb::LogicalType::JSON();
  auto VARCHAR = duckdb::LogicalType::VARCHAR;
  auto BIGINT = duckdb::LogicalType::BIGINT;

  // json -> int  (json_extract_index)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "json_extract_index", {JSON, BIGINT}, JSON, JsonExtractIndexFunction});

  // json -> text  (json_extract_field)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "json_extract_field", {JSON, VARCHAR}, JSON, JsonExtractFieldFunction});

  // json ->> int  (json_extract_index_text)
  loader.RegisterFunction(duckdb::ScalarFunction{"json_extract_index_text",
                                                 {JSON, BIGINT},
                                                 VARCHAR,
                                                 JsonExtractIndexTextFunction});

  // json ->> text  (json_extract_field_text)
  loader.RegisterFunction(duckdb::ScalarFunction{"json_extract_field_text",
                                                 {JSON, VARCHAR},
                                                 VARCHAR,
                                                 JsonExtractFieldTextFunction});

  // json_extract_path(json, VARIADIC text) -> json
  {
    duckdb::ScalarFunction func{
      "json_extract_path", {JSON}, JSON, JsonExtractPathFunction};
    func.GetSignature().AddArgs("args", VARCHAR).AddKwargs("kwargs", VARCHAR);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    duckdb::CreateScalarFunctionInfo info{std::move(func)};
    info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }

  // json_extract_path_text(json, VARIADIC text) -> text
  {
    duckdb::ScalarFunction func{
      "json_extract_path_text", {JSON}, VARCHAR, JsonExtractPathTextFunction};
    func.GetSignature().AddArgs("args", VARCHAR).AddKwargs("kwargs", VARCHAR);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    duckdb::CreateScalarFunctionInfo info{std::move(func)};
    info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }

  // Internal names for #> / #>> operators (takes json, text[])
  {
    duckdb::ScalarFunction func{"pg_json_extract_path",
                                {JSON, duckdb::LogicalType::LIST(VARCHAR)},
                                JSON,
                                JsonExtractPathFunction};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{"pg_json_extract_path_text",
                                {JSON, duckdb::LogicalType::LIST(VARCHAR)},
                                VARCHAR,
                                JsonExtractPathTextFunction};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // jsonin -- validation cast
  loader.RegisterFunction(
    duckdb::ScalarFunction{"jsonin", {VARCHAR}, JSON, JsonInFunction});

  // json_typeof
  loader.RegisterFunction(
    duckdb::ScalarFunction{"json_typeof", {JSON}, VARCHAR, JsonTypeofFunction});

  // json_array_length -- not registered, DuckDB's json extension provides it

  // json_strip_nulls
  loader.RegisterFunction(duckdb::ScalarFunction{
    "json_strip_nulls", {JSON}, JSON, JsonStripNullsFunction});

  // Override DuckDB's json extension functions so whitespace is preserved.
  // json_extract (-> operator) -- DuckDB's version re-serializes JSON
  {
    duckdb::ScalarFunctionSet extract_set("json_extract");
    {
      duckdb::ScalarFunction f{
        "json_extract", {JSON, BIGINT}, JSON, JsonExtractIndexFunction};
      f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      extract_set.AddFunction(f);
    }
    {
      duckdb::ScalarFunction f{
        "json_extract", {JSON, VARCHAR}, JSON, JsonExtractFieldFunction};
      f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      extract_set.AddFunction(f);
    }
    loader.RegisterFunction(extract_set);
  }
  // ->> operator (DuckDB registers as "json_extract_string" and "->>")
  {
    duckdb::ScalarFunctionSet arrow_text_set("->>");
    {
      duckdb::ScalarFunction f{
        "->>", {JSON, BIGINT}, VARCHAR, JsonExtractIndexTextFunction};
      f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      arrow_text_set.AddFunction(f);
    }
    {
      duckdb::ScalarFunction f{
        "->>", {JSON, VARCHAR}, VARCHAR, JsonExtractFieldTextFunction};
      f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      arrow_text_set.AddFunction(f);
    }
    loader.RegisterFunction(arrow_text_set);
  }
  {
    duckdb::ScalarFunctionSet extract_string_set("json_extract_string");
    {
      duckdb::ScalarFunction f{"json_extract_string",
                               {JSON, BIGINT},
                               VARCHAR,
                               JsonExtractIndexTextFunction};
      f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      extract_string_set.AddFunction(f);
    }
    {
      duckdb::ScalarFunction f{"json_extract_string",
                               {JSON, VARCHAR},
                               VARCHAR,
                               JsonExtractFieldTextFunction};
      f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      extract_string_set.AddFunction(f);
    }
    loader.RegisterFunction(extract_string_set);
  }

  using enum duckdb::TableFunctionReturnType;
  loader.RegisterFunction(MakeJsonSetFunction<JsonSetFunction::ArrayElements>(
    "json_array_elements", TABLE_RETURNING_FUNCTION));
  loader.RegisterFunction(
    MakeJsonSetFunction<JsonSetFunction::ArrayElementsText>(
      "json_array_elements_text", TABLE_RETURNING_FUNCTION));
  loader.RegisterFunction(MakeJsonSetFunction<JsonSetFunction::EachText>(
    "json_each_text", TABLE_RETURNING_FUNCTION));
  loader.RegisterFunction(MakeJsonSetFunction<JsonSetFunction::ObjectKeys>(
    "json_object_keys", SET_RETURNING_FUNCTION));

  {
    duckdb::TableFunctionSet set{"json_populate_recordset"};
    for (const auto& arguments :
         {duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::ANY, JSON},
          duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::ANY, JSON,
                                              duckdb::LogicalType::BOOLEAN}}) {
      set.AddFunction(MakeJsonPopulateRecordsetFunction(arguments));
    }
    loader.RegisterFunction(std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"json_populate_record"};
    for (const auto& arguments :
         {duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::ANY, JSON},
          duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::ANY, JSON,
                                              duckdb::LogicalType::BOOLEAN}}) {
      duckdb::ScalarFunction function{
        "json_populate_record",     arguments,
        duckdb::LogicalType::ANY,   JsonPopulateRecordFunction,
        JsonPopulateRecordBind,     nullptr,
        InitJsonPopulateRecordState};
      function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      function.SetStability(duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
      function.GetSignature().SetRecordResult();
      set.AddFunction(std::move(function));
    }
    set.SetFallible();
    loader.RegisterFunction(std::move(set));
  }

  RegisterScalarAlias(loader, "json_array", "json_build_array");
  RegisterScalarAlias(loader, "json_object", "json_build_object");

  const auto texts = duckdb::LogicalType::LIST(VARCHAR);
  for (auto function : {
         duckdb::ScalarFunction{
           "json_object", {texts}, JSON, JsonObjectFromPairsFunction},
         duckdb::ScalarFunction{"json_object",
                                {duckdb::LogicalType::LIST(texts)},
                                JSON,
                                JsonObjectFromRowsFunction},
         duckdb::ScalarFunction{
           "json_object", {texts, texts}, JSON, JsonObjectFromKeysFunction},
         duckdb::ScalarFunction{"json_object",
                                {VARCHAR},
                                JSON,
                                JsonObjectFromPairsFunction,
                                JsonObjectFromTextBind},
       }) {
    function.SetFallible();
    loader.AddFunctionOverload(std::move(function));
  }
}

}  // namespace sdb::connector
