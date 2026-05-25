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

#include <absl/strings/numbers.h>
#include <simdjson.h>

#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include "basics/assert.h"

namespace sdb::connector {
namespace {

// ---------------------------------------------------------------------------
// JsonParser -- ported from server/pg/functions/json.tpp
// ---------------------------------------------------------------------------

enum class JsonOutputType { JSON, TEXT };

class JsonParser {
 public:
  void PrepareJson(std::string_view json) {
    _padded_input = simdjson::padded_string{json};
  }

  simdjson::ondemand::document GetJsonDocument() {
    simdjson::ondemand::document doc;
    auto ec = _parser.iterate(_padded_input).get(doc);
    if (ec != simdjson::SUCCESS) {
      throw duckdb::InvalidInputException("Invalid JSON input: %s",
                                          simdjson::error_message(ec));
    }
    if (doc.type().error()) {
      throw duckdb::InvalidInputException("Invalid JSON input: tape error");
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
        throw duckdb::InvalidInputException(
          "Invalid input syntax for type json: %s", sv);
      }
      return duckdb::StringVector::AddString(result, input);
    });
}

// ---------------------------------------------------------------------------
// -> operator (index / field) -- ported from PgJsonExtractIndex/Field
// ---------------------------------------------------------------------------

namespace {

// Replacement for BinaryExecutor::ExecuteWithNulls<TA, TB, TR>(left, right,
// result, count, fn) which was removed upstream. `fn` returns string_t and sets
// the validity bit via the supplied callback when the extracted JSON value is
// missing.
template<typename TA, typename TB, typename FN>
void JsonBinaryExecuteWithNulls(duckdb::Vector& left, duckdb::Vector& right,
                                duckdb::Vector& result, duckdb::idx_t count,
                                FN&& fn) {
  duckdb::UnifiedVectorFormat ldata, rdata;
  left.ToUnifiedFormat(ldata);
  right.ToUnifiedFormat(rdata);
  const auto* lptr = duckdb::UnifiedVectorFormat::GetData<TA>(ldata);
  const auto* rptr = duckdb::UnifiedVectorFormat::GetData<TB>(rdata);
  auto* result_ptr =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto l_idx = ldata.sel->get_index(i);
    auto r_idx = rdata.sel->get_index(i);
    if (!ldata.validity.RowIsValid(l_idx) ||
        !rdata.validity.RowIsValid(r_idx)) {
      result_validity.SetInvalid(i);
      continue;
    }
    bool valid = true;
    result_ptr[i] = fn(lptr[l_idx], rptr[r_idx], valid);
    if (!valid) {
      result_validity.SetInvalid(i);
    }
  }
}

}  // namespace

// json -> int  (returns JSON)
void JsonExtractIndexFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  JsonBinaryExecuteWithNulls<duckdb::string_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t json, int64_t index, bool& valid) -> duckdb::string_t {
      JsonParser parser;
      parser.PrepareJson({json.GetData(), json.GetSize()});
      auto str = parser.ExtractByIndex<JsonOutputType::JSON>(index);
      if (str.empty() && !str.data()) {
        valid = false;
        return duckdb::string_t{};
      }
      return duckdb::StringVector::AddString(result, str.data(), str.size());
    });
}

// json -> text  (returns JSON)
void JsonExtractFieldFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  JsonBinaryExecuteWithNulls<duckdb::string_t, duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t json, duckdb::string_t field,
        bool& valid) -> duckdb::string_t {
      JsonParser parser;
      parser.PrepareJson({json.GetData(), json.GetSize()});
      auto str = parser.ExtractByField<JsonOutputType::JSON>(
        {field.GetData(), field.GetSize()});
      if (str.empty() && !str.data()) {
        valid = false;
        return duckdb::string_t{};
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
  JsonBinaryExecuteWithNulls<duckdb::string_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t json, int64_t index, bool& valid) -> duckdb::string_t {
      JsonParser parser;
      parser.PrepareJson({json.GetData(), json.GetSize()});
      auto str = parser.ExtractByIndex<JsonOutputType::TEXT>(index);
      if (str.empty() && !str.data()) {
        valid = false;
        return duckdb::string_t{};
      }
      return duckdb::StringVector::AddString(result, str.data(), str.size());
    });
}

// json ->> text  (returns text)
void JsonExtractFieldTextFunction(duckdb::DataChunk& args,
                                  duckdb::ExpressionState&,
                                  duckdb::Vector& result) {
  JsonBinaryExecuteWithNulls<duckdb::string_t, duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t json, duckdb::string_t field,
        bool& valid) -> duckdb::string_t {
      JsonParser parser;
      parser.PrepareJson({json.GetData(), json.GetSize()});
      auto str = parser.ExtractByField<JsonOutputType::TEXT>(
        {field.GetData(), field.GetSize()});
      if (str.empty() && !str.data()) {
        valid = false;
        return duckdb::string_t{};
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

  std::vector<duckdb::UnifiedVectorFormat> vdata(ncols);
  for (duckdb::idx_t c = 0; c < ncols; c++) {
    args.data[c].ToUnifiedFormat(count, vdata[c]);
  }

  auto result_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

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
    if (ncols == 2 &&
        args.data[1].GetType().id() == duckdb::LogicalTypeId::LIST) {
      // #> / #>> with text[] argument
      auto list_idx = vdata[1].sel->get_index(row);
      if (!vdata[1].validity.RowIsValid(list_idx)) {
        result_validity.SetInvalid(row);
        continue;
      }
      auto& list_entry =
        duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
          vdata[1])[list_idx];
      auto& child = duckdb::ListVector::GetEntry(args.data[1]);
      duckdb::UnifiedVectorFormat child_data;
      child.ToUnifiedFormat(duckdb::ListVector::GetListSize(args.data[1]),
                            child_data);
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
        throw duckdb::InvalidInputException("invalid JSON");
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
    func.SetVarArgs(VARCHAR);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // json_extract_path_text(json, VARIADIC text) -> text
  {
    duckdb::ScalarFunction func{
      "json_extract_path_text", {JSON}, VARCHAR, JsonExtractPathTextFunction};
    func.SetVarArgs(VARCHAR);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
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
}

}  // namespace sdb::connector
