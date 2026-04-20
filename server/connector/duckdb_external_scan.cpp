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

#include "connector/duckdb_external_scan.h"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

#include "catalog/format_options.h"
#include "catalog/storage_options.h"
#include "catalog/table.h"

namespace sdb::connector {
namespace {

// Resolves a DuckDB built-in table function by name and picks the overload
// that takes a single VARCHAR argument (the file path).
duckdb::TableFunction LookupSingleStringReader(duckdb::ClientContext& context,
                                               const std::string& name) {
  auto& sys = duckdb::Catalog::GetSystemCatalog(context);
  auto tx = duckdb::CatalogTransaction::GetSystemTransaction(*context.db);
  auto& schema = sys.GetSchema(tx, DEFAULT_SCHEMA);
  auto entry =
    schema.GetEntry(tx, duckdb::CatalogType::TABLE_FUNCTION_ENTRY, name);
  if (!entry) {
    throw duckdb::CatalogException(
      "built-in table function \"%s\" not registered "
      "(is the %s extension linked?)",
      name, name);
  }
  auto& tf_entry = entry->Cast<duckdb::TableFunctionCatalogEntry>();

  for (duckdb::idx_t i = 0; i < tf_entry.functions.Size(); ++i) {
    auto candidate = tf_entry.functions.GetFunctionByOffset(i);
    if (candidate.arguments.size() == 1 &&
        candidate.arguments[0].id() == duckdb::LogicalTypeId::VARCHAR) {
      return candidate;
    }
  }
  throw duckdb::CatalogException(
    "built-in table function \"%s\" has no (VARCHAR) overload", name);
}

std::string PickReaderByPath(std::string_view path) {
  auto dot = path.rfind('.');
  if (dot == std::string::npos) {
    throw duckdb::CatalogException(
      "cannot derive reader from external path (no extension): %s", path);
  }
  auto ext = absl::AsciiStrToLower(path.substr(dot + 1));
  if (ext == "parquet") {
    return "read_parquet";
  }
  if (ext == "csv" || ext == "tsv" || ext == "txt") {
    return "read_csv_auto";
  }
  if (ext == "json" || ext == "jsonl" || ext == "ndjson") {
    return "read_json_auto";
  }
  throw duckdb::CatalogException(
    "unsupported external table extension \".%s\" (supported: parquet, csv, "
    "tsv, txt, json, jsonl, ndjson)",
    ext);
}

duckdb::named_parameter_map_t ToNamedParams(const catalog::FileInfo& fi) {
  duckdb::named_parameter_map_t result;
  if (fi.format_options) {
    for (const auto& [k, v] : fi.format_options->Options()) {
      result.emplace(k, duckdb::Value(v));
    }
  }
  return result;
}

// STRUCT(colname -> type_as_string). Used for both read_csv_auto and
// read_json_auto -- same shape as DuckDB's own ReadCSVRelation builds
// (see third_party/duckdb/src/main/relation/read_csv_relation.cpp:121-127).
duckdb::Value BuildColumnsStruct(const std::vector<catalog::Column>& cols) {
  duckdb::child_list_t<duckdb::Value> fields;
  fields.reserve(cols.size());
  for (const auto& c : cols) {
    fields.emplace_back(c.name, duckdb::Value(c.type.ToString()));
  }
  return duckdb::Value::STRUCT(std::move(fields));
}

// MAP(colname -> STRUCT{name, type, default_value}). Parquet-specific shape
// (parquet_multi_file_info.cpp:VerifyParquetSchemaParameter). The reader
// uses this to override each column's output type, casting physical file
// values to the declared type while preserving filter/row-group pushdown.
duckdb::Value BuildParquetSchema(const std::vector<catalog::Column>& cols) {
  auto struct_t = duckdb::LogicalType::STRUCT({
    {"name", duckdb::LogicalType::VARCHAR},
    {"type", duckdb::LogicalType::VARCHAR},
    {"default_value", duckdb::LogicalType::VARCHAR},
  });
  duckdb::vector<duckdb::Value> keys;
  duckdb::vector<duckdb::Value> values;
  keys.reserve(cols.size());
  values.reserve(cols.size());
  for (const auto& c : cols) {
    keys.emplace_back(c.name);
    values.push_back(duckdb::Value::STRUCT({
      {"name", duckdb::Value(c.name)},
      {"type", duckdb::Value(c.type.ToString())},
      {"default_value", duckdb::Value(duckdb::LogicalType::VARCHAR)},
    }));
  }
  return duckdb::Value::MAP(duckdb::LogicalType::VARCHAR, struct_t,
                            std::move(keys), std::move(values));
}

// Pins the reader's output to the catalog's declared column types. Each
// reader does the coercion in its own code path (parquet casts from the
// physical column, CSV parses text directly as declared type, JSON extracts
// + casts per field). Users supplying the matching named param themselves
// take precedence -- we only fill it in when absent.
void InjectDeclaredTypes(const std::string& reader_name,
                         const std::vector<catalog::Column>& cols,
                         duckdb::named_parameter_map_t& named) {
  if (reader_name == "read_parquet") {
    if (named.find("schema") == named.end()) {
      named["schema"] = BuildParquetSchema(cols);
    }
  } else if (reader_name == "read_csv_auto" ||
             reader_name == "read_json_auto") {
    if (named.find("columns") == named.end()) {
      named["columns"] = BuildColumnsStruct(cols);
    }
  }
}

struct ReaderBind {
  duckdb::unique_ptr<duckdb::FunctionData> bind_data;
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<std::string> names;
};

// Invokes a built-in reader's bind callback with a single VARCHAR argument
// (the file path). The three unused placeholders below satisfy
// TableFunctionBindInput's ctor -- the readers we care about never read
// them.
ReaderBind BindReader(duckdb::ClientContext& context,
                      duckdb::TableFunction& func, std::string path,
                      duckdb::named_parameter_map_t named_params) {
  duckdb::vector<duckdb::Value> inputs{duckdb::Value{std::move(path)}};
  duckdb::vector<duckdb::LogicalType> unused_input_types;
  duckdb::vector<std::string> unused_input_names;
  duckdb::TableFunctionRef unused_ref;
  duckdb::TableFunctionBindInput input(
    inputs, named_params, unused_input_types, unused_input_names,
    func.function_info.get(), nullptr, func, unused_ref);

  ReaderBind out;
  out.bind_data = func.bind(context, input, out.types, out.names);
  return out;
}

}  // namespace

duckdb::TableFunction MakeExternalScanFunction(
  duckdb::ClientContext& context, std::shared_ptr<catalog::Table> sdb_table,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  const auto& fi = sdb_table->GetFileInfo();
  SDB_ENSURE(fi.storage_options, ERROR_INTERNAL);

  // Pick the reader by file extension + invoke its bind. WITH-options flow
  // through as named parameters; remote paths (s3://, https://, ...)
  // authenticate via DuckDB's CREATE SECRET.
  std::string path{fi.storage_options->Path()};
  auto reader_name = PickReaderByPath(path);
  auto func = LookupSingleStringReader(context, reader_name);
  const auto& declared = sdb_table->Columns();

  // First bind without a type override -- in the common case the file's
  // types already match the catalog, and injecting `schema` / `columns`
  // changes how the reader reports projections and filter pushdown in
  // EXPLAIN output. Only re-bind with an override when we actually need a
  // cast.
  auto bound = BindReader(context, func, path, ToNamedParams(fi));

  if (declared.size() != bound.types.size()) {
    throw duckdb::CatalogException(
      "external table declares %llu columns but file has %llu", declared.size(),
      bound.types.size());
  }
  bool needs_override = false;
  for (size_t i = 0; i < declared.size(); ++i) {
    if (declared[i].type != bound.types[i]) {
      needs_override = true;
      break;
    }
  }
  if (needs_override) {
    auto named_params = ToNamedParams(fi);
    InjectDeclaredTypes(reader_name, declared, named_params);
    bound = BindReader(context, func, std::move(path), std::move(named_params));
    for (size_t i = 0; i < declared.size(); ++i) {
      if (declared[i].type != bound.types[i]) {
        throw duckdb::CatalogException(
          "external table column \"%s\" is declared as %s but %s returned %s "
          "even after type override",
          declared[i].name, declared[i].type.ToString(), reader_name,
          bound.types[i].ToString());
      }
    }
  }

  bind_data = std::move(bound.bind_data);
  return func;
}

}  // namespace sdb::connector
