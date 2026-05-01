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
#include <duckdb/execution/operator/csv_scanner/csv_multi_file_info.hpp>

namespace duckdb {

// Forward-decls of the per-format lookup builders. parquet's and json's
// declarations live in extension-internal headers that SereneDB doesn't
// include directly; CSV's lives in csv_multi_file_info.hpp (above).
TableFunction MakeParquetLookupTableFunction();
TableFunction MakeJSONLookupTableFunction();

}  // namespace duckdb

#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include "catalog/format_options.h"
#include "catalog/storage_options.h"
#include "catalog/table.h"
#include "connector/functions/search.h"

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

// --- Delegating callbacks ---
//
// These unwrap the ExternalScanBindData, build a TableFunctionInput /
// TableFunctionInitInput that points at the underlying reader's bind_data,
// and forward to the underlying callback. Any callback on `underlying` that
// consumes FunctionData must be routed through one of these adapters --
// passing our wrapper bind_data to the underlying reader would cause a bad
// cast.

duckdb::BindInfo ExternalScanGetBindInfo(
  duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  auto& data = bind_data->Cast<ExternalScanBindData>();
  if (data.table_entry) {
    return duckdb::BindInfo(*data.table_entry);
  }
  return duckdb::BindInfo(duckdb::ScanType::TABLE);
}

duckdb::TableFunctionInitInput MakeDelegatedInitInput(
  const ExternalScanBindData& bd, duckdb::TableFunctionInitInput& input) {
  return duckdb::TableFunctionInitInput(
    bd.underlying_bind_data.get(), input.column_indexes, input.projection_ids,
    input.filters, input.sample_options, input.op);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ExternalScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bd = input.bind_data->Cast<ExternalScanBindData>();
  auto delegated = MakeDelegatedInitInput(bd, input);
  return bd.underlying.init_global(context, delegated);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> ExternalScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state) {
  auto& bd = input.bind_data->Cast<ExternalScanBindData>();
  auto delegated = MakeDelegatedInitInput(bd, input);
  return bd.underlying.init_local(context, delegated, global_state);
}

void ExternalScanFunction(duckdb::ClientContext& context,
                          duckdb::TableFunctionInput& data,
                          duckdb::DataChunk& output) {
  auto& bd = data.bind_data->Cast<ExternalScanBindData>();
  duckdb::TableFunctionInput delegated(bd.underlying_bind_data.get(),
                                       data.local_state, data.global_state);
  bd.underlying.function(context, delegated, output);
}

duckdb::unique_ptr<duckdb::NodeStatistics> ExternalScanCardinality(
  duckdb::ClientContext& context, const duckdb::FunctionData* bind_data) {
  auto& bd = bind_data->Cast<ExternalScanBindData>();
  if (!bd.underlying.cardinality) {
    return nullptr;
  }
  return bd.underlying.cardinality(context, bd.underlying_bind_data.get());
}

double ExternalScanProgress(duckdb::ClientContext& context,
                            const duckdb::FunctionData* bind_data,
                            const duckdb::GlobalTableFunctionState* gstate) {
  auto& bd = bind_data->Cast<ExternalScanBindData>();
  if (!bd.underlying.table_scan_progress) {
    return -1.0;  // DuckDB's convention for "unknown progress".
  }
  return bd.underlying.table_scan_progress(
    context, bd.underlying_bind_data.get(), gstate);
}

void ExternalScanPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  auto& bd = bind_data->Cast<ExternalScanBindData>();
  if (!bd.underlying.pushdown_complex_filter) {
    return;  // no-op if reader has no complex pushdown.
  }
  bd.underlying.pushdown_complex_filter(context, get,
                                        bd.underlying_bind_data.get(), filters);
}

duckdb::virtual_column_map_t ExternalScanGetVirtualColumns(
  duckdb::ClientContext& context,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  auto& bd = bind_data->Cast<ExternalScanBindData>();
  duckdb::virtual_column_map_t result;
  if (bd.underlying.get_virtual_columns) {
    result =
      bd.underlying.get_virtual_columns(context, bd.underlying_bind_data.get());
  }
  // Merge in the SereneDB-specific virtual columns so references like
  // `idx.tableoid` or `idx.rowid` resolve on an external-table index
  // scan. Without this, bind-time sees these column_ids but
  // init-time/scan-time fails with "Failed to find referenced virtual
  // column" because the underlying parquet/csv reader doesn't know
  // about them.
  if (bd.table_entry) {
    auto sdb_virtuals = bd.table_entry->GetVirtualColumns();
    for (auto& [id, col] : sdb_virtuals) {
      result.insert({id, col});
    }
  }
  return result;
}

duckdb::InsertionOrderPreservingMap<std::string> ExternalScanDynamicToString(
  duckdb::TableFunctionDynamicToStringInput& input) {
  auto& bd = input.bind_data->Cast<ExternalScanBindData>();
  if (!bd.underlying.dynamic_to_string) {
    return {};
  }
  duckdb::TableFunctionDynamicToStringInput delegated(
    bd.underlying, bd.underlying_bind_data.get(), input.local_state,
    input.global_state);
  return bd.underlying.dynamic_to_string(delegated);
}

duckdb::OperatorPartitionData ExternalScanGetPartitionData(
  duckdb::ClientContext& context,
  duckdb::TableFunctionGetPartitionInput& input) {
  auto& bd = input.bind_data->Cast<ExternalScanBindData>();
  SDB_ASSERT(bd.underlying.get_partition_data,
             "get_partition_data called on a reader that doesn't implement it");
  duckdb::TableFunctionGetPartitionInput delegated(
    bd.underlying_bind_data.get(), input.local_state, input.global_state,
    input.partition_info);
  return bd.underlying.get_partition_data(context, delegated);
}

duckdb::TablePartitionInfo ExternalScanGetPartitionInfo(
  duckdb::ClientContext& context, duckdb::TableFunctionPartitionInput& input) {
  auto& bd = input.bind_data->Cast<ExternalScanBindData>();
  if (!bd.underlying.get_partition_info) {
    return {};  // default-constructed TablePartitionInfo =
                // SINGLE_VALUED_PARTITIONS.
  }
  duckdb::TableFunctionPartitionInput delegated(bd.underlying_bind_data.get(),
                                                input.partition_ids);
  return bd.underlying.get_partition_info(context, delegated);
}

// --- Optimizer-facing pushdown / stats adapters ---
//
// These callbacks let DuckDB's optimizer reason about what the underlying
// reader can absorb into the scan (so filters aren't left as separate
// FILTER nodes above us). Without them, even trivial parquet predicates
// like `NOT IN (...)` or `contains(col, 'x')` surface as post-scan
// FILTERs -- losing row-group skipping and the EMPTY_RESULT shortcut
// when stats prove the predicate can't match any row group.

duckdb::unique_ptr<duckdb::BaseStatistics> ExternalScanStatistics(
  duckdb::ClientContext& context, const duckdb::FunctionData* bind_data,
  duckdb::column_t column_index) {
  auto& bd = bind_data->Cast<ExternalScanBindData>();
  return bd.underlying.statistics(context, bd.underlying_bind_data.get(),
                                  column_index);
}

duckdb::unique_ptr<duckdb::BaseStatistics> ExternalScanStatisticsExtended(
  duckdb::ClientContext& context,
  duckdb::TableFunctionGetStatisticsInput& input) {
  auto& bd = input.bind_data->Cast<ExternalScanBindData>();
  duckdb::TableFunctionGetStatisticsInput delegated(
    bd.underlying_bind_data.get(), input.column_index);
  return bd.underlying.statistics_extended(context, delegated);
}

bool ExternalScanSupportsPushdownType(const duckdb::FunctionData& bind_data,
                                      duckdb::idx_t col_idx) {
  auto& bd = bind_data.Cast<ExternalScanBindData>();
  return bd.underlying.supports_pushdown_type(*bd.underlying_bind_data,
                                              col_idx);
}

bool ExternalScanSupportsPushdownExtract(const duckdb::FunctionData& bind_data,
                                         const duckdb::LogicalIndex& col_idx) {
  auto& bd = bind_data.Cast<ExternalScanBindData>();
  return bd.underlying.supports_pushdown_extract(*bd.underlying_bind_data,
                                                 col_idx);
}

void ExternalScanTypePushdown(
  duckdb::ClientContext& context,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data,
  const duckdb::unordered_map<duckdb::idx_t, duckdb::LogicalType>&
    new_column_types) {
  auto& bd = bind_data->Cast<ExternalScanBindData>();
  bd.underlying.type_pushdown(context, bd.underlying_bind_data.get(),
                              new_column_types);
}

// Search-family functions that only the iresearch optimizer knows how to
// rewrite. If we tell DuckDB the underlying parquet scan can absorb
// them, the generic filter-pushdown phase pushes them down before
// iresearch_plan ever sees them, and the phrase/bm25/offsets lose their
// chance to be claimed as a SearchScan.
static bool IsSearchFamilyFunction(std::string_view name) {
  return name == kTSQueryMatch || name == kBm25 || name == kTfidf ||
         name == kRawTf || name == kLmJm || name == kLmDirichlet ||
         name == kIndriDirichlet || name == kDfi || name == kOffsets;
}

static bool ExpressionReferencesSearchFamily(const duckdb::Expression& expr) {
  bool found = false;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      if (found) {
        return;
      }
      if (child.GetExpressionClass() ==
          duckdb::ExpressionClass::BOUND_FUNCTION) {
        auto& fn = child.Cast<duckdb::BoundFunctionExpression>();
        if (IsSearchFamilyFunction(fn.function.name)) {
          found = true;
          return;
        }
      }
      if (ExpressionReferencesSearchFamily(child)) {
        found = true;
      }
    });
  if (!found &&
      expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto& fn = expr.Cast<duckdb::BoundFunctionExpression>();
    if (IsSearchFamilyFunction(fn.function.name)) {
      return true;
    }
  }
  return found;
}

// pushdown_expression receives the LogicalGet (whose bind_data is our
// wrapper). parquet's implementation ignores bind_data and simply
// returns true for anything; CSV/JSON don't set this callback today.
// Before forwarding, reject expressions that the iresearch_plan rule
// needs to claim (PHRASE, BM25, TERM_*, OFFSETS, ...) so they survive
// to that pass instead of being absorbed by the generic parquet
// filter-pushdown first.
duckdb::vector<duckdb::column_t> ExternalScanGetRowIdColumns(
  duckdb::ClientContext& context,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  auto& bd = bind_data->Cast<ExternalScanBindData>();
  return bd.underlying.get_row_id_columns(context,
                                          bd.underlying_bind_data.get());
}

bool ExternalScanPushdownExpression(duckdb::ClientContext& context,
                                    const duckdb::LogicalGet& get,
                                    duckdb::Expression& expr) {
  if (ExpressionReferencesSearchFamily(expr)) {
    return false;
  }
  auto& bd = get.bind_data->Cast<ExternalScanBindData>();
  return bd.underlying.pushdown_expression(context, get, expr);
}

}  // namespace

duckdb::unique_ptr<duckdb::FunctionData> ExternalScanBindData::Copy() const {
  auto result = duckdb::make_uniq<ExternalScanBindData>();
  // Inherited SereneDBScanBindData fields.
  result->table = table;
  result->column_ids = column_ids;
  result->column_types = column_types;
  result->has_rowid = has_rowid;
  result->table_entry = table_entry;
  if (scan_source) {
    result->scan_source = scan_source->Clone();
  }
  // ExternalScanBindData-specific fields.
  result->underlying = underlying;
  result->underlying_bind_data =
    underlying_bind_data ? underlying_bind_data->Copy() : nullptr;
  result->types = types;
  result->names = names;
  return duckdb::unique_ptr_cast<ExternalScanBindData, duckdb::FunctionData>(
    std::move(result));
}

bool ExternalScanBindData::Equals(const duckdb::FunctionData& other) const {
  auto* o = dynamic_cast<const ExternalScanBindData*>(&other);
  if (!o) {
    return false;
  }
  if (table != o->table || column_ids != o->column_ids) {
    return false;
  }
  if (!underlying_bind_data) {
    return !o->underlying_bind_data;
  }
  return o->underlying_bind_data &&
         underlying_bind_data->Equals(*o->underlying_bind_data);
}

duckdb::TableFunction MakeExternalLookupTableFunction(std::string_view path) {
  auto reader_name = PickReaderByPath(path);
  if (reader_name == "read_parquet") {
    return duckdb::MakeParquetLookupTableFunction();
  }
  if (reader_name == "read_csv_auto") {
    return duckdb::MakeCSVLookupTableFunction();
  }
  if (reader_name == "read_json_auto") {
    return duckdb::MakeJSONLookupTableFunction();
  }
  throw duckdb::CatalogException(
    "no lookup TableFunction registered for reader \"%s\"", reader_name);
}

duckdb::TableFunction MakeExternalScanFunction(
  duckdb::ClientContext& context, std::shared_ptr<catalog::Table> sdb_table,
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  const auto& fi = sdb_table->GetFileInfo();
  SDB_ENSURE(fi.storage_options, ERROR_INTERNAL);

  // Pick the reader by file extension + invoke its bind. WITH-options flow
  // through as named parameters; remote paths (s3://, https://, ...)
  // authenticate via DuckDB's CREATE SECRET.
  std::string path{fi.storage_options->Path()};
  auto reader_name = PickReaderByPath(path);
  auto underlying = LookupSingleStringReader(context, reader_name);
  const auto& declared = sdb_table->Columns();

  // First bind without a type override -- in the common case the file's
  // types already match the catalog, and injecting `schema` / `columns`
  // changes how the reader reports projections and filter pushdown in
  // EXPLAIN output. Only re-bind with an override when we actually need a
  // cast.
  auto bound = BindReader(context, underlying, path, ToNamedParams(fi));

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
    bound =
      BindReader(context, underlying, std::move(path), std::move(named_params));
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

  // Populate the reader's virtual columns into its bind_data. DuckDB's
  // normal BaseTableRef binding calls get_virtual_columns after the
  // bind; when we bind directly we must do it ourselves, otherwise
  // the scan throws on any virtual-column column_id (e.g.
  // `file_row_number`).
  if (underlying.get_virtual_columns && bound.bind_data) {
    underlying.get_virtual_columns(context, bound.bind_data.get());
  }

  // Build the SereneDB wrapper. We copy the underlying function so we keep
  // its flags (projection_pushdown, filter_pushdown, ...) but replace every
  // callback that consumes FunctionData with an adapter that swaps in the
  // underlying bind_data. Callbacks we don't implement are nulled out to
  // avoid passing our wrapper bind_data to a reader that would cast it to
  // its own type and crash.
  duckdb::TableFunction wrapper = underlying;
  // Keep the underlying reader's name (`read_parquet`, `read_csv_auto`,
  // ...) so EXPLAIN output is stable across before/after the wrapper
  // (users' mental model of "ah, parquet row-group skipping" carries
  // through).
  wrapper.arguments = {};
  // Pre-bound via this function; the table reference path calls
  // GetScanFunction, it never rebinds.
  wrapper.bind = nullptr;
  wrapper.bind_replace = nullptr;
  wrapper.bind_operator = nullptr;

  wrapper.function = ExternalScanFunction;
  wrapper.init_global = ExternalScanInitGlobal;
  wrapper.init_local = underlying.init_local ? ExternalScanInitLocal : nullptr;

  wrapper.get_bind_info = ExternalScanGetBindInfo;

  wrapper.cardinality =
    underlying.cardinality ? ExternalScanCardinality : nullptr;
  wrapper.table_scan_progress =
    underlying.table_scan_progress ? ExternalScanProgress : nullptr;
  wrapper.pushdown_complex_filter = underlying.pushdown_complex_filter
                                      ? ExternalScanPushdownComplexFilter
                                      : nullptr;
  wrapper.get_virtual_columns =
    underlying.get_virtual_columns ? ExternalScanGetVirtualColumns : nullptr;
  wrapper.dynamic_to_string =
    underlying.dynamic_to_string ? ExternalScanDynamicToString : nullptr;
  wrapper.get_partition_data =
    underlying.get_partition_data ? ExternalScanGetPartitionData : nullptr;
  wrapper.get_partition_info =
    underlying.get_partition_info ? ExternalScanGetPartitionInfo : nullptr;

  // Pushdown / stats callbacks -- delegate when the underlying sets them
  // so parquet row-group skipping, NOT IN / contains-style expression
  // pushdown, and EMPTY_RESULT via stats are preserved through the
  // wrapper. The flags (filter_pushdown, filter_prune, late_materialization)
  // already survive via `wrapper = underlying`.
  wrapper.statistics = underlying.statistics ? ExternalScanStatistics : nullptr;
  wrapper.statistics_extended =
    underlying.statistics_extended ? ExternalScanStatisticsExtended : nullptr;
  wrapper.supports_pushdown_type = underlying.supports_pushdown_type
                                     ? ExternalScanSupportsPushdownType
                                     : nullptr;
  wrapper.supports_pushdown_extract = underlying.supports_pushdown_extract
                                        ? ExternalScanSupportsPushdownExtract
                                        : nullptr;
  wrapper.type_pushdown =
    underlying.type_pushdown ? ExternalScanTypePushdown : nullptr;
  wrapper.pushdown_expression =
    underlying.pushdown_expression ? ExternalScanPushdownExpression : nullptr;
  // Required when late_materialization = true (copied from underlying).
  wrapper.get_row_id_columns =
    underlying.get_row_id_columns ? ExternalScanGetRowIdColumns : nullptr;

  // Remaining callbacks we still don't delegate: none of our tests
  // exercise them, and each would need its own adapter.
  wrapper.to_string = nullptr;
  wrapper.dependency = nullptr;
  wrapper.set_scan_order = nullptr;
  wrapper.get_multi_file_reader = nullptr;
  wrapper.get_partition_stats = nullptr;
  wrapper.rows_scanned = nullptr;
  wrapper.serialize = nullptr;
  wrapper.deserialize = nullptr;
  wrapper.verify_serialization = false;

  auto wrapper_bd = duckdb::make_uniq<ExternalScanBindData>();
  // Populate inherited SereneDBScanBindData fields so the iresearch_plan
  // and rocksdb_plan optimizers can read/mutate this bind_data when they
  // see PHRASE/ANN/equality predicates on an external-table index entry.
  wrapper_bd->table = sdb_table;
  for (const auto& col : sdb_table->Columns()) {
    if (col.id == catalog::Column::kGeneratedPKId) {
      continue;
    }
    wrapper_bd->column_ids.push_back(col.id);
    wrapper_bd->column_types.push_back(col.type);
  }
  wrapper_bd->has_rowid = false;
  wrapper_bd->table_entry = table_entry;
  // scan_source starts as default FullTableScan.

  wrapper_bd->underlying = underlying;
  wrapper_bd->underlying_bind_data = std::move(bound.bind_data);
  wrapper_bd->types = bound.types;
  wrapper_bd->names = bound.names;

  bind_data =
    duckdb::unique_ptr_cast<ExternalScanBindData, duckdb::FunctionData>(
      std::move(wrapper_bd));
  return wrapper;
}

}  // namespace sdb::connector
