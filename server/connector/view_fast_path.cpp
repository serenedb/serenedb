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

#include "connector/view_fast_path.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/comparison_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/star_expression.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/result_modifier.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/planner/tableref/bound_at_clause.hpp>

#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/view.h"
#include "connector/duckdb_table_entry.h"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "planning/iceberg_multi_file_list.hpp"

namespace duckdb {

TableFunction MakeParquetLookupTableFunction();
TableFunction MakeCSVLookupTableFunction();
TableFunction MakeJSONLookupTableFunction();
TableFunction MakeJSONObjectsLookupTableFunction();
TableFunction MakeTextLookupTableFunction();
TableFunction MakeDuckDBLookupTableFunction();

}  // namespace duckdb
namespace sdb::connector {
namespace {

struct RegistryEntry {
  std::string_view function_name;
  catalog::PkSpec single_pk_spec;
  catalog::PkSpec glob_pk_spec;
  duckdb::TableFunction (*make_lookup)();
};

const RegistryEntry kRegistry[] = {
  {
    .function_name = "read_parquet",
    .single_pk_spec = catalog::PkSpec::FileRowNumber,
    .glob_pk_spec = catalog::PkSpec::FileIndexPlusRowNumber,
    .make_lookup = duckdb::MakeParquetLookupTableFunction,
  },
  {
    .function_name = "read_csv",
    .single_pk_spec = catalog::PkSpec::FileOffset,
    .glob_pk_spec = catalog::PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeCSVLookupTableFunction,
  },
  {
    .function_name = "read_json",
    .single_pk_spec = catalog::PkSpec::FileOffset,
    .glob_pk_spec = catalog::PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeJSONLookupTableFunction,
  },
  {
    .function_name = "read_ndjson",
    .single_pk_spec = catalog::PkSpec::FileOffset,
    .glob_pk_spec = catalog::PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeJSONLookupTableFunction,
  },
  {
    .function_name = "read_json_objects",
    .single_pk_spec = catalog::PkSpec::FileOffset,
    .glob_pk_spec = catalog::PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeJSONObjectsLookupTableFunction,
  },
  {
    .function_name = "read_ndjson_objects",
    .single_pk_spec = catalog::PkSpec::FileOffset,
    .glob_pk_spec = catalog::PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeJSONObjectsLookupTableFunction,
  },
  // Iceberg data files are parquet; reuse the parquet lookup TF.
  {
    .function_name = "iceberg_scan",
    .single_pk_spec = catalog::PkSpec::FileIndexPlusRowNumber,
    .glob_pk_spec = catalog::PkSpec::FileIndexPlusRowNumber,
    .make_lookup = duckdb::MakeParquetLookupTableFunction,
  },
  // read_text emits one row per file; PK is (file_index, 0) in glob mode.
  {
    .function_name = "read_text",
    .single_pk_spec = catalog::PkSpec::FileRowNumber,
    .glob_pk_spec = catalog::PkSpec::FileIndexPlusRowNumber,
    .make_lookup = duckdb::MakeTextLookupTableFunction,
  },
  {
    .function_name = "read_duckdb",
    .single_pk_spec = catalog::PkSpec::DuckDBRowId,
    .glob_pk_spec = catalog::PkSpec::FileIndexPlusDuckDBRowId,
    .make_lookup = duckdb::MakeDuckDBLookupTableFunction,
  },
  // TODO: read_avro, postgres_scan / postgres_query.
};

constexpr struct {
  std::string_view alias;
  std::string_view canonical;
} kFunctionAliases[] = {
  {"parquet_scan", "read_parquet"},
  {"read_csv_auto", "read_csv"},
  {"read_json_auto", "read_json"},
  {"read_json_objects_auto", "read_json_objects"},
  {"read_ndjson_auto", "read_ndjson"},
};

std::string_view ResolveAlias(std::string_view name) noexcept {
  for (const auto& [alias, canonical] : kFunctionAliases) {
    if (alias == name) {
      return canonical;
    }
  }
  return name;
}

const RegistryEntry* LookupRegistry(std::string_view function_name) {
  for (const auto& e : kRegistry) {
    if (e.function_name == function_name) {
      return &e;
    }
  }
  return nullptr;
}

bool LooksLikeGlob(std::string_view path) noexcept {
  return path.find('*') != std::string_view::npos ||
         path.find('?') != std::string_view::npos;
}

duckdb::TableFunction LookupSingleStringReader(duckdb::ClientContext& context,
                                               std::string_view name) {
  auto& sys = duckdb::Catalog::GetSystemCatalog(context);
  auto tx = duckdb::CatalogTransaction::GetSystemTransaction(*context.db);
  auto& schema = sys.GetSchema(tx, DEFAULT_SCHEMA);
  auto entry = schema.GetEntry(tx, duckdb::CatalogType::TABLE_FUNCTION_ENTRY,
                               duckdb::Identifier{name});
  if (!entry) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("fast-path source function \"", name, "\" not registered"));
  }
  auto& tf_entry = entry->Cast<duckdb::TableFunctionCatalogEntry>();
  for (duckdb::idx_t i = 0; i < tf_entry.functions.Size(); ++i) {
    auto candidate = tf_entry.functions.GetFunctionByOffset(i);
    if (candidate.arguments.size() == 1 &&
        candidate.arguments[0].id() == duckdb::LogicalTypeId::VARCHAR) {
      return candidate;
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("fast-path source function \"", name,
                          "\" has no (VARCHAR) overload"));
}

}  // namespace

std::optional<ViewFastPath> ResolveViewFastPath(
  duckdb::ClientContext& context, const catalog::PgSqlView& view) {
  const auto& info = view.GetInfo();
  if (!info.query) {
    return std::nullopt;
  }
  if (info.query->node->type != duckdb::QueryNodeType::SELECT_NODE) {
    return std::nullopt;
  }
  const auto& select_node = info.query->node->Cast<duckdb::SelectNode>();
  if (select_node.having || select_node.qualify || select_node.sample ||
      !select_node.groups.group_expressions.empty() ||
      !select_node.cte_map.map.empty()) {
    return std::nullopt;
  }
  // DISTINCT would collapse base rows -- we'd lose dedupe at materialisation.
  for (const auto& mod : select_node.modifiers) {
    switch (mod->type) {
      case duckdb::ResultModifierType::ORDER_MODIFIER:
      case duckdb::ResultModifierType::LIMIT_MODIFIER:
      case duckdb::ResultModifierType::LEGACY_LIMIT_PERCENT_MODIFIER:
        break;
      default:
        return std::nullopt;
    }
  }
  std::vector<std::string> projection_columns;
  if (select_node.select_list.empty()) {
    return std::nullopt;
  }
  if (select_node.select_list.size() == 1 &&
      select_node.select_list[0]->GetExpressionClass() ==
        duckdb::ExpressionClass::STAR) {
  } else {
    for (const auto& item : select_node.select_list) {
      const duckdb::ParsedExpression* cur = item.get();
      while (cur->GetExpressionClass() == duckdb::ExpressionClass::CAST) {
        cur = &cur->Cast<duckdb::CastExpression>().Child();
      }
      if (cur->GetExpressionClass() != duckdb::ExpressionClass::COLUMN_REF) {
        return std::nullopt;
      }
      const auto& colref = cur->Cast<duckdb::ColumnRefExpression>();
      if (colref.IsQualified()) {
        return std::nullopt;
      }
      projection_columns.emplace_back(
        colref.GetColumnName().GetIdentifierName());
    }
  }
  if (!select_node.from_table) {
    return std::nullopt;
  }
  if (select_node.from_table->type == duckdb::TableReferenceType::BASE_TABLE) {
    const auto& base_ref = select_node.from_table->Cast<duckdb::BaseTableRef>();
    const auto& qname = base_ref.GetQualifiedName();
    duckdb::EntryLookupInfo entry_lookup(duckdb::CatalogType::TABLE_ENTRY,
                                         duckdb::QualifiedName(qname.Name()));
    auto generic = duckdb::Catalog::GetEntry(
      context, qname.Catalog(), qname.Schema(), entry_lookup,
      duckdb::OnEntryNotFound::RETURN_NULL);
    if (!generic) {
      return std::nullopt;
    }
    if (generic->type != duckdb::CatalogType::TABLE_ENTRY) {
      return std::nullopt;
    }
    auto& entry = generic->Cast<duckdb::TableCatalogEntry>();
    const auto cat_type = entry.ParentCatalog().GetCatalogType();
    if (cat_type == "iceberg") {
      const auto* registry_entry = LookupRegistry("iceberg_scan");
      if (!registry_entry) {
        return std::nullopt;
      }
      ViewFastPath out;
      out.function_name = std::string{registry_entry->function_name};
      out.catalog_ref = CatalogTableRef{
        .catalog = entry.ParentCatalog().GetName().GetIdentifierName(),
        .schema = entry.ParentSchema().name.GetIdentifierName(),
        .table = entry.name.GetIdentifierName()};
      out.is_glob = true;
      out.projection_columns = std::move(projection_columns);
      out.pk_spec = registry_entry->glob_pk_spec;
      return out;
    }
    if (cat_type == "duckdb") {
      auto& src_catalog = entry.ParentCatalog();
      if (src_catalog.IsSystemCatalog() || src_catalog.IsTemporaryCatalog() ||
          src_catalog.InMemory()) {
        return std::nullopt;
      }
      ViewFastPath out;
      out.function_name = "read_duckdb";
      out.catalog_ref =
        CatalogTableRef{.catalog = src_catalog.GetName().GetIdentifierName(),
                        .schema = entry.ParentSchema().name.GetIdentifierName(),
                        .table = entry.name.GetIdentifierName()};
      out.projection_columns = std::move(projection_columns);
      out.pk_spec = catalog::PkSpec::DuckDBRowId;
      return out;
    }
    if (cat_type == "serenedb") {
      const auto* sdb_entry = dynamic_cast<const SereneDBTableEntry*>(&entry);
      if (!sdb_entry) {
        return std::nullopt;
      }
      auto sdb_table = sdb_entry->GetSereneDBTable();
      if (!sdb_table) {
        return std::nullopt;
      }
      // The table's rows live in the hidden store table; views over it ride
      // the same rowid-keyed machinery as views over attached databases.
      ViewFastPath out;
      out.catalog_ref =
        CatalogTableRef{.catalog = std::string{catalog::kStoreDatabaseName},
                        .schema = "main",
                        .table = catalog::StoreTableName(
                          entry.ParentCatalog().GetName().GetIdentifierName(),
                          entry.ParentSchema().name.GetIdentifierName(),
                          entry.name.GetIdentifierName())};
      out.pk_spec = catalog::PkSpec::DuckDBRowId;
      out.projection_columns = std::move(projection_columns);
      return out;
    }
    if (cat_type == "clickhouse" || cat_type == "postgres") {
      // Attached external DB whose connector records its PK as a standard
      // primary-key constraint on the table entry (postgres_scanner from
      // pg_constraint; clickhouse from system.columns.is_in_primary_key). Take
      // the PK from that metadata -- read via the engine-agnostic catalog API,
      // so this body is identical for both. v1: a single 64-bit integer column;
      // anything else -> no fast path -> materialisation stays unsupported.
      // NB: a postgres PRIMARY KEY is a true uniqueness guarantee; clickhouse's
      // is only the MergeTree sorting prefix (correctness assumes uniqueness).
      std::optional<std::string> pk_name;
      for (const auto& constraint : entry.GetConstraints()) {
        if (constraint->type != duckdb::ConstraintType::UNIQUE) {
          continue;
        }
        const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
        if (!unique.IsPrimaryKey()) {
          continue;
        }
        if (unique.GetColumnNames().size() != 1) {
          return std::nullopt;
        }
        pk_name = unique.GetColumnNames()[0];
      }
      if (!pk_name) {
        return std::nullopt;
      }
      std::optional<duckdb::column_t> pk_index;
      duckdb::idx_t pos = 0;
      for (const auto& col : entry.GetColumns().Logical()) {
        if (col.Name() == *pk_name) {
          // v1: a signed 64-bit key. The index keys postings via the int64
          // primary-key encoding and the materialiser renders the value back
          // into a `WHERE pk IN (...)` list -- both exact only for BIGINT.
          if (col.GetType().id() != duckdb::LogicalTypeId::BIGINT) {
            return std::nullopt;
          }
          pk_index = static_cast<duckdb::column_t>(pos);
          break;
        }
        ++pos;
      }
      if (!pk_index) {
        return std::nullopt;
      }
      ViewFastPath out;
      out.catalog_ref =
        CatalogTableRef{.catalog = entry.ParentCatalog().GetName(),
                        .schema = entry.ParentSchema().name,
                        .table = entry.name};
      out.pk_spec = catalog::PkSpec::ExternalDBKey;
      out.pk_column_index = *pk_index;
      out.pk_column_name = std::move(*pk_name);
      out.pk_uniqueness = cat_type == "postgres" ? PkUniqueness::Enforced
                                                 : PkUniqueness::Unverified;
      out.projection_columns = std::move(projection_columns);
      return out;
    }
    return std::nullopt;
  }
  if (select_node.from_table->type !=
      duckdb::TableReferenceType::TABLE_FUNCTION) {
    return std::nullopt;
  }
  const auto& tf_ref = select_node.from_table->Cast<duckdb::TableFunctionRef>();
  if (!tf_ref.function || tf_ref.function->GetExpressionType() !=
                            duckdb::ExpressionType::FUNCTION) {
    return std::nullopt;
  }
  const auto& fn_expr = tf_ref.function->Cast<duckdb::FunctionExpression>();
  duckdb::vector<duckdb::Value> args;
  duckdb::named_parameter_map_t named_params;
  args.reserve(fn_expr.GetArguments().size());
  // CAST targets are unbound here; coercion happens in BindFastPathSource.
  auto peel_cast =
    [](this auto& self,
       const duckdb::ParsedExpression& expr) -> std::optional<duckdb::Value> {
    const duckdb::ParsedExpression* cur = &expr;
    while (cur->GetExpressionClass() == duckdb::ExpressionClass::CAST) {
      cur = &cur->Cast<duckdb::CastExpression>().Child();
    }
    if (cur->GetExpressionClass() == duckdb::ExpressionClass::CONSTANT) {
      return cur->Cast<duckdb::ConstantExpression>().GetValue();
    }
    if (cur->GetExpressionType() == duckdb::ExpressionType::FUNCTION) {
      const auto& fn = cur->Cast<duckdb::FunctionExpression>();
      if (fn.FunctionName() == "list_value" ||
          fn.FunctionName() == "array_value") {
        duckdb::vector<duckdb::Value> elements;
        elements.reserve(fn.GetArguments().size());
        duckdb::LogicalType child_type = duckdb::LogicalType::SQLNULL;
        for (const auto& arg : fn.GetArguments()) {
          const auto& c = arg.GetExpression();
          auto folded = self(c);
          if (!folded) {
            return std::nullopt;
          }
          if (child_type.id() == duckdb::LogicalTypeId::SQLNULL) {
            child_type = folded->type();
          }
          elements.push_back(std::move(*folded));
        }
        return duckdb::Value::LIST(child_type, std::move(elements));
      }
      if (fn.FunctionName() == "struct_pack") {
        duckdb::child_list_t<duckdb::Value> fields;
        fields.reserve(fn.GetArguments().size());
        for (const auto& arg : fn.GetArguments()) {
          const auto& c = arg.GetExpression();
          if (c.GetAlias().empty()) {
            return std::nullopt;
          }
          auto folded = self(c);
          if (!folded) {
            return std::nullopt;
          }
          fields.emplace_back(c.GetAlias(), std::move(*folded));
        }
        return duckdb::Value::STRUCT(std::move(fields));
      }
    }
    return std::nullopt;
  };

  for (const auto& arg : fn_expr.GetArguments()) {
    const auto& child = arg.GetExpression();
    if (child.GetExpressionType() == duckdb::ExpressionType::COMPARE_EQUAL) {
      auto& comp = child.Cast<duckdb::ComparisonExpression>();
      if (comp.Left().GetExpressionType() ==
          duckdb::ExpressionType::COLUMN_REF) {
        const auto& colref = comp.Left().Cast<duckdb::ColumnRefExpression>();
        if (!colref.IsQualified()) {
          if (auto v = peel_cast(comp.Right())) {
            named_params.emplace(colref.GetColumnName().GetIdentifierName(),
                                 std::move(*v));
            continue;
          }
        }
      }
      return std::nullopt;
    }
    if (auto v = peel_cast(child)) {
      args.push_back(std::move(*v));
      continue;
    }
    return std::nullopt;
  }
  auto canonical =
    std::string{ResolveAlias(fn_expr.FunctionName().GetIdentifierName())};
  const auto* entry = LookupRegistry(canonical);
  if (!entry) {
    return std::nullopt;
  }
  if (args.size() != 1 ||
      args[0].type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return std::nullopt;
  }
  const bool is_json = canonical == "read_json" || canonical == "read_ndjson" ||
                       canonical == "read_json_objects" ||
                       canonical == "read_ndjson_objects";
  if (is_json) {
    if (auto it = named_params.find("compression");
        it != named_params.end() &&
        it->second.type().id() == duckdb::LogicalTypeId::VARCHAR) {
      auto comp = absl::AsciiStrToLower(it->second.GetValue<std::string>());
      if (comp != "none" && comp != "uncompressed" && comp != "auto") {
        return std::nullopt;
      }
    }
    const auto path = args[0].GetValue<std::string>();
    static constexpr std::string_view kCompressedSuffixes[] = {
      ".gz", ".gzip", ".zst", ".zstd", ".bz2", ".xz", ".lzma"};
    auto lower_path = absl::AsciiStrToLower(path);
    for (auto suffix : kCompressedSuffixes) {
      if (lower_path.ends_with(suffix)) {
        return std::nullopt;
      }
    }
  }
  ViewFastPath out;
  out.function_name = std::move(canonical);
  out.args = std::move(args);
  out.named_params = std::move(named_params);
  out.is_glob = LooksLikeGlob(out.args[0].GetValue<std::string>());
  out.projection_columns = std::move(projection_columns);
  out.pk_spec = out.is_glob ? entry->glob_pk_spec : entry->single_pk_spec;
  return out;
}

std::vector<duckdb::column_t> BackfillPkVirtualColumns(const ViewFastPath& fp) {
  if (fp.pk_spec == catalog::PkSpec::ExternalDBKey) {
    // The PK is a real source column (not a virtual one); project it so the
    // index sink can key postings by its value.
    return {fp.pk_column_index};
  }
  if (fp.pk_spec == catalog::PkSpec::DuckDBRowId) {
    return {duckdb::COLUMN_IDENTIFIER_ROW_ID};
  }
  if (fp.pk_spec == catalog::PkSpec::FileIndexPlusDuckDBRowId) {
    return {duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX,
            duckdb::COLUMN_IDENTIFIER_ROW_ID};
  }
  if (catalog::IsGlobPK(fp.pk_spec)) {
    return {duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX,
            duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER};
  }
  return {duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER};
}

duckdb::TableFunction MakeFastPathLookupFunction(const ViewFastPath& fp) {
  const auto* entry = LookupRegistry(fp.function_name);
  SDB_ENSURE(entry, ERROR_INTERNAL,
             "fast-path classification missing for function ",
             fp.function_name);
  return entry->make_lookup();
}

void EnableIcebergSort(duckdb::FunctionData* bind_data) noexcept {
  if (!bind_data) {
    return;
  }
  auto* multi_bd = dynamic_cast<duckdb::MultiFileBindData*>(bind_data);
  if (!multi_bd || !multi_bd->file_list) {
    return;
  }
  if (auto* iceberg_list = dynamic_cast<duckdb::IcebergMultiFileList*>(
        multi_bd->file_list.get())) {
    iceberg_list->need_sort = true;
  }
}

duckdb::unique_ptr<duckdb::FunctionData> BindFastPathSource(
  duckdb::ClientContext& context, const ViewFastPath& fp) {
  if (fp.catalog_ref) {
    auto& entry =
      duckdb::Catalog::GetEntry(
        context, duckdb::CatalogType::TABLE_ENTRY,
        duckdb::QualifiedName(duckdb::Identifier{fp.catalog_ref->catalog},
                              duckdb::Identifier{fp.catalog_ref->schema},
                              duckdb::Identifier{fp.catalog_ref->table}))
        .Cast<duckdb::TableCatalogEntry>();
    duckdb::unique_ptr<duckdb::FunctionData> bind_data;
    std::optional<duckdb::BoundAtClause> at_clause;
    if (fp.pinned_iceberg_snapshot_id != 0) {
      at_clause.emplace("version",
                        duckdb::Value::BIGINT(fp.pinned_iceberg_snapshot_id));
    }
    duckdb::EntryLookupInfo lookup(
      duckdb::CatalogType::TABLE_ENTRY,
      duckdb::QualifiedName(duckdb::Identifier{fp.catalog_ref->table}),
      at_clause ? duckdb::optional_ptr<duckdb::BoundAtClause>(&*at_clause)
                : duckdb::optional_ptr<duckdb::BoundAtClause>{},
      duckdb::QueryErrorContext{});
    auto fn = entry.GetScanFunction(context, bind_data, lookup);
    if (fn.get_virtual_columns && bind_data) {
      fn.get_virtual_columns(context, bind_data.get());
    }
    if (fp.function_name == "iceberg_scan" && bind_data) {
      EnableIcebergSort(bind_data.get());
    }
    return bind_data;
  }
  SDB_ASSERT(!fp.args.empty());
  auto reader = LookupSingleStringReader(context, fp.function_name);
  duckdb::vector<duckdb::Value> inputs = fp.args;
  duckdb::named_parameter_map_t named_params;
  named_params.reserve(fp.named_params.size() + 1);
  for (auto& [k, v] : fp.named_params) {
    auto it = reader.named_parameters.find(k);
    if (it == reader.named_parameters.end() ||
        it->second.id() == duckdb::LogicalTypeId::ANY) {
      named_params.emplace(k, v);
      continue;
    }
    duckdb::Value coerced = v;
    if (!coerced.DefaultTryCastAs(it->second)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("named argument `", k.GetIdentifierName(), "` for `",
                fp.function_name, "` cannot be coerced from ",
                v.type().ToString(), " to ", it->second.ToString()));
    }
    named_params.emplace(k, std::move(coerced));
  }
  if (fp.pinned_iceberg_snapshot_id != 0 &&
      fp.function_name == "iceberg_scan") {
    named_params["snapshot_from_id"] = duckdb::Value::UBIGINT(
      static_cast<uint64_t>(fp.pinned_iceberg_snapshot_id));
  }
  duckdb::vector<duckdb::LogicalType> unused_types;
  duckdb::vector<duckdb::Identifier> unused_names;
  duckdb::TableFunctionRef unused_ref;
  duckdb::TableFunctionBindInput input(inputs, named_params, unused_types,
                                       unused_names, reader.function_info.get(),
                                       nullptr, reader, unused_ref);
  duckdb::vector<duckdb::LogicalType> out_types;
  duckdb::vector<std::string> out_names;
  auto bind_data = reader.bind(context, input, out_types, out_names);
  if (reader.get_virtual_columns && bind_data) {
    reader.get_virtual_columns(context, bind_data.get());
  }
  if (fp.function_name == "iceberg_scan" && bind_data) {
    EnableIcebergSort(bind_data.get());
  }

  return bind_data;
}

int64_t ExtractIcebergSnapshotId(duckdb::FunctionData& bind_data) noexcept {
  auto* multi_bd = dynamic_cast<duckdb::MultiFileBindData*>(&bind_data);
  if (!multi_bd || !multi_bd->file_list) {
    return 0;
  }
  auto* iceberg_list =
    dynamic_cast<duckdb::IcebergMultiFileList*>(multi_bd->file_list.get());
  if (!iceberg_list) {
    return 0;
  }
  const auto& snapshot_info = iceberg_list->GetSnapshot();
  if (!snapshot_info.snapshot) {
    return 0;
  }
  return snapshot_info.snapshot->snapshot_id;
}

std::string FormatLookupLabel(const ViewFastPath& fp) {
  if (fp.function_name == "iceberg_scan") {
    return "iceberg";
  }
  std::string_view name = fp.function_name;
  if (name.starts_with("read_")) {
    name.remove_prefix(5);
  }
  if (name == "ndjson") {
    name = "json";
  }
  if (fp.is_glob) {
    return absl::StrCat("glob ", name);
  }
  return std::string{name};
}

}  // namespace sdb::connector
