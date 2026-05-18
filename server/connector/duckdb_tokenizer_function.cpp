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

#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>
#include <unicode/locid.h>
#include <vpack/builder.h>
#include <vpack/value.h>
#include <vpack/value_type.h>

#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/minhash_tokenizer.hpp>
#include <iresearch/analysis/multi_delimited_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/path_hierarchy_tokenizer.hpp>
#include <iresearch/analysis/pattern_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/stopwords_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <type_traits>
#include <utility>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/opclass.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "pg/commands/create_opclass.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

// PRAGMA create_opclass('name', if_not_exists, key := value, ...)
// Positional parameters:
//   [0] name (VARCHAR)  -- optionally schema-qualified as "schema.name"
//   [1] if_not_exists (BOOLEAN)
// Named parameters: opclass options (type, plus per-type options).
void CreateOpClassPragma(duckdb::ClientContext& context,
                         const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 2) {
    throw duckdb::InvalidInputException(
      "create_opclass requires at least name and if_not_exists");
  }

  auto dict_name = args[0].GetValue<std::string>();
  auto if_not_exists = args[1].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  auto name = pg::ParseObjectName(dict_name, StaticStrings::kPublic);
  pg::CreateOpClass(conn_ctx, name.relation, name.schema, if_not_exists,
                    params.named_parameters);
}

// PRAGMA drop_opclass('name', missing_ok)
// Parameters:
//   [0] name (VARCHAR) -- optionally schema-qualified as "schema.name"
//   [1] missing_ok (BOOLEAN)
void DropOpClassPragma(duckdb::ClientContext& context,
                       const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 2) {
    throw duckdb::InvalidInputException(
      "drop_opclass requires name and missing_ok");
  }

  const auto dict_name = args[0].GetValue<std::string>();
  const auto missing_ok = args[1].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalog_feature.Global();

  auto name = pg::ParseObjectName(dict_name, StaticStrings::kPublic);

  auto r =
    catalog.DropOpClass(conn_ctx.GetDatabase(), name.schema, name.relation);

  std::string_view object_name = "text search dictionary";
  if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
    // The error message from catalog contains the actual object type name
    auto actual_type = r.errorMessage();
    auto actual_name = absl::AsciiStrToLower(actual_type);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("\"", name.relation, "\" is not ",
              basics::string_utils::GetArticle(object_name), " ", object_name),
      ERR_HINT("Use DROP ", absl::AsciiStrToUpper(actual_type), " to remove ",
               basics::string_utils::GetArticle(actual_name), " ", actual_name,
               "."));
  }
  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    if (!missing_ok) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG(object_name, " \"", name.relation, "\" does not exist"));
    }
    conn_ctx.AddNotice(SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                                      ERR_MSG(object_name, " \"", name.relation,
                                              "\" does not exist, skipping")));
    r = {};
  }
  SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

}  // namespace

void RegisterOpClassPragma(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  auto create_pragma = duckdb::PragmaFunction::PragmaCall(
    "create_opclass", CreateOpClassPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN});
  loader.RegisterFunction(create_pragma);

  auto drop_pragma = duckdb::PragmaFunction::PragmaCall(
    "drop_opclass", DropOpClassPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN});
  loader.RegisterFunction(drop_pragma);
}

}  // namespace sdb::connector
