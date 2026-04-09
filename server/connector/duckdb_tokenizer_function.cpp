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

#include "connector/duckdb_tokenizer_function.h"

#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <vpack/builder.h>
#include <vpack/value.h>

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

// PRAGMA create_text_search_dictionary('name', false, 'template=text', ...)
// Parameters:
//   [0] name (VARCHAR)
//   [1] if_not_exists (BOOLEAN)
//   [2..] option pairs as "key=value" strings
void CreateTSDictionaryPragma(duckdb::ClientContext& context,
                              const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 2) {
    throw duckdb::InvalidInputException(
      "create_text_search_dictionary requires at least name and if_not_exists");
  }

  auto dict_name = args[0].GetValue<std::string>();
  auto if_not_exists = args[1].GetValue<bool>();

  // Parse options from "key=value" string parameters
  std::unordered_map<std::string, std::string> options;
  for (size_t i = 2; i < args.size(); ++i) {
    auto opt = args[i].GetValue<std::string>();
    auto eq = opt.find('=');
    if (eq != std::string::npos) {
      options[opt.substr(0, eq)] = opt.substr(eq + 1);
    }
  }

  // Get template type (required)
  auto it = options.find("template");
  if (it == options.end()) {
    throw duckdb::InvalidInputException(
      "CREATE TEXT SEARCH DICTIONARY requires 'template' option");
  }
  auto template_type = it->second;
  options.erase(it);

  // Build VPack serialization (same format as CreateTSDictionaryOptions)
  vpack::Builder builder;
  builder.openObject();

  // analyzer object
  builder.add(std::string_view{"analyzer"},
              vpack::Value(vpack::ValueType::Object));
  builder.add(std::string_view{"type"}, std::string_view{template_type});
  builder.add(std::string_view{"properties"},
              vpack::Value(vpack::ValueType::Object));
  for (auto& [key, value] : options) {
    // Skip feature options — handled separately
    if (key == "frequency" || key == "position" || key == "offset" ||
        key == "norm") {
      continue;
    }
    auto sv_key = std::string_view{key};
    // Try to parse as bool
    if (value == "true" || value == "false") {
      builder.add(sv_key, value == "true");
    } else {
      // Try as integer
      try {
        auto num = std::stoll(value);
        builder.add(sv_key, static_cast<int64_t>(num));
      } catch (...) {
        // String
        builder.add(sv_key, std::string_view{value});
      }
    }
  }
  builder.close();  // close properties
  builder.close();  // close analyzer

  // features array — needs a key inside the root object
  search::Features features;
  for (auto& [key, value] : options) {
    if ((key == "frequency" || key == "position" || key == "offset" ||
         key == "norm") &&
        value == "true") {
      features.Add(key);
    }
  }
  // Features::ToVPack opens its own ArrayBuilder
  // But we need to write it as a named field "features" in the root object.
  // The ArrayBuilder in ToVPack creates an unnamed array, which inside
  // an object needs a key. Let's write it manually.
  builder.add(std::string_view{"features"},
              vpack::Value(vpack::ValueType::Array));
  for (const auto* feat_name : {"frequency", "position", "offset", "norm"}) {
    auto fi = options.find(feat_name);
    if (fi != options.end() && fi->second == "true") {
      builder.add(std::string_view{feat_name});
    }
  }
  builder.close();  // close features array

  builder.close();  // close root

  // Create tokenizer object
  auto slice = builder.slice();
  auto tokenizer = std::make_shared<catalog::Tokenizer>(
    ObjectId{0}, dict_name, features,
    std::string{reinterpret_cast<const char*>(slice.start()),
                slice.byteSize()});

  // Register in catalog
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalog_feature.Global();
  auto snapshot = catalog.GetCatalogSnapshot();
  auto databases = snapshot->GetDatabases();
  SDB_ASSERT(!databases.empty());

  auto r =
    catalog.CreateTokenizer(databases.front()->GetId(), "public",
                            std::move(tokenizer));

  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && if_not_exists) {
    return;
  }
  if (!r.ok()) {
    throw duckdb::InvalidInputException(
      "Failed to create text search dictionary: %s",
      std::string{r.errorMessage()});
  }
}

}  // namespace

void RegisterTokenizerPragma(duckdb::DatabaseInstance& db) {
  auto pragma = duckdb::PragmaFunction::PragmaCall(
    "create_text_search_dictionary", CreateTSDictionaryPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN});
  pragma.varargs = duckdb::LogicalType::VARCHAR;

  duckdb::ExtensionLoader loader(db, "serenedb");
  loader.RegisterFunction(pragma);
}

}  // namespace sdb::connector
