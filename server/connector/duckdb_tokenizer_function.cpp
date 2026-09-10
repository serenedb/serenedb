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

#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <string>

#include "connector/duckdb_client_state.h"
#include "pg/commands/create_tsdictionary.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

// PRAGMA create_text_search_dictionary('name', if_not_exists, key := value,
// ...) Positional parameters:
//   [0] name (VARCHAR)  -- optionally schema-qualified as "schema.name"
//   [1] if_not_exists (BOOLEAN)
// Named parameters: tokenizer options (template, frequency, etc.)
void CreateTSDictionaryPragma(duckdb::ClientContext& context,
                              const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 2) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("create_text_search_dictionary requires at least name and "
              "if_not_exists"));
  }

  auto dict_name = args[0].GetValue<std::string>();
  auto if_not_exists = args[1].GetValue<bool>();

  pg::CreateTokenizer(GetSereneDBContext(context),
                      duckdb::QualifiedName::Parse(dict_name), if_not_exists,
                      params.named_parameters);
}

}  // namespace

void RegisterTokenizerPragma(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  auto create_pragma = duckdb::PragmaFunction::PragmaCall(
    "create_text_search_dictionary", CreateTSDictionaryPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN});
  // Tokenizer-specific kwargs are validated by CreateTSDictionaryPragma itself.
  create_pragma.accept_arbitrary_named_parameters = true;
  loader.RegisterFunction(create_pragma);
}

}  // namespace sdb::connector
