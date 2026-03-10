////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <velox/core/QueryConfig.h>
#include <velox/functions/Macros.h>
#include <velox/type/SimpleFunctionApi.h>

#include <iresearch/utils/string.hpp>

#include "basics/fwd.h"
#include "basics/misc.hpp"
#include "catalog/catalog.h"
#include "pg/connection_context.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

template<typename T>
struct PgTsLexize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(  // NOLINT
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<velox::Varchar>* /*ts_dict*/,
    const arg_type<velox::Varchar>* /*text*/) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    db_id = conn->GetDatabaseId();
    current_schema = conn->GetCurrentSchema();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Array<velox::Varchar>>& result,
                                const arg_type<velox::Varchar>& ts_dict,
                                const arg_type<velox::Varchar>& text) {
    auto snapshot = catalog::GetCatalog().GetSnapshot();
    auto object_name = ParseObjectName(ts_dict, current_schema);
    auto dict = snapshot->GetTSDictionary(db_id, object_name.schema,
                                          object_name.relation);
    if (!dict) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_NAME),
                      ERR_MSG("text search dictionary \"",
                              std::string_view{ts_dict}, "\" does not exist"));
    }
    auto tokenizer = dict->GetTokenizer();
    irs::Finally return_tokenizer = [&] mutable noexcept {
      dict->PushTokenizer(std::move(tokenizer));
    };

    bool r = tokenizer->reset(text);
    if (!r) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("error while preparing tokenizer"));
    }
    auto* value = irs::get<irs::TermAttr>(*tokenizer);
    while (tokenizer->next()) {
      result.add_item().copy_from(irs::ViewCast<char>(value->value));
    }
  }

  ObjectId db_id;
  std::string current_schema;
};

}  // namespace sdb::pg
