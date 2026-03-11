/// DISCLAIMER
////////////////////////////////////////////////////////////////////////////////
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

#include <frozen/unordered_map.h>
#include <unicode/locid.h>
#include <vpack/builder.h>

#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/minhash_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/stopwords_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <yaclib/async/make.hpp>

#include "catalog/text_search_dictionary.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "utils/elog.h"
#include "utils/exec_context.h"
#include "vpack/value.h"
#include "vpack/value_type.h"

namespace sdb::pg {

namespace {
constexpr auto kNameMappings =
  frozen::make_unordered_map<std::string_view, std::string_view>({
    {"stopwordspath", "stopwordsPath"},
    {"mingram", "min"},
    {"maxgram", "max"},
    {"preserveoriginal", "preserveOriginal"},
    {"inputtype", "streamType"},
    {"startmarker", "startMarker"},
    {"endmarker", "endMarker"},
    {"modellocation", "model_location"},
    {"topk", "top_k"},
    {"numhashes", "numHashes"},
  });

void ParseCommaSeparated(std::string_view input,
                         std::invocable<std::string_view> auto&& callback) {
  while (!input.empty()) {
    auto pos = input.find(',');
    auto token = input.substr(0, pos);
    while (!token.empty() &&
           std::isspace(static_cast<unsigned char>(token.front()))) {
      token.remove_prefix(1);
    }
    while (!token.empty() &&
           std::isspace(static_cast<unsigned char>(token.back()))) {
      token.remove_suffix(1);
    }
    if (!token.empty()) {
      callback(token);
    }
    input = pos == std::string_view::npos ? "" : input.substr(pos + 1);
  }
}

void InvalidParameterThrow(std::string_view param, std::string_view details) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("invalid value for parameter "
                          "\"",
                          param, "\": ", details));
}

template<NodeTag Tag>
void ProcessParamWithType(vpack::Builder& b, std::string_view name,
                          const Node* value) {
  if constexpr (Tag == T_Integer) {
    auto val = TryGet<int>(value);
    if (!val) {
      InvalidParameterThrow(name, "expected integer");
    }
    b.add(name, *val);
  } else if constexpr (Tag == T_String) {
    auto val = TryGet<std::string_view>(value);
    if (!val) {
      InvalidParameterThrow(name, "expected string");
    }
    b.add(name, *val);
  } else if constexpr (Tag == T_Boolean) {
    auto val = TryGet<bool>(value);
    if (!val) {
      InvalidParameterThrow(name, "expected boolean");
    }
    b.add(name, *val);
  } else if constexpr (Tag == T_Float) {
    auto val = TryGet<double>(value);
    if (!val) {
      InvalidParameterThrow(name, "expected float");
    }
    b.add(name, *val);
  } else {
    static_assert(false);
  }
}

void ProcessParam(vpack::Builder& b, std::string_view name, const Node* value) {
  if (nodeTag(value) == T_Integer) {
    ProcessParamWithType<T_Integer>(b, name, value);
  } else if (nodeTag(value) == T_String) {
    ProcessParamWithType<T_String>(b, name, value);
  } else if (nodeTag(value) == T_Boolean) {
    ProcessParamWithType<T_Boolean>(b, name, value);
  } else if (nodeTag(value) == T_Float) {
    ProcessParamWithType<T_Float>(b, name, value);
  } else {
    InvalidParameterThrow(name, "cannot process value type");
  }
}

void VisitDefineDefinitions(const DefineStmt& stmt, auto&& callback) {
  for (const auto* def : PgListWrapper<DefElem>{stmt.definition}) {
    const std::string_view name{def->defname};
    std::string_view vpack_name;
    auto it = kNameMappings.find(name);
    if (it == kNameMappings.end()) {
      vpack_name = name;
    } else {
      vpack_name = it->second;
    }
    callback(vpack_name, def->arg);
  }
}

vpack::Builder BuildTokenizerVPack(const DefineStmt& stmt) {
  vpack::Builder b;
  std::string_view template_name;
  b.openObject();
  b.add("analyzer", vpack::Value{vpack::ValueType::Object});
  b.add("properties", vpack::Value{vpack::ValueType::Object});
  VisitDefineDefinitions(stmt, [&](std::string_view name, const Node* value) {
    if (name == "accent" || name == "stemming" || name == "preserveOriginal") {
      ProcessParamWithType<T_Boolean>(b, name, value);
    } else if (name == "stopwords") {
      auto val = TryGet<std::string_view>(value);
      if (!val) {
        InvalidParameterThrow(name, "expected comma-separated string");
      }
      b.add("stopwords", vpack::Value{vpack::ValueType::Array});
      ParseCommaSeparated(*val, [&](std::string_view word) { b.add(word); });
      b.close();  // close "stopwords" array
    } else if (name == "hex") {
      ProcessParamWithType<T_Boolean>(b, name, value);
    } else if (name == "template") {
      auto type = TryGet<std::string_view>(value);
      if (!type) {
        InvalidParameterThrow(name, "expected string");
      }
      template_name = *type;
    } else {
      ProcessParam(b, name, value);
    }
  });
  b.close();  // close properties
  if (!template_name.data()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("template value is not provided"));
  }
  b.add("type", template_name);
  b.close();  // close analyzer
  b.close();  // close object
  return b;
}

}  // namespace

yaclib::Future<> CreateTSDictionary(ExecContext& ctx, const DefineStmt& stmt) {
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(ctx);
  const auto db = ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();
  const auto dict_name =
    ParseObjectName(stmt.defnames, ctx.GetDatabase(), current_schema);

  std::string_view template_name;
  PgListWrapper<DefElem> defs{stmt.definition};
  for (const auto* def : defs) {
    std::string_view name{def->defname};
    if (name == "template") {
      if (template_name.data()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
                        ERR_MSG("multiple \"template\" definitions"));
      }
      auto type = TryGet<std::string_view>(def->arg);
      if (!type) {
        InvalidParameterThrow("template", "expected string");
      }
      template_name = *type;
    }
  }

  vpack::Builder b = BuildTokenizerVPack(stmt);

  auto ts_dict = std::make_shared<catalog::TSDictionary>(
    ObjectId{0}, dict_name.relation,
    std::string{reinterpret_cast<const char*>(b.slice().getDataPtr()),
                b.slice().byteSize()});

  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();
  auto r = catalog.CreateTSDictionary(db, dict_name.schema, std::move(ts_dict));

  if (!r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("unable to create text search dictionary"));
  }
  return yaclib::MakeFuture();
}

}  // namespace sdb::pg
