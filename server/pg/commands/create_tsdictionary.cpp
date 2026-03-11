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

#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
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

void BuildTextAnalyzerVPack(vpack::Builder& b, const DefineStmt& stmt) {
  for (const auto* def : PgListWrapper<DefElem>{stmt.definition}) {
    const std::string_view name{def->defname};

    if (name == "template") {
      continue;
    } else if (name == "locale") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("locale", "expected string");
      }
      b.add("locale", *val);
    } else if (name == "case") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("case", "expected string");
      }
      if (irs::kCaseConvertMap.find(*val) == irs::kCaseConvertMap.end()) {
        InvalidParameterThrow("case", "expected one of: lower, none, upper");
      }
      b.add("case", *val);
    } else if (name == "accent") {
      auto val = TryGet<bool>(def->arg);
      if (!val) {
        InvalidParameterThrow("accent", "expected boolean");
      }
      b.add("accent", *val);
    } else if (name == "stemming") {
      auto val = TryGet<bool>(def->arg);
      if (!val) {
        InvalidParameterThrow("stemming", "expected boolean");
      }
      b.add("stemming", *val);
    } else if (name == "stopwords") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("stopwords", "expected comma-separated string");
      }
      b.add("stopwords", vpack::Value{vpack::ValueType::Array});
      ParseCommaSeparated(*val, [&](std::string_view word) { b.add(word); });
      b.close();  // close "stopwords" array
    } else if (name == "stopwordspath") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("stopwordspath", "expected string");
      }
      b.add("stopwordsPath", *val);
    } else if (name == "mingram") {
      auto val = TryGet<int>(def->arg);
      if (!val || *val < 1) {
        InvalidParameterThrow("mingram", "must be a positive integer");
      }
      b.add("mingram", *val);
    } else if (name == "maxgram") {
      auto val = TryGet<int>(def->arg);
      if (!val || *val < 1) {
        InvalidParameterThrow("maxgram", "must be a positive integer");
      }
      b.add("maxgram", *val);
    } else if (name == "preserveoriginal") {
      auto val = TryGet<bool>(def->arg);
      if (!val) {
        InvalidParameterThrow("preserveoriginal", "expected boolean");
      }
      b.add("preserveoriginal", *val);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("unrecognized text search dictionary parameter: \"", name,
                "\""));
    }
  }
}

void BuildNGramAnalyzerVPack(vpack::Builder& b, const DefineStmt& stmt) {
  for (const auto* def : PgListWrapper<DefElem>{stmt.definition}) {
    const std::string_view name{def->defname};

    if (name == "template") {
      continue;
    } else if (name == "mingram") {
      auto val = TryGet<int>(def->arg);
      if (!val || *val < 1) {
        InvalidParameterThrow("mingram", "must be a positive integer");
      }
      b.add("min", static_cast<uint64_t>(*val));
    } else if (name == "maxgram") {
      auto val = TryGet<int>(def->arg);
      if (!val || *val < 1) {
        InvalidParameterThrow("maxgram", "must be a positive integer");
      }
      b.add("max", static_cast<uint64_t>(*val));
    } else if (name == "preserveoriginal") {
      auto val = TryGet<bool>(def->arg);
      if (!val) {
        InvalidParameterThrow("preserveoriginal", "expected boolean");
      }
      b.add("preserveOriginal", *val);
    } else if (name == "inputtype") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("inputtype", "expected string");
      }
      if (*val != "binary" && *val != "utf8") {
        InvalidParameterThrow("inputtype", "expected one of: binary, utf8");
      }
      b.add("streamType", *val);
    } else if (name == "startmarker") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("startmarker", "expected string");
      }
      b.add("startMarker", *val);
    } else if (name == "endmarker") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("endmarker", "expected string");
      }
      b.add("endMarker", *val);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("unrecognized text search dictionary parameter: \"", name,
                "\""));
    }
  }
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

  vpack::Builder b;
  b.openObject();
  b.add("analyzer", vpack::Value{vpack::ValueType::Object});
  b.add("type", template_name);
  b.add("properties", vpack::Value{vpack::ValueType::Object});
  if (template_name == irs::analysis::TextTokenizer::type_name()) {
    BuildTextAnalyzerVPack(b, stmt);
  } else if (template_name ==
               irs::analysis::NGramTokenizer<
                 irs::analysis::NGramTokenizerBase::InputType::Binary>::
                 type_name() ||
             template_name ==
               irs::analysis::NGramTokenizer<irs::analysis::NGramTokenizerBase::
                                               InputType::UTF8>::type_name()) {
    BuildNGramAnalyzerVPack(b, stmt);
  } else {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
      ERR_MSG("text search template \"", template_name, "\" does not exist"));
  }
  b.close();  // close "properties" object
  b.close();  // close "analyzer" object
  b.close();  // close outer object

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
