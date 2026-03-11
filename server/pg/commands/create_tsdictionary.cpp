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

irs::analysis::TextTokenizer::OptionsT ParseTextTokenizerOptions(
  const DefineStmt& stmt) {
  irs::analysis::TextTokenizer::OptionsT options;

  for (const auto* def : PgListWrapper<DefElem>{stmt.definition}) {
    const std::string_view name{def->defname};

    if (name == "template") {
      continue;
    } else if (name == "locale") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("locale", "expected string");
      }
      options.locale = icu::Locale::createFromName(val->data());
      if (options.locale.isBogus()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("unrecognized locale \"", *val, "\""));
      }
    } else if (name == "case") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("case", "expected string");
      }
      const auto* it = irs::kCaseConvertMap.find(*val);
      if (it == irs::kCaseConvertMap.end()) {
        InvalidParameterThrow("cast", "expected one of: lower, none, upper");
      }
      options.case_convert = it->second;
    } else if (name == "accent") {
      auto val = TryGet<bool>(def->arg);
      if (!val) {
        InvalidParameterThrow("accent", "expected boolean");
      }
      options.accent = *val;
    } else if (name == "stemming") {
      auto val = TryGet<bool>(def->arg);
      if (!val) {
        InvalidParameterThrow("stemming", "expected boolean");
      }
      options.stemming = *val;
    } else if (name == "stopwords") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("stopwords", "expected comma-separated string");
      }
      ParseCommaSeparated(*val, [&](std::string_view word) {
        options.explicit_stopwords.emplace(word);
      });
      options.explicit_stopwords_set = true;
    } else if (name == "stopwordspath") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("stopwordspath", "expected string");
      }
      options.stopwords_path = std::move(*val);
    } else if (name == "mingram") {
      auto val = TryGet<int>(def->arg);
      if (!val || *val < 1) {
        InvalidParameterThrow("mingram", "must be a positive integer");
      }
      options.min_gram = static_cast<size_t>(*val);
      options.min_gram_set = true;
    } else if (name == "maxgram") {
      auto val = TryGet<int>(def->arg);
      if (!val || *val < 1) {
        InvalidParameterThrow("maxgram", "must be a positive integer");
      }
      options.max_gram = static_cast<size_t>(*val);
      options.max_gram_set = true;
    } else if (name == "preserveoriginal") {
      auto val = TryGet<bool>(def->arg);
      if (!val) {
        InvalidParameterThrow("preserveoriginal", "expected bool");
      }
      options.preserve_original = *val;
      options.preserve_original_set = true;
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("unrecognized text search dictionary parameter: \"", name,
                "\""));
    }
  }

  if (options.locale.isBogus()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("parameter \"locale\" is required for text search dictionary "
              "with template \"text\""));
  }

  if (options.min_gram_set && options.max_gram_set &&
      options.min_gram > options.max_gram) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("\"mingram\" must be less than or equal to \"maxgram\""));
  }

  return options;
}

irs::analysis::NGramTokenizerBase::Options ParseNGramTokenizerOptions(
  const DefineStmt& stmt) {
  irs::analysis::NGramTokenizerBase::Options options;
  bool min_gram_set = false;
  bool max_gram_set = false;

  for (const auto* def : PgListWrapper<DefElem>{stmt.definition}) {
    const std::string_view name{def->defname};

    if (name == "template") {
      continue;
    } else if (name == "mingram") {
      auto val = TryGet<int>(def->arg);
      if (!val || *val < 1) {
        InvalidParameterThrow("mingram", "must be a positive integer");
      }
      options.min_gram = static_cast<size_t>(*val);
      min_gram_set = true;
    } else if (name == "maxgram") {
      auto val = TryGet<int>(def->arg);
      if (!val || *val < 1) {
        InvalidParameterThrow("maxgram", "must be a positive integer");
      }
      options.max_gram = static_cast<size_t>(*val);
      max_gram_set = true;
    } else if (name == "preserveoriginal") {
      auto val = TryGet<bool>(def->arg);
      if (!val) {
        InvalidParameterThrow("preserveoriginal", "expected boolean");
      }
      options.preserve_original = *val;
    } else if (name == "inputtype") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("inputtype", "expected string");
      }
      if (*val == "binary") {
        options.stream_bytes_type =
          irs::analysis::NGramTokenizerBase::InputType::Binary;
      } else if (*val == "utf8") {
        options.stream_bytes_type =
          irs::analysis::NGramTokenizerBase::InputType::UTF8;
      } else {
        InvalidParameterThrow("inputtype", "expected one of: binary, utf8");
      }
    } else if (name == "startmarker") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("startmarker", "expected string");
      }
      options.start_marker = irs::bstring{
        reinterpret_cast<const irs::byte_type*>(val->data()), val->size()};
    } else if (name == "endmarker") {
      auto val = TryGet<std::string_view>(def->arg);
      if (!val) {
        InvalidParameterThrow("endmarker", "expected string");
      }
      options.end_marker = irs::bstring{
        reinterpret_cast<const irs::byte_type*>(val->data()), val->size()};
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("unrecognized text search dictionary parameter: \"", name,
                "\""));
    }
  }

  if (!min_gram_set) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("parameter \"mingram\" is required for text search dictionary "
              "with template \"ngram\""));
  }

  if (!max_gram_set) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("parameter \"maxgram\" is required for text search dictionary "
              "with template \"ngram\""));
  }

  if (options.min_gram > options.max_gram) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("\"mingram\" must be less than or equal to \"maxgram\""));
  }

  return options;
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

  std::shared_ptr<catalog::TSDictionary> ts_dict;
  if (template_name == "text") {
    auto options = ParseTextTokenizerOptions(stmt);
    ts_dict =
      catalog::TSDictionary::CreateTokenizer<irs::analysis::TextTokenizer>(
        ObjectId{0}, dict_name.relation, options, options.explicit_stopwords);
  } else if (template_name == "ngram") {
    auto options = ParseNGramTokenizerOptions(stmt);
    if (options.stream_bytes_type ==
        irs::analysis::NGramTokenizerBase::InputType::UTF8) {
      ts_dict = catalog::TSDictionary::CreateTokenizer<irs::analysis::NGramTokenizer<
        irs::analysis::NGramTokenizerBase::InputType::UTF8>>(
        ObjectId{0}, dict_name.relation, options);
    } else {
      ts_dict = catalog::TSDictionary::CreateTokenizer<irs::analysis::NGramTokenizer<
        irs::analysis::NGramTokenizerBase::InputType::Binary>>(
        ObjectId{0}, dict_name.relation, options);
    }
  } else {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
      ERR_MSG("text search template \"", template_name, "\" does not exist"));
  }
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
