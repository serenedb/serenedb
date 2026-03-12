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
#include <iresearch/index/index_features.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <yaclib/async/make.hpp>

#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/option_help.h"
#include "pg/options_parser.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_error.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "pg/tokenizer_options.h"
#include "utils/elog.h"
#include "utils/exec_context.h"
#include "vpack/value.h"
#include "vpack/value_type.h"

namespace sdb::pg {

namespace {

using namespace std::string_view_literals;

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

std::string_view GetVPackName(std::string_view pg_name) {
  auto it = kNameMappings.find(pg_name);
  return it != kNameMappings.end() ? it->second : pg_name;
}

constexpr OptionInfo kTemplate{"template",
                               OptionInfo::RequiredTag<std::string_view>{},
                               "Tokenizer template type"};
constexpr OptionInfo kTSDictionaryRootOptions[] = {kTemplate};
constexpr OptionGroup kTSDictionaryOptionGroups[] = {
  {"Text Search Dictionary", kTSDictionaryRootOptions,
   tokenizer_options::kTokenizerSubgroups},
};

class CreateTSDictionaryOptions : public OptionsParser {
 public:
  CreateTSDictionaryOptions(const List* ts_dictionary_options)
    : OptionsParser(MakeOptions(ts_dictionary_options, {}),
                    kTSDictionaryOptionGroups,
                    {.operation = "CREATE TEXT SEARCH DICTIONARY"}) {
    ParseOptions([&] { Parse(); });
  }

  auto Result() && { return std::make_pair(std::move(_builder), _features); }

 private:
  static const OptionGroup* FindSubgroup(std::string_view name) {
    for (const auto& group : tokenizer_options::kTokenizerSubgroups) {
      if (group.name == name) {
        return &group;
      }
    }
    return nullptr;
  }

  void Parse() {
    ParseFeatures();

    const auto* tmpl_opt = EraseOption(kTemplate);
    if (!tmpl_opt) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("template value is not provided"));
    }
    auto tmpl_name = TryGet<std::string_view>(tmpl_opt->arg);
    if (!tmpl_name) {
      InvalidParameterThrow("template", "expected string");
    }
    const std::string_view type = *tmpl_name;

    const auto* subgroup = FindSubgroup(type);
    if (!subgroup) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unknown tokenizer template \"", type, "\""));
    }

    // Validate all remaining options belong to this template's group
    auto valid_names = subgroup->FlatNames();
    for (const auto& [name, opt] : _options) {
      if (std::ranges::find(valid_names, name) == valid_names.end()) {
        THROW_SQL_ERROR(
          CURSOR_POS(ErrorPosition(ExprLocation(opt))),
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("option \"", name, "\" is not supported by tokenizer \"",
                  type, "\""));
      }
    }

    _builder.openObject();
    _builder.add("analyzer", vpack::Value{vpack::ValueType::Object});
    _builder.add("properties", vpack::Value{vpack::ValueType::Object});

    WriteTokenizerOptions(*subgroup);

    _builder.close();  // close properties
    _builder.add("type", type);
    _builder.close();  // close analyzer
    _builder.close();  // close object
  }

  void WriteTokenizerOptions(const OptionGroup& subgroup) {
    if (const auto* opt = EraseOption(tokenizer_options::kStopwords)) {
      auto val = TryGet<std::string_view>(opt->arg);
      if (!val) {
        InvalidParameterThrow("stopwords", "expected comma-separated string");
      }
      _builder.add("stopwords", vpack::Value{vpack::ValueType::Array});
      ParseCommaSeparated(*val,
                          [&](std::string_view word) { _builder.add(word); });
      _builder.close();  // close stopwords array
    }

    for (const auto& opt : subgroup.FlatOptions()) {
      auto it = _options.find(opt.name);
      if (it == _options.end()) {
        continue;
      }
      const auto* def = it->second;
      _options.erase(it);
      WriteParam(GetVPackName(opt.name), opt.type, def->arg);
    }
  }

  void ParseFeatures() {
    const auto* features_subgroup =
      FindSubgroup(tokenizer_options::kFeaturesGroup.name);
    if (!features_subgroup) {
      return;
    }
    auto features = features_subgroup->FlatOptions();
    for (const auto& feature : features) {
      auto it = _options.find(feature.name);
      if (it == _options.end()) {
        continue;
      }
      _options.erase(it);
      if (!_features.Add(feature.name)) {
        InvalidParameterThrow(feature.name, "feature name was not found");
      }
    }
  }

  void WriteParam(std::string_view name, OptionInfo::Type type,
                  const Node* value) {
    switch (type) {
      case OptionInfo::Type::Boolean: {
        auto val = TryGet<bool>(value);
        if (!val) {
          InvalidParameterThrow(name, "expected boolean");
        }
        _builder.add(name, *val);
      } break;
      case OptionInfo::Type::Integer: {
        auto val = TryGet<int>(value);
        if (!val) {
          InvalidParameterThrow(name, "expected integer");
        }
        _builder.add(name, *val);
      } break;
      case OptionInfo::Type::Double: {
        auto val = TryGet<double>(value);
        if (!val) {
          InvalidParameterThrow(name, "expected float");
        }
        _builder.add(name, *val);
      } break;
      case OptionInfo::Type::String: {
        auto val = TryGet<std::string_view>(value);
        if (!val) {
          InvalidParameterThrow(name, "expected string");
        }
        _builder.add(name, *val);
      } break;
      default:
        InvalidParameterThrow(name, "unsupported option type");
    }
  }

  vpack::Builder _builder;
  search::Features _features;
};

}  // namespace

yaclib::Future<> CreateTokenizer(ExecContext& ctx, const DefineStmt& stmt) {
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(ctx);
  const auto db = ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();
  const auto dict_name =
    ParseObjectName(stmt.defnames, ctx.GetDatabase(), current_schema);

  auto [b, features] =
    std::move(CreateTSDictionaryOptions{stmt.definition}).Result();

  auto ts_dict = std::make_shared<catalog::Tokenizer>(
    ObjectId{0}, dict_name.relation, features,
    std::string{reinterpret_cast<const char*>(b.slice().getDataPtr()),
                b.slice().byteSize()});

  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();
  auto r = catalog.CreateTokenizer(db, dict_name.schema, std::move(ts_dict));

  if (!r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("unable to create text search dictionary"));
  }
  return yaclib::MakeFuture();
}

}  // namespace sdb::pg
