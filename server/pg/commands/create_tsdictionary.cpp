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
#include <frozen/unordered_set.h>
#include <unicode/locid.h>
#include <vpack/builder.h>

#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/minhash_tokenizer.hpp>
#include <iresearch/analysis/multi_delimited_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
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
    {tokenizer_options::kStopwordsPath.name, "stopwordsPath"},
    {tokenizer_options::kMinGram.name, "min"},
    {tokenizer_options::kMaxGram.name, "max"},
    {tokenizer_options::kEdgeNGramGroup.name, "edgeNGram"},
    {tokenizer_options::kPreserveOriginal.name, "preserveOriginal"},
    {tokenizer_options::kInputType.name, "streamType"},
    {tokenizer_options::kStartMarker.name, "startMarker"},
    {tokenizer_options::kEndMarker.name, "endMarker"},
    {tokenizer_options::kModelLocation.name, "model_location"},
    {tokenizer_options::kTopK.name, "top_k"},
    {tokenizer_options::kNumHashes.name, "numHashes"},
  });

void InvalidParameterThrow(const OptionInfo& opt) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("Incorrect value for parameter \"", opt.name, "\""));
}

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

std::string_view GetVPackName(std::string_view pg_name) {
  auto it = kNameMappings.find(pg_name);
  return it != kNameMappings.end() ? it->second : pg_name;
}

constexpr OptionInfo kTemplate{
  "template", OptionInfo::RequiredTag<std::string_view>{},
  "Tokenizer template type", [](const OptionInfo::DefaultValueT& value) {
    static constexpr auto kTokenizerTypes = frozen::make_unordered_set({
      irs::analysis::TextTokenizer::type_name(),
      irs::analysis::NormalizingTokenizer::type_name(),
      irs::analysis::NGramTokenizerBase::type_name(),
      irs::analysis::CollationTokenizer::type_name(),
      irs::analysis::DelimitedTokenizer::type_name(),
      irs::analysis::MultiDelimitedTokenizer::type_name(),
      irs::analysis::SegmentationTokenizer::type_name(),
      irs::analysis::ClassificationTokenizer::type_name(),
      irs::analysis::MinHashTokenizer::type_name(),
      irs::analysis::NearestNeighborsTokenizer::type_name(),
      irs::analysis::StemmingTokenizer::type_name(),
      irs::analysis::StopwordsTokenizer::type_name(),
    });
    auto str = std::get<std::string_view>(value);
    return kTokenizerTypes.count(str) == 1;
  }};
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
  void Parse() {
    ParseFeatures();

    const auto* tmpl_opt = EraseOption(kTemplate);
    SDB_ASSERT(tmpl_opt);
    auto tmpl_name = TryGet<std::string_view>(tmpl_opt->arg);
    SDB_ASSERT(tmpl_name);
    const std::string_view type = *tmpl_name;

    const OptionGroup* subgroup = nullptr;
    for (const auto& g : tokenizer_options::kTokenizerSubgroups) {
      if (g.name == type) {
        subgroup = &g;
        break;
      }
    }
    SDB_ASSERT(subgroup);

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
    auto write_to_builder = [&](const auto& options) {
      for (const auto& opt : options) {
        if (opt.name == tokenizer_options::kStopwords.name) {
          std::string_view stopwords =
            EraseOptionOrDefault<tokenizer_options::kStopwords>();
          _builder.add("stopwords", vpack::Value{vpack::ValueType::Array});
          ParseCommaSeparated(
            stopwords, [&](std::string_view word) { _builder.add(word); });
          _builder.close();
        } else {
          ApplyOnOptionOrDefault(opt, [&](const auto& val) {
            _builder.add(GetVPackName(opt.name), val);
          });
        }
        EraseOption(opt);
      }
    };

    if (subgroup.name == tokenizer_options::kTextGroup.name) {
      // process edge ngram
      const auto& edge_ngram = tokenizer_options::kEdgeNGramGroup;
      bool has_ngram = false;
      for (const auto& opt : edge_ngram.options) {
        has_ngram |= _options.contains(opt.name);
      }
      if (has_ngram) {
        _builder.add(GetVPackName(tokenizer_options::kEdgeNGramGroup.name),
                     vpack::Value{vpack::ValueType::Object});
        write_to_builder(edge_ngram.options);
        _builder.close();
      }
    }

    write_to_builder(subgroup.options);
  }

  void ParseFeatures() {
    const auto& features_subgroup = tokenizer_options::kFeaturesGroup;
    auto features = features_subgroup.FlatOptions();
    for (const auto& feature : features) {
      auto it = _options.find(feature.name);
      if (it == _options.end()) {
        continue;
      }
      _options.erase(it);
      if (!_features.Add(feature.name)) {
        InvalidParameterThrow(feature);
      }
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
