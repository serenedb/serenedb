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
#include <type_traits>
#include <utility>
#include <yaclib/async/make.hpp>

#include "basics/assert.h"
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

void CheckTemplate(std::string_view value) {
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
  if (kTokenizerTypes.count(value) != 1) {
    THROW_SQL_ERROR(ERR_MSG(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Invalid type of text search dictionary"));
  }
}

constexpr OptionInfo kTemplate{"template",
                               OptionInfo::RequiredTag<std::string_view>{},
                               "Tokenizer template type", CheckTemplate};
constexpr OptionInfo kTSDictionaryRootOptions[] = {kTemplate};
constexpr OptionGroup kTSDictionaryGroup = {
  "Text Search Dictionary", kTSDictionaryRootOptions,
  tokenizer_options::kTokenizerSubgroups};

class CreateTSDictionaryOptions : public OptionsParser {
 public:
  CreateTSDictionaryOptions(const List* ts_dictionary_options)
    : OptionsParser(MakeOptions(ts_dictionary_options, {}),
                    {kTSDictionaryGroup},
                    {.operation = "CREATE TEXT SEARCH DICTIONARY"}) {
    ParseOptions([&] { Parse(); });
  }

  auto Result() && { return std::make_pair(std::move(_builder), _features); }

 private:
  void Parse() {
    const std::string_view type = EraseOptionOrDefault<kTemplate>();
    _builder.openObject();
    _builder.add("analyzer", vpack::Value{vpack::ValueType::Object});
    _builder.add("properties", vpack::Value{vpack::ValueType::Object});

    [&]<std::size_t... Is>(std::index_sequence<Is...>) {
      (
        [&] {
          constexpr const auto& kGroup = kTSDictionaryGroup.subgroups[Is];
          if (kGroup.name == type) {
            WriteTokenizerOptions<kGroup>();
          }
        }(),
        ...);
    }(std::make_index_sequence<std::size(kTSDictionaryGroup.subgroups)>{});

    _builder.close();  // close properties
    _builder.add("type", type);
    _builder.close();  // close analyzer
    _builder.close();  // close object
    ParseFeatures(type);
  }

  template<const OptionGroup& Group>
  void WriteTokenizerOptions() {
    if constexpr (Group.name == tokenizer_options::kTextGroup.name) {
      bool has_ngram = HasOption(tokenizer_options::kMinGram) ||
                       HasOption(tokenizer_options::kMaxGram) ||
                       HasOption(tokenizer_options::kPreserveOriginal);
      if (has_ngram) {
        _builder.add(GetVPackName(tokenizer_options::kEdgeNGramGroup.name),
                     vpack::Value{vpack::ValueType::Object});
        WriteTokenizerOptions<Group.subgroups[0]>();
        _builder.close();
      }
    }
    [&]<std::size_t... Is>(std::index_sequence<Is...>) {
      (
        [&] {
          constexpr const auto& kOption = Group.options[Is];
          auto value = EraseOptionOrDefault<kOption>();
          if constexpr (kOption.name == tokenizer_options::kStopwords.name) {
            _builder.add(GetVPackName(kOption.name),
                         vpack::Value{vpack::ValueType::Array});
            ParseCommaSeparated(
              value, [&](std::string_view word) { _builder.add(word); });
            _builder.close();
          } else {
            if constexpr (std::is_same_v<std::remove_cvref_t<decltype(value)>,
                                         std::string_view>) {
              if (value.empty()) {
                return;
              }
            }
            _builder.add(GetVPackName(kOption.name), value);
          }
        }(),
        ...);
    }(std::make_index_sequence<std::size(Group.options)>{});
  }

  void ParseFeatures(std::string_view type) {
    [&]<std::size_t... Is>(std::index_sequence<Is...>) {
      (
        [&] {
          constexpr const auto& kFeature =
            tokenizer_options::kFeaturesOptions[Is];
          bool use_feature = EraseOptionOrDefault<kFeature>();
          if (use_feature) {
            bool added = _features.Add(kFeature.name);
            SDB_ASSERT(added);
          }
        }(),
        ...);
    }(std::make_index_sequence<std::size(
        tokenizer_options::kFeaturesOptions)>{});
    auto r = _features.Validate(type);
    if (!r.ok()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG(r.errorMessage()));
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
                    ERR_MSG("text search dictionary \"", dict_name.relation,
                            "\" already exists"));
  }
  return {};
}

}  // namespace sdb::pg
