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

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <unicode/locid.h>

#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/geo_tokenizer.hpp>
#include <iresearch/analysis/icu_text_tokenizer.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/analysis/multi_delimited_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/path_hierarchy_tokenizer.hpp>
#include <iresearch/analysis/pattern_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/solr_synonyms_tokenizer.hpp>
#include <iresearch/analysis/sparse_ngram_tokenizer.hpp>
#include <iresearch/analysis/split_by_non_alpha_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/stopwords_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/analysis/wildcard_tokenizer.hpp>
#include <iresearch/analysis/wordnet_synonyms_tokenizer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/icu_locale_serde.hpp>
#include <iresearch/utils/misc.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/tokenizer.h"
#include "pg/commands/tsdictionary_spec.h"
#include "pg/connection_context.h"
#include "pg/option_help.h"
#include "pg/options_parser.h"
#include "pg/sql_utils.h"
#include "pg/tokenizer_options.h"
#include "search/search_analyzer_impl.h"

namespace sdb::pg {
namespace {

using namespace std::string_view_literals;

template<const auto& Array>
void VisitValues(auto&& callback) {
  [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    (callback.template operator()<Array[Is]>(), ...);
  }(std::make_index_sequence<std::size(Array)>{});
}

void ParseCommaSeparated(std::string_view input,
                         std::invocable<std::string_view> auto&& callback) {
  for (std::string_view token :
       absl::StrSplit(input, ',', absl::SkipWhitespace())) {
    token = absl::StripAsciiWhitespace(token);
    if (token.size() < 2 || token.front() != '\"' || token.back() != '\"') {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("Invalid format of list of words(should be "
                              "comma-separated and quoted)"));
    }
    token = token.substr(1, token.size() - 2);
    if (!token.empty()) {
      callback(token);
    }
  }
}

constexpr OptionInfo kTSDictionaryRootOptions[] = {
  tokenizer_options::kTemplate};
constexpr OptionGroup kTSDictionaryGroup = {
  "Text Search Dictionary", kTSDictionaryRootOptions,
  tokenizer_options::kTokenizerSubgroups};

std::string_view TypeNameOf(const irs::analysis::TokenizerConfig& cfg) {
  return std::visit(
    [](const auto& opts) {
      using Options = std::decay_t<decltype(opts)>;
      return Options::Owner::type_name();
    },
    cfg.config);
}

constexpr std::string_view kCreateOperation = "CREATE TEXT SEARCH DICTIONARY";

class CreateTSDictionaryOptions : public OptionsParser {
 public:
  CreateTSDictionaryOptions(duckdb::ClientContext& context, ObjectId db_id,
                            std::string_view current_schema,
                            std::string_view type, Options options,
                            TokenizerConfigs children,
                            std::string_view operation = kCreateOperation)
    : OptionsParser{std::move(options),
                    kTSDictionaryGroup,
                    {.operation = operation,
                     .help_hint = operation == kCreateOperation
                                    ? "Use WITH (HELP) to see available options"
                                    : ""}},
      _context{context},
      _db_id{db_id},
      _current_schema{current_schema},
      _children{std::move(children)} {
    ParseOptions([&] { BuildChild(type, _config); });
  }

  irs::analysis::TokenizerConfig Result() && { return std::move(_config); }

 private:
  TokenizerConfigs TakeChildren() { return std::move(_children); }

  template<const OptionInfo& Info, typename T = OptionInfo::CppType<Info.type>>
  T Value() {
    return OptionsParser::EraseOptionOrDefault<Info>();
  }

  template<const OptionInfo& Info>
  duckdb::Value EraseValue() {
    auto entry = OptionsParser::EraseOption(Info, /*requires_parameter=*/true);
    SDB_ASSERT(entry && *entry);
    return std::move(**entry);
  }

  static bool ForEachListValue(
    const duckdb::Value& value,
    std::invocable<std::string_view> auto&& callback) {
    const auto id = value.type().id();
    if (id != duckdb::LogicalTypeId::LIST &&
        id != duckdb::LogicalTypeId::ARRAY) {
      return false;
    }
    const auto& children = id == duckdb::LogicalTypeId::LIST
                             ? duckdb::ListValue::GetChildren(value)
                             : duckdb::ArrayValue::GetChildren(value);
    for (const auto& item : children) {
      if (item.IsNull()) {
        continue;
      }
      const auto text = item.DefaultCastAs(duckdb::LogicalType::VARCHAR)
                          .GetValue<std::string>();
      if (!text.empty()) {
        callback(std::string_view{text});
      }
    }
    return true;
  }

  template<const OptionInfo& Info>
  void ForEachListItem(std::invocable<std::string_view> auto&& callback) {
    const duckdb::Value value = EraseValue<Info>();
    if (ForEachListValue(value, callback)) {
      return;
    }
    ParseCommaSeparated(
      value.DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>(),
      callback);
  }

  template<const OptionInfo& Info>
  std::vector<std::string> PathSegments() {
    std::vector<std::string> segments;
    const duckdb::Value value = EraseValue<Info>();
    if (ForEachListValue(value, [&](std::string_view segment) {
          segments.emplace_back(segment);
        })) {
      return segments;
    }
    const auto path =
      value.DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>();
    return absl::StrSplit(path, '/', absl::SkipEmpty());
  }

  template<const OptionInfo& Info, typename Field>
  void ResolveStringInto(Field& field) {
    const auto assign = [](Field& f, std::string&& s) {
      if constexpr (std::is_same_v<Field, std::string>) {
        f = std::move(s);
      } else {
        f.assign(reinterpret_cast<const typename Field::value_type*>(s.data()),
                 s.size());
      }
    };
    if (OptionsParser::HasOption(Info.name)) {
      auto raw = OptionsParser::EraseOptionOrDefault<Info>();
      if (!raw.empty()) {
        assign(field, std::move(raw));
      }
      return;
    }
    auto def = Info.GetDefaultValue<std::string>();
    if (!def.empty()) {
      assign(field, std::move(def));
    }
  }

  template<const OptionInfo& Info>
  icu::Locale ResolveLocale() {
    if (OptionsParser::HasOption(Info.name)) {
      auto raw = OptionsParser::EraseOptionOrDefault<Info>();
      if (raw.empty()) {
        return irs::MakeBogusLocale();
      }
      auto loc = icu::Locale::createFromName(raw.c_str());
      if (loc.isBogus()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("Invalid locale \"", raw, "\" for option \"",
                                Info.name, "\""));
      }
      return loc;
    }
    return irs::MakeBogusLocale();
  }

  template<const OptionInfo& Info, typename Enum>
  Enum ResolveEnum() {
    if (OptionsParser::HasOption(Info.name)) {
      auto raw = OptionsParser::EraseOptionOrDefault<Info>();
      auto parsed =
        magic_enum::enum_cast<Enum>(raw, magic_enum::case_insensitive);
      SDB_ASSERT(parsed.has_value());
      return *parsed;
    }
    auto default_view = Info.GetDefaultValue<std::string>();
    auto parsed =
      magic_enum::enum_cast<Enum>(default_view, magic_enum::case_insensitive);
    SDB_ASSERT(parsed.has_value());
    return *parsed;
  }

  irs::KeywordTokenizer::Options BuildKeyword() { return {}; }

  irs::analysis::StemmingTokenizer::Options BuildStem() {
    irs::analysis::StemmingTokenizer::Options opts;
    opts.locale = ResolveLocale<tokenizer_options::kLocale>();
    return opts;
  }

  irs::analysis::CollationTokenizer::Options BuildCollation() {
    irs::analysis::CollationTokenizer::Options opts;
    opts.locale = ResolveLocale<tokenizer_options::kLocale>();
    return opts;
  }

  irs::analysis::NormalizingTokenizer::Options BuildNormalizing() {
    irs::analysis::NormalizingTokenizer::Options opts;
    opts.locale = ResolveLocale<tokenizer_options::kNormLocale>();
    opts.case_convert = ResolveEnum<tokenizer_options::kCase, irs::Case>();
    opts.accent = Value<tokenizer_options::kAccent>();
    opts.form =
      ResolveEnum<tokenizer_options::kForm, irs::analysis::NormForm>();
    return opts;
  }

  irs::analysis::DelimitedTokenizer::Options BuildDelimiter() {
    irs::analysis::DelimitedTokenizer::Options opts;
    opts.delimiter =
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kDelimiter>();
    return opts;
  }

  irs::analysis::MultiDelimitedTokenizer::Options BuildMultiDelimiter() {
    irs::analysis::MultiDelimitedTokenizer::Options opts;
    ForEachListItem<tokenizer_options::kDelimiters>([&](std::string_view d) {
      opts.delimiters.emplace_back(irs::ViewCast<irs::byte_type>(d));
    });
    return opts;
  }

  irs::analysis::PatternTokenizer::Options BuildPattern() {
    irs::analysis::PatternTokenizer::Options opts;
    opts.pattern =
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kPattern>();
    opts.group = Value<tokenizer_options::kGroup>();
    return opts;
  }

  irs::analysis::SqlTokenizer::Options BuildSql() {
    irs::analysis::SqlTokenizer::Options opts;
    opts.expression =
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kSqlExpression>();
    return opts;
  }

  irs::analysis::PathHierarchyTokenizer::Options BuildPathHierarchy() {
    irs::analysis::PathHierarchyTokenizer::Options opts;
    ResolveStringInto<tokenizer_options::kPathDelimiter>(opts.delimiter);
    // `replacement` defaults to the delimiter (separators kept verbatim); the
    // option's own default is empty, so it only overrides when given.
    opts.replacement = opts.delimiter;
    ResolveStringInto<tokenizer_options::kPathReplacement>(opts.replacement);
    opts.reverse = Value<tokenizer_options::kReverse>();
    opts.skip = static_cast<size_t>(Value<tokenizer_options::kSkip>());
    return opts;
  }

  irs::analysis::NGramTokenizer::Options BuildNGram() {
    using IT = irs::analysis::NGramTokenizer::InputType;
    using NM = irs::analysis::NGramTokenizer::NGramMode;
    irs::analysis::NGramTokenizer::Options opts;
    opts.min_gram = static_cast<size_t>(Value<tokenizer_options::kMinGram>());
    opts.max_gram = static_cast<size_t>(Value<tokenizer_options::kMaxGram>());
    opts.preserve_original = Value<tokenizer_options::kPreserveOriginal>();
    opts.stream_bytes_type = ResolveEnum<tokenizer_options::kInputType, IT>();
    ResolveStringInto<tokenizer_options::kStartMarker>(opts.start_marker);
    ResolveStringInto<tokenizer_options::kEndMarker>(opts.end_marker);
    opts.ngram_mode = ResolveEnum<tokenizer_options::kMode, NM>();
    return opts;
  }

  irs::analysis::SparseNGramTokenizer::Options BuildSparseNGram() {
    irs::analysis::SparseNGramTokenizer::Options opts;
    opts.max_ngram_length = Value<tokenizer_options::kMaxNGramLength>();
    opts.covering = Value<tokenizer_options::kCovering>();
    return opts;
  }

  irs::analysis::TextTokenizer::Options BuildText() {
    using Opts = irs::analysis::TextTokenizer::Options;
    Opts opts;
    if (OptionsParser::HasOption(tokenizer_options::kBreak)) {
      auto raw =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kBreak>();
      const auto accept =
        magic_enum::enum_cast<Opts::Accept>(raw, magic_enum::case_insensitive);
      const auto separate = magic_enum::enum_cast<Opts::Separate>(
        raw, magic_enum::case_insensitive);
      if (accept) {
        opts.accept = *accept;
      } else if (separate && *separate != Opts::Separate::Word &&
                 *separate != Opts::Separate::None) {
        opts.separate = *separate;
        opts.accept = Opts::Accept::Any;
      } else {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("invalid value in \"break\" parameter"),
                        ERR_HINT(tokenizer_options::kBreak.description));
      }
    } else {
      opts.accept = Opts::Accept::AlphaNumeric;
    }

    opts.convert = ResolveEnum<tokenizer_options::kCase, irs::Case>();
    return opts;
  }

  irs::analysis::IcuTextTokenizer::Options BuildIcuText() {
    using Opts = irs::analysis::IcuTextTokenizer::Options;
    Opts opts;
    opts.locale = ResolveLocale<tokenizer_options::kIcuTextLocale>();
    if (OptionsParser::HasOption(tokenizer_options::kIcuTextBreak)) {
      auto raw =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kIcuTextBreak>();
      const auto accept =
        magic_enum::enum_cast<Opts::Accept>(raw, magic_enum::case_insensitive);
      const auto separate = magic_enum::enum_cast<Opts::Separate>(
        raw, magic_enum::case_insensitive);
      if (accept) {
        opts.accept = *accept;
      } else if (separate && *separate != Opts::Separate::Word) {
        opts.separate = *separate;
        opts.accept = Opts::Accept::Any;
      } else {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("invalid value in \"break\" parameter"),
                        ERR_HINT(tokenizer_options::kIcuTextBreak.description));
      }
    } else {
      opts.accept = Opts::Accept::AlphaNumeric;
    }
    return opts;
  }

  irs::analysis::StopwordsTokenizer::Options BuildStopwords() {
    irs::analysis::StopwordsTokenizer::Options opts;
    const bool hex = Value<tokenizer_options::kHex>();
    if (OptionsParser::HasOption(tokenizer_options::kStopwords)) {
      ForEachListItem<tokenizer_options::kStopwords>([&](std::string_view w) {
        if (!hex) {
          opts.mask.emplace_back(w);
          return;
        }
        std::string decoded;
        if (!absl::HexStringToBytes(w, &decoded)) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG("invalid hex stopword"));
        }
        opts.mask.emplace_back(std::move(decoded));
      });
    }
    ResolveStringInto<tokenizer_options::kStopwordsPath>(opts.stopwords_path);
    return opts;
  }

  irs::analysis::SplitByNonAlphaTokenizer::Options BuildSplitByNonAlpha() {
    irs::analysis::SplitByNonAlphaTokenizer::Options opts;
    opts.case_convert = ResolveEnum<tokenizer_options::kCase, irs::Case>();
    return opts;
  }

  irs::analysis::ClassificationTokenizer::Options BuildClassification() {
    irs::analysis::ClassificationTokenizer::Options opts;
    ResolveStringInto<tokenizer_options::kModelLocation>(opts.model_location);
    opts.threshold = Value<tokenizer_options::kThreshold>();
    opts.top_k = Value<tokenizer_options::kTopK>();
    return opts;
  }

  irs::analysis::NearestNeighborsTokenizer::Options BuildNearestNeighbors() {
    irs::analysis::NearestNeighborsTokenizer::Options opts;
    ResolveStringInto<tokenizer_options::kModelLocation>(opts.model_location);
    opts.top_k = Value<tokenizer_options::kTopK>();
    return opts;
  }

  irs::analysis::SolrSynonymsTokenizer::Options BuildSolrSynonyms() {
    irs::analysis::SolrSynonymsTokenizer::Options opts;
    opts.synonyms_text =
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kSolrSynonyms>();
    return opts;
  }

  irs::analysis::WordnetSynonymsTokenizer::Options BuildWordnetSynonyms() {
    irs::analysis::WordnetSynonymsTokenizer::Options opts;
    opts.synonyms_text = OptionsParser::EraseOptionOrDefault<
      tokenizer_options::kWordnetSynonyms>();
    return opts;
  }

  void ResolveGeoS2(irs::geo::GeoOptions& opts) {
    opts.max_cells = Value<tokenizer_options::kGeoMaxCells>();
    opts.min_level = Value<tokenizer_options::kGeoMinLevel>();
    opts.max_level = Value<tokenizer_options::kGeoMaxLevel>();
    opts.level_mod =
      static_cast<int8_t>(Value<tokenizer_options::kGeoLevelMod>());
    opts.optimize_for_space = Value<tokenizer_options::kGeoOptimizeForSpace>();
  }

  irs::analysis::GeoPointTokenizer::Options BuildGeoPoint() {
    irs::analysis::GeoPointTokenizer::Options opts;
    bool lat_set = false;
    bool lng_set = false;
    if (OptionsParser::HasOption(tokenizer_options::kGeoLatitude)) {
      opts.latitude = PathSegments<tokenizer_options::kGeoLatitude>();
      lat_set = !opts.latitude.empty();
    }
    if (OptionsParser::HasOption(tokenizer_options::kGeoLongitude)) {
      opts.longitude = PathSegments<tokenizer_options::kGeoLongitude>();
      lng_set = !opts.longitude.empty();
    }
    if (lat_set != lng_set) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("'latitude' and 'longitude' must be both set or both "
                "left empty for the geopoint tokenizer"));
    }
    ResolveGeoS2(opts.options);
    return opts;
  }

  irs::analysis::GeoJsonTokenizer::Options BuildGeoJson() {
    using GJ = irs::analysis::GeoJsonTokenizer;
    irs::analysis::GeoJsonTokenizer::Options opts;
    opts.type = ResolveEnum<tokenizer_options::kGeoJsonType, GJ::Type>();
    opts.coding = ResolveEnum<tokenizer_options::kGeoJsonCoding, GJ::Coding>();
    ResolveGeoS2(opts.options);
    return opts;
  }

  template<typename Opts>
  Opts BuildComposite() {
    return Opts{.children = TakeChildren()};
  }

  std::unique_ptr<irs::analysis::TokenizerConfig> TakeBaseAnalyzer() {
    auto children = TakeChildren();
    SDB_ASSERT(children.size() == 1);
    return std::move(children.front());
  }

  irs::analysis::WildcardTokenizer::Options BuildWildcard() {
    irs::analysis::WildcardTokenizer::Options opts;
    opts.base_analyzer = TakeBaseAnalyzer();
    opts.ngram_size =
      static_cast<size_t>(Value<tokenizer_options::kNGramSize>());
    return opts;
  }

  irs::analysis::ShingleTokenizer::Options BuildShingle() {
    irs::analysis::ShingleTokenizer::Options opts;
    opts.base_analyzer = TakeBaseAnalyzer();
    opts.min_shingle_size =
      static_cast<uint32_t>(Value<tokenizer_options::kMinShingleSize>());
    opts.max_shingle_size =
      static_cast<uint32_t>(Value<tokenizer_options::kMaxShingleSize>());
    if (opts.max_shingle_size < opts.min_shingle_size) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("\"max_gram\" must be >= \"min_gram\""));
    }
    opts.output_unigrams = Value<tokenizer_options::kOutputUnigrams>();
    opts.fallback_unigrams =
      Value<tokenizer_options::kOutputUnigramsIfNoShingles>();
    opts.store_tokens = Value<tokenizer_options::kStoreTokens>();
    if (OptionsParser::HasOption(tokenizer_options::kFrequentWords)) {
      ForEachListItem<tokenizer_options::kFrequentWords>(
        [&](std::string_view w) {
          opts.frequent_words.emplace_back(
            reinterpret_cast<const irs::byte_type*>(w.data()), w.size());
        });
    }
    if (!opts.store_tokens && !opts.frequent_words.empty()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("\"store_tokens\" = false cannot be combined with "
                "\"frequent_words\""),
        ERR_HINT("Without the stored token stream every shingle size must be "
                 "dense so phrases up to max_gram stay exact."));
    }
    ResolveStringInto<tokenizer_options::kFillerToken>(opts.filler_token);
    if (OptionsParser::HasOption(tokenizer_options::kTokenSeparator)) {
      const auto raw = OptionsParser::EraseOptionOrDefault<
        tokenizer_options::kTokenSeparator>();
      opts.token_separator.assign(
        reinterpret_cast<const irs::byte_type*>(raw.data()), raw.size());
    }
    return opts;
  }

  void BuildChild(std::string_view type, irs::analysis::TokenizerConfig& out) {
    if (type == tokenizer_options::kDictionaryTemplate) {
      BuildDictionary(out);
      return;
    }
    Dispatch(type, out);
  }

  template<auto Builder>
  static void Build(CreateTSDictionaryOptions& self,
                    irs::analysis::TokenizerConfig& cfg) {
    cfg.config = (self.*Builder)();
  }

  void Dispatch(std::string_view type, irs::analysis::TokenizerConfig& out) {
    using namespace irs::analysis;
    using Self = CreateTSDictionaryOptions;
    using Builder = void (*)(Self&, TokenizerConfig&);
    static const irs::containers::FlatHashMap<std::string_view, Builder>
      kBuilders{
        {NGramTokenizer::type_name(), &Build<&Self::BuildNGram>},
        {SparseNGramTokenizer::type_name(), &Build<&Self::BuildSparseNGram>},
        {NearestNeighborsTokenizer::type_name(),
         &Build<&Self::BuildNearestNeighbors>},
        {StemmingTokenizer::type_name(), &Build<&Self::BuildStem>},
        {StopwordsTokenizer::type_name(), &Build<&Self::BuildStopwords>},
        {ClassificationTokenizer::type_name(),
         &Build<&Self::BuildClassification>},
        {CollationTokenizer::type_name(), &Build<&Self::BuildCollation>},
        {SplitByNonAlphaTokenizer::type_name(),
         &Build<&Self::BuildSplitByNonAlpha>},
        {DelimitedTokenizer::type_name(), &Build<&Self::BuildDelimiter>},
        {MultiDelimitedTokenizer::type_name(),
         &Build<&Self::BuildMultiDelimiter>},
        {WildcardTokenizer::type_name(), &Build<&Self::BuildWildcard>},
        {NormalizingTokenizer::type_name(), &Build<&Self::BuildNormalizing>},
        {TextTokenizer::type_name(), &Build<&Self::BuildText>},
        {IcuTextTokenizer::type_name(), &Build<&Self::BuildIcuText>},
        {PipelineTokenizer::type_name(),
         &Build<&Self::BuildComposite<PipelineTokenizer::Options>>},
        {PatternTokenizer::type_name(), &Build<&Self::BuildPattern>},
        {SqlTokenizer::type_name(), &Build<&Self::BuildSql>},
        {ShingleTokenizer::type_name(), &Build<&Self::BuildShingle>},
        {PathHierarchyTokenizer::type_name(),
         &Build<&Self::BuildPathHierarchy>},
        {UnionTokenizer::type_name(),
         &Build<&Self::BuildComposite<UnionTokenizer::Options>>},
        {GeoPointTokenizer::type_name(), &Build<&Self::BuildGeoPoint>},
        {GeoJsonTokenizer::type_name(), &Build<&Self::BuildGeoJson>},
        {irs::KeywordTokenizer::type_name(), &Build<&Self::BuildKeyword>},
        {SolrSynonymsTokenizer::type_name(), &Build<&Self::BuildSolrSynonyms>},
        {WordnetSynonymsTokenizer::type_name(),
         &Build<&Self::BuildWordnetSynonyms>},
      };
    const auto it = kBuilders.find(type);
    SDB_ASSERT(it != kBuilders.end());
    it->second(*this, out);
  }

  void BuildDictionary(irs::analysis::TokenizerConfig& out) {
    std::string from = Value<tokenizer_options::kFrom>();
    auto name = ParseObjectName(from, _current_schema);
    const auto schema_id =
      catalog::FindSchemaId(&_context, _db_id, name.schema);
    auto tokenizer = schema_id.isSet() ? catalog::FindTokenizer(
                                           &_context, schema_id, name.relation)
                                       : nullptr;
    if (!tokenizer) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("text search dictionary \"", from, "\" does not exist"));
    }
    out = irs::analysis::Clone(tokenizer->Config());
  }

  irs::analysis::TokenizerConfig _config;
  duckdb::ClientContext& _context;
  ObjectId _db_id;
  std::string_view _current_schema;
  TokenizerConfigs _children;
};

// The WITH clause carries the feature flags and nothing else.
class FeatureOptions : public OptionsParser {
 public:
  explicit FeatureOptions(const duckdb::named_parameter_map_t& with)
    : OptionsParser{
        ConvertMap(with), kTSDictionaryGroup, {.operation = kCreateOperation}} {
    ParseOptions([&] {
      VisitValues<tokenizer_options::kFeaturesOptions>(
        [&]<const OptionInfo & Feature> {
          if (EraseOptionOrDefault<Feature>()) {
            const bool added = _features.Add(Feature.name);
            SDB_ASSERT(added);
          }
        });
    });
  }

  search::Features Take(std::string_view type) && {
    _features.Validate(type);
    return std::move(_features);
  }

 private:
  search::Features _features;
};

search::Features ParseFeatures(const duckdb::named_parameter_map_t& with,
                               std::string_view type) {
  return std::move(FeatureOptions{with}).Take(type);
}

void CheckWithClause(const duckdb::named_parameter_map_t& with) {
  auto options = OptionsParser::ConvertMap(with);
  if (options.contains("help")) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("\n", FormatTSDictionaryHelp()));
  }
  for (const auto& [name, _] : options) {
    if (name == tokenizer_options::kTemplate.name) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("option \"template\" is set by the expression"));
    }
    const bool feature =
      absl::c_any_of(tokenizer_options::kFeaturesOptions,
                     [&](const OptionInfo& info) { return info.name == name; });
    if (!feature) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("option \"", name, "\" is not applicable after AS"),
        ERR_HINT("Set analyzer options in the expression; WITH (...) takes "
                 "the feature flags frequency, position, norm and offset"));
    }
  }
}

}  // namespace

irs::analysis::TokenizerConfig BuildStage(
  duckdb::ClientContext& context, ObjectId db_id,
  std::string_view current_schema, std::string_view type, Options options,
  TokenizerConfigs children, std::string_view operation) {
  return std::move(CreateTSDictionaryOptions{context, db_id, current_schema,
                                             type, std::move(options),
                                             std::move(children), operation})
    .Result();
}

void CreateTokenizer(ConnectionContext& conn_ctx, std::string_view name,
                     std::string_view schema, bool if_not_exists,
                     const duckdb::named_parameter_map_t& with,
                     std::string_view spec) {
  auto db_id = conn_ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();
  auto& client_ctx = conn_ctx.GetClientContext();

  CheckWithClause(with);
  auto cfg = CompileTSDictionarySpec(client_ctx, db_id, current_schema, spec);
  auto features = ParseFeatures(with, TypeNameOf(cfg));

  auto test_analyzer = irs::analysis::CreateTokenizer(
    irs::analysis::Clone(cfg),
    duckdb::DatabaseInstance::GetDatabase(client_ctx).GetSharedObjectCache());
  SDB_ASSERT(test_analyzer);
  test_analyzer->Bind(client_ctx);
  irs::Finally unbind = [&]() noexcept { test_analyzer->Unbind(); };

  if (features.HasFeatures(irs::IndexFeatures::Offs) &&
      !test_analyzer->Traits().offsets) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Unsupported index features are specified"));
  }

  if (features.HasFeatures(irs::IndexFeatures::Norm) &&
      test_analyzer->Traits().store) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("the 'norm' feature cannot be combined with an analyzer that "
              "stores a per-document blob"),
      ERR_HINT("norm and the stored blob share one synthetic column; disable "
               "the analyzer's token storage or drop the 'norm' feature."));
  }

  auto tokenizer = std::make_shared<catalog::CreateTokenizerInfo>(
    ObjectId{}, ObjectId{}, name, features, std::move(cfg));

  auto& catalog = catalog::DatabaseCatalog(&conn_ctx.GetClientContext(), db_id);
  catalog.CreateTokenizer(
    catalog::ActingAs(conn_ctx.GetRoleId(), conn_ctx.GetClientContext()), db_id,
    schema, std::move(tokenizer), if_not_exists);
}

}  // namespace sdb::pg
