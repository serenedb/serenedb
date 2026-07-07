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

#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <unicode/locid.h>

#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/minhash_tokenizer.hpp>
#include <iresearch/analysis/multi_delimited_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/path_hierarchy_tokenizer.hpp>
#include <iresearch/analysis/pattern_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/solr_synonyms_tokenizer.hpp>
#include <iresearch/analysis/sparse_ngram_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/stopwords_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/analysis/wordnet_synonyms_tokenizer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <iresearch/utils/icu_locale_serde.hpp>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"
#include "pg/connection_context.h"
#include "pg/option_help.h"
#include "pg/options_parser.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "pg/tokenizer_options.h"

namespace magic_enum {

template<>
constexpr customize::customize_t
customize::enum_name<irs::analysis::SegmentationTokenizer::Options::Accept>(
  irs::analysis::SegmentationTokenizer::Options::Accept value) noexcept {
  using Accept = irs::analysis::SegmentationTokenizer::Options::Accept;
  switch (value) {
    case Accept::Any:
      return "all";
    case Accept::Graphic:
      return "graphic";
    case Accept::AlphaNumeric:
      return "alpha";
    case Accept::Alpha:
      return invalid_tag;
  }
  return invalid_tag;
}

}  // namespace magic_enum
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

class CreateTSDictionaryOptions : public OptionsParser {
 public:
  CreateTSDictionaryOptions(std::shared_ptr<const catalog::Snapshot> snapshot,
                            ObjectId db_id, std::string_view current_schema,
                            const duckdb::named_parameter_map_t& named_params)
    : OptionsParser{named_params,
                    kTSDictionaryGroup,
                    {.operation = "CREATE TEXT SEARCH DICTIONARY",
                     .help_hint = "Use WITH (HELP) to see available options"}},
      _snapshot{std::move(snapshot)},
      _db_id{db_id},
      _current_schema{current_schema} {
    ParseOptions([&] {
      const auto type =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>();
      BuildChild(type, /*prefix=*/"", /*parent_cfg=*/nullptr, _config);
      ParseFeatures(TypeNameOf(_config));
    });
  }

  auto Result() && {
    return std::make_tuple(std::move(_config), std::move(_features),
                           _norm_row_group_size);
  }

 private:
  bool HasUnionChildOption(std::string_view prefix) const {
    auto child_prefix = OptionInfo::AdjustPrefix(prefix, "tokenizer");
    for (const auto& [name, _] : _options) {
      if (name.starts_with(child_prefix)) {
        return true;
      }
    }
    return false;
  }

  template<const OptionInfo& Info, typename T = OptionInfo::CppType<Info.type>>
  T Resolve(std::string_view prefix, const T* parent_field) {
    if (OptionsParser::HasOption(Info.name, prefix)) {
      return OptionsParser::EraseOptionOrDefault<Info>(prefix);
    }
    if (parent_field) {
      return *parent_field;
    }
    return Info.GetDefaultValue<T>();
  }

  template<const OptionInfo& Info, typename Field>
  void ResolveStringInto(std::string_view prefix, Field& field,
                         const Field* parent_field) {
    const auto assign = [](Field& f, std::string&& s) {
      if constexpr (std::is_same_v<Field, std::string>) {
        f = std::move(s);
      } else {
        f.assign(reinterpret_cast<const typename Field::value_type*>(s.data()),
                 s.size());
      }
    };
    if (OptionsParser::HasOption(Info.name, prefix)) {
      auto raw = OptionsParser::EraseOptionOrDefault<Info>(prefix);
      if (!raw.empty()) {
        assign(field, std::move(raw));
      }
      return;
    }
    if (parent_field) {
      field = *parent_field;
      return;
    }
    auto def = Info.GetDefaultValue<std::string>();
    if (!def.empty()) {
      assign(field, std::move(def));
    }
  }

  template<const OptionInfo& Info>
  icu::Locale ResolveLocale(std::string_view prefix,
                            const icu::Locale* parent_locale) {
    if (OptionsParser::HasOption(Info.name, prefix)) {
      auto raw = OptionsParser::EraseOptionOrDefault<Info>(prefix);
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
    if (parent_locale) {
      return *parent_locale;
    }
    return irs::MakeBogusLocale();
  }

  template<const OptionInfo& Info, typename Enum>
  Enum ResolveEnum(std::string_view prefix, const Enum* parent_value) {
    if (OptionsParser::HasOption(Info.name, prefix)) {
      auto raw = OptionsParser::EraseOptionOrDefault<Info>(prefix);
      auto parsed =
        magic_enum::enum_cast<Enum>(raw, magic_enum::case_insensitive);
      SDB_ASSERT(parsed.has_value());
      return *parsed;
    }
    if (parent_value) {
      return *parent_value;
    }
    auto default_view = Info.GetDefaultValue<std::string>();
    auto parsed =
      magic_enum::enum_cast<Enum>(default_view, magic_enum::case_insensitive);
    SDB_ASSERT(parsed.has_value());
    return *parsed;
  }

  irs::StringTokenizer::Options BuildKeyword(
    std::string_view /*prefix*/,
    const irs::StringTokenizer::Options* /*parent*/) {
    return {};
  }

  irs::analysis::TextTokenizer::Options BuildText(
    std::string_view prefix,
    const irs::analysis::TextTokenizer::Options* parent) {
    irs::analysis::TextTokenizer::Options opts;
    opts.locale = ResolveLocale<tokenizer_options::kLocale>(
      prefix, parent ? &parent->locale : nullptr);
    opts.case_convert = ResolveEnum<tokenizer_options::kCase, irs::Case>(
      prefix, parent ? &parent->case_convert : nullptr);
    opts.accent = Resolve<tokenizer_options::kAccent>(
      prefix, parent ? &parent->accent : nullptr);
    opts.stemming = Resolve<tokenizer_options::kStemming>(
      prefix, parent ? &parent->stemming : nullptr);

    if (OptionsParser::HasOption(tokenizer_options::kStopwords, prefix)) {
      auto raw =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kStopwords>(
          prefix);
      ParseCommaSeparated(
        raw, [&](std::string_view w) { opts.explicit_stopwords.emplace(w); });
      opts.explicit_stopwords_set = true;
    } else if (parent) {
      opts.explicit_stopwords = parent->explicit_stopwords;
      opts.explicit_stopwords_set = parent->explicit_stopwords_set;
    } else {
      opts.explicit_stopwords_set = true;
    }

    ResolveStringInto<tokenizer_options::kStopwordsPath>(
      prefix, opts.stopwords_path, parent ? &parent->stopwords_path : nullptr);

    auto resolve_size_field = [&]<const OptionInfo & Info>(size_t parent_field,
                                                           bool parent_set) {
      if (OptionsParser::HasOption(Info.name, prefix)) {
        return static_cast<size_t>(
          OptionsParser::EraseOptionOrDefault<Info>(prefix));
      }
      if (parent && parent_set) {
        return parent_field;
      }
      return static_cast<size_t>(Info.template GetDefaultValue<int>());
    };
    const bool any_ngram_sql =
      OptionsParser::HasOption(tokenizer_options::kMinGram, prefix) ||
      OptionsParser::HasOption(tokenizer_options::kMaxGram, prefix) ||
      OptionsParser::HasOption(tokenizer_options::kPreserveOriginal, prefix);
    const bool parent_ngram =
      parent && (parent->min_gram_set || parent->max_gram_set ||
                 parent->preserve_original_set);
    if (any_ngram_sql || parent_ngram) {
      opts.min_gram =
        resolve_size_field.template operator()<tokenizer_options::kMinGram>(
          parent ? parent->min_gram : 0, parent && parent->min_gram_set);
      opts.max_gram =
        resolve_size_field.template operator()<tokenizer_options::kMaxGram>(
          parent ? parent->max_gram : 0, parent && parent->max_gram_set);
      if (OptionsParser::HasOption(tokenizer_options::kPreserveOriginal,
                                   prefix)) {
        opts.preserve_original = OptionsParser::EraseOptionOrDefault<
          tokenizer_options::kPreserveOriginal>(prefix);
      } else if (parent && parent->preserve_original_set) {
        opts.preserve_original = parent->preserve_original;
      } else {
        opts.preserve_original =
          tokenizer_options::kPreserveOriginal.GetDefaultValue<bool>();
      }
      opts.min_gram_set = true;
      opts.max_gram_set = true;
      opts.preserve_original_set = true;
    }
    return opts;
  }

  irs::analysis::StemmingTokenizer::Options BuildStem(
    std::string_view prefix,
    const irs::analysis::StemmingTokenizer::Options* parent) {
    irs::analysis::StemmingTokenizer::Options opts;
    opts.locale = ResolveLocale<tokenizer_options::kLocale>(
      prefix, parent ? &parent->locale : nullptr);
    return opts;
  }

  irs::analysis::CollationTokenizer::Options BuildCollation(
    std::string_view prefix,
    const irs::analysis::CollationTokenizer::Options* parent) {
    irs::analysis::CollationTokenizer::Options opts;
    opts.locale = ResolveLocale<tokenizer_options::kLocale>(
      prefix, parent ? &parent->locale : nullptr);
    return opts;
  }

  irs::analysis::NormalizingTokenizer::Options BuildNormalizing(
    std::string_view prefix,
    const irs::analysis::NormalizingTokenizer::Options* parent) {
    irs::analysis::NormalizingTokenizer::Options opts;
    opts.locale = ResolveLocale<tokenizer_options::kLocale>(
      prefix, parent ? &parent->locale : nullptr);
    opts.case_convert = ResolveEnum<tokenizer_options::kCase, irs::Case>(
      prefix, parent ? &parent->case_convert : nullptr);
    opts.accent = Resolve<tokenizer_options::kAccent>(
      prefix, parent ? &parent->accent : nullptr);
    return opts;
  }

  irs::analysis::DelimitedTokenizer::Options BuildDelimiter(
    std::string_view prefix,
    const irs::analysis::DelimitedTokenizer::Options* parent) {
    irs::analysis::DelimitedTokenizer::Options opts;
    if (OptionsParser::HasOption(tokenizer_options::kDelimiter, prefix)) {
      opts.delimiter =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kDelimiter>(
          prefix);
    } else if (parent) {
      opts.delimiter = parent->delimiter;
    } else {
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kDelimiter>(
        prefix);  // throws "required parameter not found"
    }
    return opts;
  }

  irs::analysis::MultiDelimitedTokenizer::Options BuildMultiDelimiter(
    std::string_view prefix,
    const irs::analysis::MultiDelimitedTokenizer::Options* parent) {
    irs::analysis::MultiDelimitedTokenizer::Options opts;
    if (OptionsParser::HasOption(tokenizer_options::kDelimiters, prefix)) {
      auto raw =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kDelimiters>(
          prefix);
      ParseCommaSeparated(raw, [&](std::string_view d) {
        opts.delimiters.emplace_back(irs::ViewCast<irs::byte_type>(d));
      });
    } else if (parent) {
      opts.delimiters = parent->delimiters;
    } else {
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kDelimiters>(
        prefix);  // throws
    }
    return opts;
  }

  irs::analysis::PatternTokenizer::Options BuildPattern(
    std::string_view prefix,
    const irs::analysis::PatternTokenizer::Options* parent) {
    irs::analysis::PatternTokenizer::Options opts;
    if (OptionsParser::HasOption(tokenizer_options::kPattern, prefix)) {
      opts.pattern =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kPattern>(
          prefix);
    } else if (parent) {
      opts.pattern = parent->pattern;
    } else {
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kPattern>(
        prefix);  // throws
    }
    opts.group = Resolve<tokenizer_options::kGroup>(
      prefix, parent ? &parent->group : nullptr);
    return opts;
  }

  irs::analysis::PathHierarchyTokenizer::Options BuildPathHierarchy(
    std::string_view prefix,
    const irs::analysis::PathHierarchyTokenizer::Options* parent) {
    irs::analysis::PathHierarchyTokenizer::Options opts;
    ResolveStringInto<tokenizer_options::kPathDelimiter>(
      prefix, opts.delimiter, parent ? &parent->delimiter : nullptr);
    // `replacement` defaults to the delimiter (separators kept verbatim); the
    // option's own default is empty, so it only overrides when given.
    opts.replacement = opts.delimiter;
    ResolveStringInto<tokenizer_options::kPathReplacement>(
      prefix, opts.replacement, parent ? &parent->replacement : nullptr);
    opts.reverse = Resolve<tokenizer_options::kReverse>(
      prefix, parent ? &parent->reverse : nullptr);
    int parent_skip = parent ? static_cast<int>(parent->skip) : 0;
    int parent_buf = parent ? static_cast<int>(parent->buffer_size) : 1024;
    opts.skip = static_cast<size_t>(Resolve<tokenizer_options::kSkip>(
      prefix, parent ? &parent_skip : nullptr));
    opts.buffer_size =
      static_cast<size_t>(Resolve<tokenizer_options::kBufferSize>(
        prefix, parent ? &parent_buf : nullptr));
    return opts;
  }

  irs::analysis::NGramTokenizerBase::Options BuildNGram(
    std::string_view prefix,
    const irs::analysis::NGramTokenizerBase::Options* parent) {
    using IT = irs::analysis::NGramTokenizerBase::InputType;
    irs::analysis::NGramTokenizerBase::Options opts;
    int parent_min = parent ? static_cast<int>(parent->min_gram) : 0;
    int parent_max = parent ? static_cast<int>(parent->max_gram) : 0;
    opts.min_gram = static_cast<size_t>(Resolve<tokenizer_options::kMinGram>(
      prefix, parent ? &parent_min : nullptr));
    opts.max_gram = static_cast<size_t>(Resolve<tokenizer_options::kMaxGram>(
      prefix, parent ? &parent_max : nullptr));
    opts.preserve_original = Resolve<tokenizer_options::kPreserveOriginal>(
      prefix, parent ? &parent->preserve_original : nullptr);
    opts.stream_bytes_type = ResolveEnum<tokenizer_options::kInputType, IT>(
      prefix, parent ? &parent->stream_bytes_type : nullptr);
    ResolveStringInto<tokenizer_options::kStartMarker>(
      prefix, opts.start_marker, parent ? &parent->start_marker : nullptr);
    ResolveStringInto<tokenizer_options::kEndMarker>(
      prefix, opts.end_marker, parent ? &parent->end_marker : nullptr);
    return opts;
  }

  irs::analysis::SparseNGramTokenizer::Options BuildSparseNGram(
    std::string_view prefix,
    const irs::analysis::SparseNGramTokenizer::Options* parent) {
    irs::analysis::SparseNGramTokenizer::Options opts;
    int parent_len = parent ? static_cast<int>(parent->max_ngram_length) : 0;
    opts.max_ngram_length = Resolve<tokenizer_options::kMaxNgramLength>(
      prefix, parent ? &parent_len : nullptr);
    opts.covering = Resolve<tokenizer_options::kCovering>(
      prefix, parent ? &parent->covering : nullptr);
    return opts;
  }

  irs::analysis::SegmentationTokenizer::Options BuildSegmentation(
    std::string_view prefix,
    const irs::analysis::SegmentationTokenizer::Options* parent) {
    using Opts = irs::analysis::SegmentationTokenizer::Options;
    Opts opts;
    if (OptionsParser::HasOption(tokenizer_options::kBreak, prefix)) {
      auto raw =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kBreak>(prefix);
      const auto parsed =
        magic_enum::enum_cast<Opts::Accept>(raw, magic_enum::case_insensitive);
      if (!parsed) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("invalid value in \"break\" parameter"),
                        ERR_HINT(tokenizer_options::kBreak.description));
      }
      opts.accept = *parsed;
    } else if (parent) {
      opts.accept = parent->accept;
    } else {
      opts.accept = Opts::Accept::AlphaNumeric;
    }

    opts.convert = ResolveEnum<tokenizer_options::kCase, Opts::Convert>(
      prefix, parent ? &parent->convert : nullptr);
    return opts;
  }

  irs::analysis::StopwordsTokenizer::Options BuildStopwords(
    std::string_view prefix,
    const irs::analysis::StopwordsTokenizer::Options* parent) {
    irs::analysis::StopwordsTokenizer::Options opts;
    const bool hex = Resolve<tokenizer_options::kHex>(
      prefix, static_cast<const bool*>(nullptr));
    if (OptionsParser::HasOption(tokenizer_options::kStopwords, prefix)) {
      auto raw =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kStopwords>(
          prefix);
      ParseCommaSeparated(raw, [&](std::string_view w) {
        if (!hex) {
          opts.mask.emplace(w);
          return;
        }
        std::string decoded;
        if (!absl::HexStringToBytes(w, &decoded)) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG("invalid hex stopword"));
        }
        opts.mask.emplace(std::move(decoded));
      });
    } else if (parent) {
      opts.mask = parent->mask;
    } else {
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kStopwords>(
        prefix);  // throws
    }
    return opts;
  }

  irs::analysis::ClassificationTokenizer::Options BuildClassification(
    std::string_view prefix,
    const irs::analysis::ClassificationTokenizer::Options* parent) {
    irs::analysis::ClassificationTokenizer::Options opts;
    ResolveStringInto<tokenizer_options::kModelLocation>(
      prefix, opts.model_location, parent ? &parent->model_location : nullptr);
    opts.threshold = Resolve<tokenizer_options::kThreshold>(
      prefix, parent ? &parent->threshold : nullptr);
    int parent_topk = parent ? parent->top_k : 1;
    opts.top_k = Resolve<tokenizer_options::kTopK>(
      prefix, parent ? &parent_topk : nullptr);
    return opts;
  }

  irs::analysis::NearestNeighborsTokenizer::Options BuildNearestNeighbors(
    std::string_view prefix,
    const irs::analysis::NearestNeighborsTokenizer::Options* parent) {
    irs::analysis::NearestNeighborsTokenizer::Options opts;
    ResolveStringInto<tokenizer_options::kModelLocation>(
      prefix, opts.model_location, parent ? &parent->model_location : nullptr);
    int parent_topk = parent ? parent->top_k : 1;
    opts.top_k = Resolve<tokenizer_options::kTopK>(
      prefix, parent ? &parent_topk : nullptr);
    return opts;
  }

  irs::analysis::SolrSynonymsTokenizer::Options BuildSolrSynonyms(
    std::string_view prefix,
    const irs::analysis::SolrSynonymsTokenizer::Options* parent) {
    irs::analysis::SolrSynonymsTokenizer::Options opts;
    if (OptionsParser::HasOption(tokenizer_options::kSolrSynonyms, prefix)) {
      opts.synonyms_text =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kSolrSynonyms>(
          prefix);
    } else if (parent) {
      opts.synonyms_text = parent->synonyms_text;
    } else {
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kSolrSynonyms>(
        prefix);  // throws
    }
    return opts;
  }

  irs::analysis::WordnetSynonymsTokenizer::Options BuildWordnetSynonyms(
    std::string_view prefix,
    const irs::analysis::WordnetSynonymsTokenizer::Options* parent) {
    irs::analysis::WordnetSynonymsTokenizer::Options opts;
    if (OptionsParser::HasOption(tokenizer_options::kWordnetSynonyms, prefix)) {
      opts.synonyms_text = OptionsParser::EraseOptionOrDefault<
        tokenizer_options::kWordnetSynonyms>(prefix);
    } else if (parent) {
      opts.synonyms_text = parent->synonyms_text;
    } else {
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kWordnetSynonyms>(
        prefix);  // throws
    }
    return opts;
  }

  void ResolveGeoS2(std::string_view prefix, sdb::geo::GeoOptions& opts,
                    const sdb::geo::GeoOptions* parent) {
    int parent_mc = parent ? parent->max_cells : 20;
    int parent_min = parent ? parent->min_level : 4;
    int parent_max = parent ? parent->max_level : 23;
    int parent_lm = parent ? parent->level_mod : 1;
    opts.max_cells = Resolve<tokenizer_options::kGeoMaxCells>(
      prefix, parent ? &parent_mc : nullptr);
    opts.min_level = Resolve<tokenizer_options::kGeoMinLevel>(
      prefix, parent ? &parent_min : nullptr);
    opts.max_level = Resolve<tokenizer_options::kGeoMaxLevel>(
      prefix, parent ? &parent_max : nullptr);
    opts.level_mod =
      static_cast<int8_t>(Resolve<tokenizer_options::kGeoLevelMod>(
        prefix, parent ? &parent_lm : nullptr));
    opts.optimize_for_space = Resolve<tokenizer_options::kGeoOptimizeForSpace>(
      prefix, parent ? &parent->optimize_for_space : nullptr);
  }

  irs::analysis::GeoPointAnalyzer::Options BuildGeoPoint(
    std::string_view prefix,
    const irs::analysis::GeoPointAnalyzer::Options* parent) {
    irs::analysis::GeoPointAnalyzer::Options opts;
    auto split_path = [](std::string_view path) {
      return absl::StrSplit(path, '/', absl::SkipEmpty());
    };
    bool lat_set = false;
    bool lng_set = false;
    if (OptionsParser::HasOption(tokenizer_options::kGeoLatitude, prefix)) {
      auto raw =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kGeoLatitude>(
          prefix);
      opts.latitude = split_path(raw);
      lat_set = !opts.latitude.empty();
    } else if (parent) {
      opts.latitude = parent->latitude;
      lat_set = !opts.latitude.empty();
    }
    if (OptionsParser::HasOption(tokenizer_options::kGeoLongitude, prefix)) {
      auto raw =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kGeoLongitude>(
          prefix);
      opts.longitude = split_path(raw);
      lng_set = !opts.longitude.empty();
    } else if (parent) {
      opts.longitude = parent->longitude;
      lng_set = !opts.longitude.empty();
    }
    if (lat_set != lng_set) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("'latitude' and 'longitude' must be both set or both "
                "left empty for the geopoint tokenizer"));
    }
    ResolveGeoS2(prefix, opts.options, parent ? &parent->options : nullptr);
    return opts;
  }

  irs::analysis::GeoJsonAnalyzer::Options BuildGeoJson(
    std::string_view prefix,
    const irs::analysis::GeoJsonAnalyzer::Options* parent) {
    using GJ = irs::analysis::GeoJsonAnalyzer;
    irs::analysis::GeoJsonAnalyzer::Options opts;
    opts.type = ResolveEnum<tokenizer_options::kGeoJsonType, GJ::Type>(
      prefix, parent ? &parent->type : nullptr);
    opts.coding = ResolveEnum<tokenizer_options::kGeoJsonCoding, GJ::Coding>(
      prefix, parent ? &parent->coding : nullptr);
    ResolveGeoS2(prefix, opts.options, parent ? &parent->options : nullptr);
    return opts;
  }

  irs::analysis::PipelineTokenizer::Options BuildPipeline(
    std::string_view prefix,
    const irs::analysis::PipelineTokenizer::Options* parent) {
    irs::analysis::PipelineTokenizer::Options opts;
    int step = 1;
    while (true) {
      auto step_prefix = OptionInfo::AdjustPrefix(prefix, "step", step);
      const irs::analysis::TokenizerConfig* parent_child = nullptr;
      std::string type;
      if (OptionsParser::HasOption(tokenizer_options::kTemplate, step_prefix)) {
        type =
          OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>(
            step_prefix);
      } else if (parent &&
                 static_cast<size_t>(step) <= parent->children.size()) {
        parent_child = parent->children[step - 1].get();
        if (!parent_child) {
          break;
        }
        type = std::string{TypeNameOf(*parent_child)};
      }
      if (type.empty()) {
        break;
      }
      auto child = std::make_unique<irs::analysis::TokenizerConfig>();
      BuildChild(type, step_prefix, parent_child, *child);
      opts.children.push_back(std::move(child));
      ++step;
    }
    return opts;
  }

  irs::analysis::UnionTokenizer::Options BuildUnion(
    std::string_view prefix,
    const irs::analysis::UnionTokenizer::Options* parent) {
    irs::analysis::UnionTokenizer::Options opts;
    int idx = 1;
    while (true) {
      auto child_prefix = OptionInfo::AdjustPrefix(prefix, "tokenizer", idx);
      const irs::analysis::TokenizerConfig* parent_child = nullptr;
      std::string type;
      if (OptionsParser::HasOption(tokenizer_options::kTemplate,
                                   child_prefix)) {
        type =
          OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>(
            child_prefix);
      } else if (parent &&
                 static_cast<size_t>(idx) <= parent->children.size()) {
        parent_child = parent->children[idx - 1].get();
        if (!parent_child) {
          break;
        }
        type = std::string{TypeNameOf(*parent_child)};
      }
      if (type.empty()) {
        break;
      }
      auto child = std::make_unique<irs::analysis::TokenizerConfig>();
      BuildChild(type, child_prefix, parent_child, *child);
      opts.children.push_back(std::move(child));
      ++idx;
    }
    if (idx == 1) {
      if (parent || !HasUnionChildOption(prefix)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("Union tokenizer requires at least one "
                                "tokenizer<N> child"));
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("Union tokenizer children must be numbered "
                              "densely starting from tokenizer1"));
    }
    return opts;
  }

  std::unique_ptr<irs::analysis::TokenizerConfig> BuildSingleChild(
    std::string_view prefix,
    const irs::analysis::TokenizerConfig* parent_child) {
    auto child_prefix = OptionInfo::AdjustPrefix(prefix, "tokenizer");
    std::string type;
    if (OptionsParser::HasOption(tokenizer_options::kTemplate, child_prefix) ||
        !parent_child) {
      type = OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>(
        child_prefix);
    } else {
      type = std::string{TypeNameOf(*parent_child)};
    }
    SDB_ASSERT(!type.empty());
    auto child = std::make_unique<irs::analysis::TokenizerConfig>();
    BuildChild(type, child_prefix, parent_child, *child);
    return child;
  }

  irs::analysis::MinHashTokenizer::Options BuildMinHash(
    std::string_view prefix,
    const irs::analysis::MinHashTokenizer::Options* parent) {
    irs::analysis::MinHashTokenizer::Options opts;
    opts.analyzer =
      BuildSingleChild(prefix, parent ? parent->analyzer.get() : nullptr);
    int parent_n = parent ? static_cast<int>(parent->num_hashes) : 1;
    opts.num_hashes =
      static_cast<uint32_t>(Resolve<tokenizer_options::kNumHashes>(
        prefix, parent ? &parent_n : nullptr));
    return opts;
  }

  irs::analysis::WildcardAnalyzer::Options BuildWildcard(
    std::string_view prefix,
    const irs::analysis::WildcardAnalyzer::Options* parent) {
    irs::analysis::WildcardAnalyzer::Options opts;
    opts.base_analyzer =
      BuildSingleChild(prefix, parent ? parent->base_analyzer.get() : nullptr);
    int parent_n = parent ? static_cast<int>(parent->ngram_size) : 3;
    opts.ngram_size =
      static_cast<size_t>(Resolve<tokenizer_options::kNgramSize>(
        prefix, parent ? &parent_n : nullptr));
    return opts;
  }

  template<typename Opts>
  static const Opts* ParentOptions(const irs::analysis::TokenizerConfig* cfg) {
    if (!cfg) {
      return nullptr;
    }
    return std::get_if<Opts>(&cfg->config);
  }

  void BuildChild(std::string_view type, std::string_view prefix,
                  const irs::analysis::TokenizerConfig* parent_cfg,
                  irs::analysis::TokenizerConfig& out) {
    if (type == tokenizer_options::kCopyFromGroup.name) {
      BuildCopyFrom(prefix, out);
      return;
    }
    Dispatch(type, prefix, parent_cfg, out);
  }

  void Dispatch(std::string_view type, std::string_view prefix,
                const irs::analysis::TokenizerConfig* parent_cfg,
                irs::analysis::TokenizerConfig& out) {
    using namespace irs::analysis;
    if (type == TextTokenizer::type_name()) {
      out.config =
        BuildText(prefix, ParentOptions<TextTokenizer::Options>(parent_cfg));
    } else if (type == NGramTokenizerBase::type_name()) {
      out.config = BuildNGram(
        prefix, ParentOptions<NGramTokenizerBase::Options>(parent_cfg));
    } else if (type == SparseNGramTokenizer::type_name()) {
      out.config = BuildSparseNGram(
        prefix, ParentOptions<SparseNGramTokenizer::Options>(parent_cfg));
    } else if (type == NearestNeighborsTokenizer::type_name()) {
      out.config = BuildNearestNeighbors(
        prefix, ParentOptions<NearestNeighborsTokenizer::Options>(parent_cfg));
    } else if (type == StemmingTokenizer::type_name()) {
      out.config = BuildStem(
        prefix, ParentOptions<StemmingTokenizer::Options>(parent_cfg));
    } else if (type == StopwordsTokenizer::type_name()) {
      out.config = BuildStopwords(
        prefix, ParentOptions<StopwordsTokenizer::Options>(parent_cfg));
    } else if (type == ClassificationTokenizer::type_name()) {
      out.config = BuildClassification(
        prefix, ParentOptions<ClassificationTokenizer::Options>(parent_cfg));
    } else if (type == CollationTokenizer::type_name()) {
      out.config = BuildCollation(
        prefix, ParentOptions<CollationTokenizer::Options>(parent_cfg));
    } else if (type == DelimitedTokenizer::type_name()) {
      out.config = BuildDelimiter(
        prefix, ParentOptions<DelimitedTokenizer::Options>(parent_cfg));
    } else if (type == MultiDelimitedTokenizer::type_name()) {
      out.config = BuildMultiDelimiter(
        prefix, ParentOptions<MultiDelimitedTokenizer::Options>(parent_cfg));
    } else if (type == MinHashTokenizer::type_name()) {
      out.config = BuildMinHash(
        prefix, ParentOptions<MinHashTokenizer::Options>(parent_cfg));
    } else if (type == WildcardAnalyzer::type_name()) {
      out.config = BuildWildcard(
        prefix, ParentOptions<WildcardAnalyzer::Options>(parent_cfg));
    } else if (type == NormalizingTokenizer::type_name()) {
      out.config = BuildNormalizing(
        prefix, ParentOptions<NormalizingTokenizer::Options>(parent_cfg));
    } else if (type == SegmentationTokenizer::type_name()) {
      out.config = BuildSegmentation(
        prefix, ParentOptions<SegmentationTokenizer::Options>(parent_cfg));
    } else if (type == PipelineTokenizer::type_name()) {
      out.config = BuildPipeline(
        prefix, ParentOptions<PipelineTokenizer::Options>(parent_cfg));
    } else if (type == PatternTokenizer::type_name()) {
      out.config = BuildPattern(
        prefix, ParentOptions<PatternTokenizer::Options>(parent_cfg));
    } else if (type == PathHierarchyTokenizer::type_name()) {
      out.config = BuildPathHierarchy(
        prefix, ParentOptions<PathHierarchyTokenizer::Options>(parent_cfg));
    } else if (type == UnionTokenizer::type_name()) {
      out.config =
        BuildUnion(prefix, ParentOptions<UnionTokenizer::Options>(parent_cfg));
    } else if (type == GeoPointAnalyzer::type_name()) {
      out.config = BuildGeoPoint(
        prefix, ParentOptions<GeoPointAnalyzer::Options>(parent_cfg));
    } else if (type == GeoJsonAnalyzer::type_name()) {
      out.config = BuildGeoJson(
        prefix, ParentOptions<GeoJsonAnalyzer::Options>(parent_cfg));
    } else if (type == irs::StringTokenizer::type_name()) {
      out.config = BuildKeyword(
        prefix, ParentOptions<irs::StringTokenizer::Options>(parent_cfg));
    } else if (type == SolrSynonymsTokenizer::type_name()) {
      out.config = BuildSolrSynonyms(
        prefix, ParentOptions<SolrSynonymsTokenizer::Options>(parent_cfg));
    } else if (type == WordnetSynonymsTokenizer::type_name()) {
      out.config = BuildWordnetSynonyms(
        prefix, ParentOptions<WordnetSynonymsTokenizer::Options>(parent_cfg));
    } else {
      SDB_ASSERT(false);
    }
  }

  void BuildCopyFrom(std::string_view prefix,
                     irs::analysis::TokenizerConfig& out) {
    std::string from =
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kFrom>(prefix);
    auto name = ParseObjectName(from, _current_schema);
    auto tokenizer = _snapshot->GetTokenizer(catalog::NoAccessCheck(), _db_id,
                                             name.schema, name.relation);
    if (!tokenizer) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("text search dictionary \"", from, "\" does not exist"));
    }
    const auto& parent_cfg = tokenizer->Config();
    auto type = std::string{TypeNameOf(parent_cfg)};
    Dispatch(type, prefix, &parent_cfg, out);
  }

  void ParseFeatures(std::string_view type) {
    VisitValues<tokenizer_options::kFeaturesOptions>(
      [&]<const OptionInfo & Feature> {
        bool use_feature = OptionsParser::EraseOptionOrDefault<Feature>();
        if (use_feature) {
          bool added = _features.Add(Feature.name);
          SDB_ASSERT(added);
        }
      });
    if (OptionsParser::HasOption(tokenizer_options::kNormRowGroupSize)) {
      if (!_features.HasFeatures(irs::IndexFeatures::Norm)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("norm_row_group_size requires norm = true"),
          ERR_HINT(tokenizer_options::kNormRowGroupSize.description));
      }
      _norm_row_group_size =
        static_cast<uint32_t>(OptionsParser::EraseOptionOrDefault<
                              tokenizer_options::kNormRowGroupSize>());
    }
    auto r = _features.Validate(type);
    if (!r.ok()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG(r.errorMessage()));
    }
  }

  irs::analysis::TokenizerConfig _config;
  search::Features _features;
  uint32_t _norm_row_group_size = DEFAULT_ROW_GROUP_SIZE;
  std::shared_ptr<const catalog::Snapshot> _snapshot;
  ObjectId _db_id;
  std::string_view _current_schema;
};

}  // namespace

void CreateTokenizer(ConnectionContext& conn_ctx, std::string_view name,
                     std::string_view schema, bool if_not_exists,
                     const duckdb::named_parameter_map_t& options) {
  auto snapshot = conn_ctx.CatalogSnapshot();
  auto db_id = conn_ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();

  auto [cfg, features, norm_row_group_size] =
    std::move(
      CreateTSDictionaryOptions{snapshot, db_id, current_schema, options})
      .Result();

  auto test_analyzer = irs::analysis::CreateAnalyzer(irs::analysis::Clone(cfg));
  SDB_ASSERT(test_analyzer);

  if (features.HasFeatures(irs::IndexFeatures::Offs) &&
      !irs::get<irs::OffsAttr>(*test_analyzer)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Unsupported index features are specified"));
  }

  auto tokenizer = std::make_shared<catalog::Tokenizer>(
    conn_ctx.GetRoleId(), ObjectId{}, ObjectId{}, name, features,
    norm_row_group_size, std::move(cfg));

  auto& catalog = catalog::GetCatalog();
  auto r = catalog.CreateTokenizer(catalog::AccessContext{conn_ctx.GetRoleId()},
                                   db_id, schema, std::move(tokenizer));

  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && !if_not_exists) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
      ERR_MSG("text search dictionary \"", name, "\" already exists"));
  }
  if (!r.ok() && !r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    SDB_THROW(std::move(r));
  }
}

}  // namespace sdb::pg
