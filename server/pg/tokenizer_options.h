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

#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/icu_text_tokenizer.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/analysis/multi_delimited_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/path_hierarchy_tokenizer.hpp>
#include <iresearch/analysis/pattern_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/shingle_tokenizer.hpp>
#include <iresearch/analysis/solr_synonyms_tokenizer.hpp>
#include <iresearch/analysis/sparse_ngram_tokenizer.hpp>
#include <iresearch/analysis/split_by_non_alpha_tokenizer.hpp>
#include <iresearch/analysis/sql_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/stopwords_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/analysis/wildcard_tokenizer.hpp>
#include <iresearch/analysis/wordnet_synonyms_tokenizer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/type_id.hpp>
#include <variant>

#include "pg/geo_tokenizer_options.h"
#include "pg/option_help.h"

namespace sdb::pg::tokenizer_options {

using namespace std::string_view_literals;

void CheckFileExists(std::string_view option, std::string_view path);

// Features

// TODO(codeworse) Add for option alternative names? dl, norms
inline constexpr OptionInfo kNormFeature{"norm", false,
                                         "Enables norm feature in index"};

// TODO(codeworse) Add for option alternative names? freq, frequencies, freqs
inline constexpr OptionInfo kFreqFeature{"frequency", false,
                                         "Enables frequency feature in index"};

// TODO(codeworse) Add for option alternative names? pos, positions
inline constexpr OptionInfo kPosFeature{"position", false,
                                        "Enables position feature in index"};

// TODO(codeworse) Add for option alternative names? offsets
inline constexpr OptionInfo kOffsetFeature{"offset", false,
                                           "Enables offset feature in index"};

// Common

inline constexpr OptionInfo kLocale{"locale", ""sv,
                                    "ICU locale string (e.g. en_US.UTF-8)"};

inline constexpr OptionInfo kAccent{"accent", true, "Preserve accent marks"};

void CheckCase(std::string_view option, std::string_view value);

inline constexpr OptionInfo kCase{
  "case", "none"sv, "Text case conversion: none, lower, upper", CheckCase};

void CheckForm(std::string_view option, std::string_view value);

inline constexpr OptionInfo kForm{
  "form", "nfc"sv, "Unicode normalization form: nfc, nfkc", CheckForm};

inline constexpr OptionInfo kModelLocation{
  "modellocation", ""sv, "Path to the ML model file", CheckFileExists};

inline constexpr OptionInfo kTopK{"topk", 1, "Number of top results to return"};

// Text

inline constexpr OptionInfo kStopwords{
  "stopwords", OptionInfo::ListTag{},
  "Inline stop words: a list of strings, or a comma-separated string of "
  "double-quoted words"};

inline constexpr OptionInfo kStopwordsPath{
  "stopwordspath", ""sv,
  "Path to a stop-word file, or to a directory whose files are all loaded",
  CheckFileExists};

// NGram

inline constexpr OptionInfo kMinGram{"mingram", 2, "Minimum n-gram length"};

inline constexpr OptionInfo kMaxGram{"maxgram", 3, "Maximum n-gram length"};

inline constexpr OptionInfo kPreserveOriginal{
  "preserveoriginal", false, "Emit the original token alongside n-grams"};

inline constexpr OptionInfo kInputType{"inputtype", "utf8"sv,
                                       "Input stream encoding: binary, utf8"};

inline constexpr OptionInfo kStartMarker{
  "startmarker", ""sv, "Prefix marker appended at n-gram boundary"};

inline constexpr OptionInfo kEndMarker{
  "endmarker", ""sv, "Suffix marker appended at n-gram boundary"};

void CheckMode(std::string_view option, std::string_view value);

inline constexpr OptionInfo kMode{
  "mode", "all"sv,
  "Mode of generation: all, only_prefix, only_suffix, only_prefix_and_suffix",
  CheckMode};

// Sparse NGram

void CheckMaxNGramLength(std::string_view option, int value);

inline constexpr OptionInfo kCovering{
  "covering", false,
  "Emit the minimal covering n-gram chain (query side) instead of all sparse "
  "n-grams (index side)"};

inline constexpr OptionInfo kMaxNGramLength{
  "maxngramlength", 16, "Maximum emitted n-gram length (minimum 3)",
  CheckMaxNGramLength};

// Classification

void CheckThreshold(std::string_view option, double value);

inline constexpr OptionInfo kThreshold{
  "threshold", 0.0, "Minimum confidence score [0.0..1.0]", CheckThreshold};

// Stopwords tokenizer

inline constexpr OptionInfo kHex{"hex", false,
                                 "Treat stop words as hex-encoded strings"};

// Wildcard

void CheckNGramSize(std::string_view option, int value);
inline constexpr OptionInfo kNGramSize{
  "ngramsize", 3, "N-gram size for wildcard prefix indexing (minimum 2)",
  CheckNGramSize};

// Geo options (kGeoMaxCells, kGeoLatitude, kGeoJsonType, ...) live in
// "pg/geo_tokenizer_options.h", brought in by the include above.

// Text

inline constexpr OptionInfo kBreak{"break", "alpha"sv,
                                   "Token boundary detection mode: all, "
                                   "graphic, alpha, sentence, line, paragraph"};

// Icu text

inline constexpr OptionInfo kIcuTextLocale{
  "locale", OptionInfo::RequiredTag<std::string_view>{},
  "ICU locale string (e.g. en_US.UTF-8)"};

inline constexpr OptionInfo kIcuTextBreak{
  "break", "alpha"sv,
  "Token boundary detection mode: all, graphic, alpha, sentence"};

// Delimiter

inline constexpr OptionInfo kDelimiter{
  "delimiter", OptionInfo::RequiredTag<std::string_view>{},
  "Token delimiter character or string"};

// Multi-Delimiter

inline constexpr OptionInfo kDelimiters{
  "delimiters", OptionInfo::RequiredListTag{},
  "Delimiters: a list of strings, or a comma-separated string of "
  "double-quoted delimiters (e.g. '\",\", \"|\", \"!\"')"};

inline constexpr std::string_view kDictionaryTemplate = "dictionary";

inline constexpr OptionInfo kFrom{"from",
                                  OptionInfo::RequiredTag<std::string_view>{},
                                  "the source tokenizer name"};

// Template

void CheckTemplate(std::string_view option, std::string_view value);
constexpr OptionInfo kTemplate{"template",
                               OptionInfo::RequiredTag<std::string_view>{},
                               "Tokenizer template type", CheckTemplate};

// Shingle (word n-gram) analyzer.

void CheckShingleSize(std::string_view option, int value);

inline constexpr OptionInfo kMinShingleSize{
  "mingram", 2, "Minimum shingle (word n-gram) size (minimum 2)",
  CheckShingleSize};

inline constexpr OptionInfo kMaxShingleSize{
  "maxgram", 2, "Maximum shingle (word n-gram) size (>= mingram)",
  CheckShingleSize};

inline constexpr OptionInfo kOutputUnigrams{
  "outputunigrams", true, "Index individual tokens alongside the shingles"};

inline constexpr OptionInfo kOutputUnigramsIfNoShingles{
  "outputunigramsifnoshingles", false,
  "Index unigrams only when the input is too short to form a shingle"};

inline constexpr OptionInfo kStoreTokens{
  "storetokens", true,
  "Persist the per-document token stream (verification source for phrases "
  "longer than maxgram). When false the index stores terms only"};

inline constexpr OptionInfo kFrequentWords{
  "frequentwords", OptionInfo::ListTag{},
  "Frequent words (typically stopwords): a list of strings, or a "
  "comma-separated string of double-quoted words. When non-empty, shingles "
  "of mingram stay dense while wider sizes are indexed only for spans "
  "containing one of these words (adaptive width escalation)"};

inline constexpr OptionInfo kFillerToken{
  "fillertoken", ""sv,
  "Token standing in for positions the base analyzer removed (e.g. "
  "stopwords) in the stored token stream; never indexed as a term. "
  "Default '_'"};

// Pattern

inline constexpr OptionInfo kPattern{
  "pattern", OptionInfo::RequiredTag<std::string_view>{},
  "RE2 regular expression pattern for matching or splitting"};

inline constexpr OptionInfo kGroup{
  "group", -1,
  "Capture group to extract: -1=split, 0=whole match, N>0=Nth group"};

// Path Hierarchy

inline constexpr OptionInfo kPathDelimiter{
  "delimiter", "/"sv, "Path separator character or string (UTF-8)"};

inline constexpr OptionInfo kPathReplacement{
  "replacement", ""sv, "Replacement for delimiter in tokens"};

inline constexpr OptionInfo kReverse{
  "reverse", false, "Use reverse tokenization for domain-like hierarchies"};

inline constexpr OptionInfo kSkip{"skip", 0,
                                  "Number of initial tokens to skip"};

// Sql

inline constexpr OptionInfo kSqlExpression{
  "expression", OptionInfo::RequiredTag<std::string_view>{},
  "DuckDB scalar expression over its input $1 (VARCHAR), "
  "returning VARCHAR or BLOB (one token per value) or a list of them (token "
  "list); built-in functions only, no subqueries, no volatile functions"};

// Synonyms (Solr / WordNet)

inline constexpr OptionInfo kSolrSynonyms{
  "synonyms", OptionInfo::RequiredTag<std::string_view>{},
  "Inline Solr-format synonyms file content: one rule per line, comma-"
  "separated terms; `=>` separates LHS from RHS for one-way mappings"};

inline constexpr OptionInfo kWordnetSynonyms{
  "synonyms", OptionInfo::RequiredTag<std::string_view>{},
  "Inline WordNet Prolog database content: one `s(synset,w_num,'word',ss_"
  "type,sense_number,tag_count).` record per line"};

// Per-tokenizer option arrays

inline constexpr OptionInfo kFeaturesOptions[] = {kNormFeature, kOffsetFeature,
                                                  kPosFeature, kFreqFeature};

inline constexpr OptionInfo kNGramOptions[] = {
  kMinGram,   kMaxGram, kPreserveOriginal, kInputType, kStartMarker,
  kEndMarker, kMode};

inline constexpr OptionInfo kSparseNGramOptions[] = {kMaxNGramLength,
                                                     kCovering};

inline constexpr OptionInfo kNearestNeighborsOptions[] = {kModelLocation,
                                                          kTopK};

inline constexpr OptionInfo kStemmingOptions[] = {kLocale};

inline constexpr OptionInfo kStopwordsTokenizerOptions[] = {
  kStopwords, kStopwordsPath, kHex};

inline constexpr OptionInfo kClassificationOptions[] = {kModelLocation, kTopK,
                                                        kThreshold};

inline constexpr OptionInfo kCollationOptions[] = {kLocale};

inline constexpr OptionInfo kDelimiterOptions[] = {kDelimiter};

inline constexpr OptionInfo kMultiDelimiterOptions[] = {kDelimiters};

inline constexpr OptionInfo kWildcardOptions[] = {kNGramSize};

inline constexpr OptionInfo kNormLocale{
  "locale", ""sv,
  "ICU locale for case conversion; omit for locale-independent simple case"};

inline constexpr OptionInfo kNormOptions[] = {kNormLocale, kCase, kAccent,
                                              kForm};

inline constexpr OptionInfo kSplitByNonAlphaOptions[] = {kCase};

inline constexpr OptionInfo kTextOptions[] = {kCase, kBreak};
inline constexpr OptionInfo kIcuTextOptions[] = {kIcuTextLocale, kIcuTextBreak};

inline constexpr OptionInfo kPatternOptions[] = {kPattern, kGroup};

inline constexpr OptionInfo kPathHierarchyOptions[] = {
  kPathDelimiter, kPathReplacement, kReverse, kSkip};

inline constexpr OptionInfo kSqlOptions[] = {kSqlExpression};

inline constexpr OptionInfo kShingleOptions[] = {
  kMinShingleSize, kMaxShingleSize,
  kOutputUnigrams, kOutputUnigramsIfNoShingles,
  kStoreTokens,    kFrequentWords,
  kFillerToken};

inline constexpr OptionInfo kSolrSynonymsOptions[] = {kSolrSynonyms};

inline constexpr OptionInfo kWordnetSynonymsOptions[] = {kWordnetSynonyms};

// Groups

inline constexpr OptionGroup kFeaturesGroup{
  "features", kFeaturesOptions, {}, {}, TemplateKind::Features,
};
inline constexpr OptionGroup kTextGroup{
  irs::analysis::TextTokenizer::type_name(),
  kTextOptions,
  {},
  "split_text",
};
inline constexpr OptionGroup kNGramGroup{
  irs::analysis::NGramTokenizer::type_name(),
  kNGramOptions,
  {},
  "generate_ngrams",
};
inline constexpr OptionGroup kSparseNGramGroup{
  irs::analysis::SparseNGramTokenizer::type_name(),
  kSparseNGramOptions,
  {},
  "generate_sparse_ngrams",
};
inline constexpr OptionGroup kNearestNeighborsGroup{
  irs::analysis::NearestNeighborsTokenizer::type_name(),
  kNearestNeighborsOptions,
  {},
  "find_nearest_words",
};
inline constexpr OptionGroup kStemmingGroup{
  irs::analysis::StemmingTokenizer::type_name(),
  kStemmingOptions,
  {},
  "stem_words",
};
inline constexpr OptionGroup kStopwordsGroup{
  irs::analysis::StopwordsTokenizer::type_name(),
  kStopwordsTokenizerOptions,
  {},
  "remove_stopwords",
};
inline constexpr OptionGroup kClassificationGroup{
  irs::analysis::ClassificationTokenizer::type_name(),
  kClassificationOptions,
  {},
  "classify_text",
};
inline constexpr OptionGroup kCollationGroup{
  irs::analysis::CollationTokenizer::type_name(),
  kCollationOptions,
  {},
  "collate_tokens",
};
inline constexpr OptionGroup kDelimiterGroup{
  irs::analysis::DelimitedTokenizer::type_name(),
  kDelimiterOptions,
  {},
  "split_csv",
};
inline constexpr OptionGroup kMultiDelimiterGroup{
  irs::analysis::MultiDelimitedTokenizer::type_name(),
  kMultiDelimiterOptions,
  {},
  "split_by_delimiters",
};
inline constexpr OptionGroup kWildcardGroup{
  irs::analysis::WildcardTokenizer::type_name(),
  kWildcardOptions,
  {},
  "generate_wildcard_ngrams",
  TemplateKind::Wrapper,
};
inline constexpr OptionGroup kNormGroup{
  irs::analysis::NormalizingTokenizer::type_name(),
  kNormOptions,
  {},
  "normalize_tokens",
};
inline constexpr OptionGroup kSplitByNonAlphaGroup{
  irs::analysis::SplitByNonAlphaTokenizer::type_name(),
  kSplitByNonAlphaOptions,
  {},
  "split_by_non_alpha",
};
inline constexpr OptionGroup kIcuTextGroup{
  irs::analysis::IcuTextTokenizer::type_name(),
  kIcuTextOptions,
  {},
  "split_text_icu",
};
inline constexpr OptionGroup kPipelineGroup{
  irs::analysis::PipelineTokenizer::type_name(),
  {},
  {},
  {},
  TemplateKind::Composite,
};
inline constexpr OptionGroup kPatternGroup{
  irs::analysis::PatternTokenizer::type_name(),
  kPatternOptions,
  {},
  "split_by_pattern",
};
inline constexpr OptionGroup kPathHierarchyGroup{
  irs::analysis::PathHierarchyTokenizer::type_name(),
  kPathHierarchyOptions,
  {},
  "expand_path",
};
inline constexpr OptionGroup kUnionGroup{
  irs::analysis::UnionTokenizer::type_name(),
  {},
  {},
  {},
  TemplateKind::Composite,
};
inline constexpr OptionGroup kKeywordGroup{
  irs::KeywordTokenizer::type_name(),
  {},
  {},
};
inline constexpr OptionGroup kSqlGroup{
  irs::analysis::SqlTokenizer::type_name(),
  kSqlOptions,
  {},
  {},
  TemplateKind::Composite,
};
inline constexpr OptionGroup kShingleGroup{
  irs::analysis::ShingleTokenizer::type_name(),
  kShingleOptions,
  {},
  "generate_shingles",
  TemplateKind::Wrapper,
};
inline constexpr OptionGroup kSolrSynonymsGroup{
  irs::analysis::SolrSynonymsTokenizer::type_name(),
  kSolrSynonymsOptions,
  {},
  "expand_solr_synonyms",
};
inline constexpr OptionGroup kWordnetSynonymsGroup{
  irs::analysis::WordnetSynonymsTokenizer::type_name(),
  kWordnetSynonymsOptions,
  {},
  "expand_wordnet_synonyms",
};

inline constexpr OptionGroup kTokenizerSubgroups[] = {
  kFeaturesGroup,        kTextGroup,
  kNGramGroup,           kNearestNeighborsGroup,
  kStemmingGroup,        kStopwordsGroup,
  kClassificationGroup,  kCollationGroup,
  kDelimiterGroup,       kMultiDelimiterGroup,
  kWildcardGroup,        kNormGroup,
  kIcuTextGroup,         kSplitByNonAlphaGroup,
  kPipelineGroup,        kPatternGroup,
  kPathHierarchyGroup,   kUnionGroup,
  kGeoPointGroup,        kGeoJsonGroup,
  kKeywordGroup,         kSqlGroup,
  kShingleGroup,         kSolrSynonymsGroup,
  kWordnetSynonymsGroup, kSparseNGramGroup,
};

}  // namespace sdb::pg::tokenizer_options
