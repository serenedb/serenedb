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

#include <duckdb/storage/storage_info.hpp>
#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/minhash_tokenizer.hpp>
#include <iresearch/analysis/multi_delimited_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/path_hierarchy_tokenizer.hpp>
#include <iresearch/analysis/pattern_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/shingle_analyzer.hpp>
#include <iresearch/analysis/solr_synonyms_tokenizer.hpp>
#include <iresearch/analysis/sparse_ngram_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/stopwords_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/analysis/wordnet_synonyms_tokenizer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/utils/type_id.hpp>
#include <variant>

#include "basics/assert.h"
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

inline constexpr OptionInfo kNormRowGroupSize{
  "norm_row_group_size",
  static_cast<int>(DEFAULT_ROW_GROUP_SIZE),
  "Norm column row-group size for indexes that bind this dictionary "
  "with norm = true.",
  CheckPositiveInt,
};

// Common

inline constexpr OptionInfo kLocale{"locale", ""sv,
                                    "ICU locale string (e.g. en_US.UTF-8)"};

inline constexpr OptionInfo kAccent{"accent", true, "Preserve accent marks"};

void CheckCase(std::string_view option, std::string_view value);

inline constexpr OptionInfo kCase{
  "case", "none"sv, "Text case conversion: none, lower, upper", CheckCase};

inline constexpr OptionInfo kModelLocation{
  "modellocation", ""sv, "Path to the ML model file", CheckFileExists};

inline constexpr OptionInfo kTopK{"topk", 1, "Number of top results to return"};

// Text

inline constexpr OptionInfo kStemming{"stemming", true,
                                      "Apply stemming to tokens"};

inline constexpr OptionInfo kStopwords{
  "stopwords", ""sv, "Comma-separated list of inline stop words"};

inline constexpr OptionInfo kStopwordsPath{
  "stopwordspath", ""sv, "Path to file containing stop words", CheckFileExists};

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

// Sparse NGram

void CheckMaxNgramLength(std::string_view option, int value);

inline constexpr OptionInfo kCovering{
  "covering", false,
  "Emit the minimal covering n-gram chain (query side) instead of all sparse "
  "n-grams (index side)"};

inline constexpr OptionInfo kMaxNgramLength{
  "maxngramlength", 16, "Maximum emitted n-gram length (minimum 3)",
  CheckMaxNgramLength};

// Classification

void CheckThreshold(std::string_view option, double value);

inline constexpr OptionInfo kThreshold{
  "threshold", 0.0, "Minimum confidence score [0.0..1.0]", CheckThreshold};

// Stopwords tokenizer

inline constexpr OptionInfo kHex{"hex", false,
                                 "Treat stop words as hex-encoded strings"};

// MinHash

inline constexpr OptionInfo kNumHashes{
  "numhashes", 1, "Number of hash functions to use", CheckPositiveInt};

// Wildcard

void CheckNgramSize(std::string_view option, int value);
inline constexpr OptionInfo kNgramSize{
  "ngramsize", 3, "N-gram size for wildcard prefix indexing (minimum 2)",
  CheckNgramSize};

// Shingle (word n-gram) analyzer for position-free phrase search.

void CheckShingleSize(std::string_view option, int value);

inline constexpr OptionInfo kMinShingleSize{
  "minshinglesize", 2, "Minimum shingle (word n-gram) size (minimum 2)",
  CheckShingleSize};

inline constexpr OptionInfo kMaxShingleSize{
  "maxshinglesize", 2, "Maximum shingle (word n-gram) size (>= minshinglesize)",
  CheckShingleSize};

inline constexpr OptionInfo kOutputUnigrams{
  "outputunigrams", true, "Index individual tokens alongside the shingles"};

inline constexpr OptionInfo kOutputUnigramsIfNoShingles{
  "outputunigramsifnoshingles", false,
  "Index unigrams only when the input is too short to form a shingle"};

inline constexpr OptionInfo kStoreTokens{
  "storetokens", true,
  "Persist the per-document token stream that verifies phrases longer than "
  "maxshinglesize on position-free columns. When false the index stores "
  "terms only and such phrases are rejected"};

inline constexpr OptionInfo kFrequentWords{
  "frequentwords", ""sv,
  "Comma-separated words (typically stopwords). When non-empty, shingles of "
  "minshinglesize stay dense while wider sizes are indexed only for spans "
  "containing one of these words (adaptive width escalation)"};

inline constexpr OptionInfo kFillerToken{
  "fillertoken", ""sv,
  "Token standing in for positions the base analyzer removed (e.g. "
  "stopwords) in the stored token stream; never indexed as a term. "
  "Default '_'"};

// Geo options (kGeoMaxCells, kGeoLatitude, kGeoJsonType, ...) live in
// "pg/geo_tokenizer_options.h", brought in by the include above.

// Segmentation

inline constexpr OptionInfo kBreak{
  "break", "alpha"sv, "Token boundary detection mode: all, graphic, alpha"};

// Delimiter

inline constexpr OptionInfo kDelimiter{
  "delimiter", OptionInfo::RequiredTag<std::string_view>{},
  "Token delimiter character or string"};

// Multi-Delimiter

inline constexpr OptionInfo kDelimiters{
  "delimiters", OptionInfo::RequiredTag<std::string_view>{},
  "The list of delimiters(e.g., \'\",\", \"|\", \"!\"\'"};

// Copy From

inline constexpr OptionInfo kFrom{"from",
                                  OptionInfo::RequiredTag<std::string_view>{},
                                  "the source tokenizer name"};

// Template

void CheckTemplate(std::string_view option, std::string_view value);
constexpr OptionInfo kTemplate{"template",
                               OptionInfo::RequiredTag<std::string_view>{},
                               "Tokenizer template type", CheckTemplate};

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

inline constexpr OptionInfo kBufferSize{
  "buffersize", 1024, "Term buffer size hint (characters per pass)"};

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

inline constexpr OptionInfo kTextOptions[] = {
  kLocale, kAccent, kStemming, kStopwords, kStopwordsPath, kCase};

inline constexpr OptionInfo kNGramOptions[] = {
  kMinGram, kMaxGram, kPreserveOriginal, kInputType, kStartMarker, kEndMarker};

inline constexpr OptionInfo kSparseNGramOptions[] = {kMaxNgramLength,
                                                     kCovering};

inline constexpr OptionInfo kNearestNeighborsOptions[] = {kModelLocation,
                                                          kTopK};

inline constexpr OptionInfo kStemmingOptions[] = {kLocale};

inline constexpr OptionInfo kStopwordsTokenizerOptions[] = {kStopwords, kHex};

inline constexpr OptionInfo kClassificationOptions[] = {kModelLocation, kTopK,
                                                        kThreshold};

inline constexpr OptionInfo kCollationOptions[] = {kLocale};

inline constexpr OptionInfo kDelimiterOptions[] = {kDelimiter};

inline constexpr OptionInfo kMultiDelimiterOptions[] = {kDelimiters};

inline constexpr OptionInfo kCopyFromOptions[] = {kFrom};

inline constexpr OptionInfo kMinHashOptions[] = {kNumHashes};

inline constexpr OptionInfo kWildcardOptions[] = {kNgramSize};

inline constexpr OptionInfo kShingleOptions[] = {
  kMinShingleSize,  kMaxShingleSize, kOutputUnigrams,
  kOutputUnigramsIfNoShingles, kStoreTokens, kFrequentWords, kFillerToken};

inline constexpr OptionInfo kNormOptions[] = {kLocale, kCase, kAccent};

inline constexpr OptionInfo kSegmentationOptions[] = {kCase, kBreak};

inline constexpr OptionInfo kEdgeNGramOptions[] = {kMinGram, kMaxGram,
                                                   kPreserveOriginal};

inline constexpr OptionInfo kPatternOptions[] = {kPattern, kGroup};

inline constexpr OptionInfo kPathHierarchyOptions[] = {
  kPathDelimiter, kPathReplacement, kReverse, kSkip, kBufferSize};

inline constexpr OptionInfo kSolrSynonymsOptions[] = {kSolrSynonyms};

inline constexpr OptionInfo kWordnetSynonymsOptions[] = {kWordnetSynonyms};

// Groups

inline constexpr OptionGroup kEdgeNGramGroup{
  "edgengram",
  kEdgeNGramOptions,
  {},
};
inline constexpr OptionGroup kTextSubgroups[] = {
  kEdgeNGramGroup,
};
inline constexpr OptionGroup kFeaturesGroup{
  "features",
  kFeaturesOptions,
  {},
};
inline constexpr OptionGroup kTextGroup{
  irs::analysis::TextTokenizer::type_name(),
  kTextOptions,
  kTextSubgroups,
};
inline constexpr OptionGroup kNGramGroup{
  irs::analysis::NGramTokenizerBase::type_name(),
  kNGramOptions,
  {},
};
inline constexpr OptionGroup kSparseNGramGroup{
  irs::analysis::SparseNGramTokenizer::type_name(),
  kSparseNGramOptions,
  {},
};
inline constexpr OptionGroup kNearestNeighborsGroup{
  irs::analysis::NearestNeighborsTokenizer::type_name(),
  kNearestNeighborsOptions,
  {},
};
inline constexpr OptionGroup kStemmingGroup{
  irs::analysis::StemmingTokenizer::type_name(),
  kStemmingOptions,
  {},
};
inline constexpr OptionGroup kStopwordsGroup{
  irs::analysis::StopwordsTokenizer::type_name(),
  kStopwordsTokenizerOptions,
  {},
};
inline constexpr OptionGroup kClassificationGroup{
  irs::analysis::ClassificationTokenizer::type_name(),
  kClassificationOptions,
  {},
};
inline constexpr OptionGroup kCollationGroup{
  irs::analysis::CollationTokenizer::type_name(),
  kCollationOptions,
  {},
};
inline constexpr OptionGroup kDelimiterGroup{
  irs::analysis::DelimitedTokenizer::type_name(),
  kDelimiterOptions,
  {},
};
inline constexpr OptionGroup kMultiDelimiterGroup{
  irs::analysis::MultiDelimitedTokenizer::type_name(),
  kMultiDelimiterOptions,
  {},
};
inline constexpr OptionGroup kCopyFromGroup{
  "copy_from",
  kCopyFromOptions,
  {},
};
inline constexpr OptionGroup kMinHashGroup{
  irs::analysis::MinHashTokenizer::type_name(),
  kMinHashOptions,
  {},
};
inline constexpr OptionGroup kWildcardGroup{
  irs::analysis::WildcardAnalyzer::type_name(),
  kWildcardOptions,
  {},
};
inline constexpr OptionGroup kShingleGroup{
  irs::analysis::ShingleAnalyzer::type_name(),
  kShingleOptions,
  {},
};
inline constexpr OptionGroup kNormGroup{
  irs::analysis::NormalizingTokenizer::type_name(),
  kNormOptions,
  {},
};
inline constexpr OptionGroup kSegmentationGroup{
  irs::analysis::SegmentationTokenizer::type_name(),
  kSegmentationOptions,
  {},
};
inline constexpr OptionGroup kPipelineGroup{
  irs::analysis::PipelineTokenizer::type_name(),
  {},
  {},
};
inline constexpr OptionGroup kPatternGroup{
  irs::analysis::PatternTokenizer::type_name(),
  kPatternOptions,
  {},
};
inline constexpr OptionGroup kPathHierarchyGroup{
  irs::analysis::PathHierarchyTokenizer::type_name(),
  kPathHierarchyOptions,
  {},
};
inline constexpr OptionGroup kUnionGroup{
  irs::analysis::UnionTokenizer::type_name(),
  {},
  {},
};
inline constexpr OptionGroup kKeywordGroup{
  irs::StringTokenizer::type_name(),
  {},
  {},
};
inline constexpr OptionGroup kSolrSynonymsGroup{
  irs::analysis::SolrSynonymsTokenizer::type_name(),
  kSolrSynonymsOptions,
  {},
};
inline constexpr OptionGroup kWordnetSynonymsGroup{
  irs::analysis::WordnetSynonymsTokenizer::type_name(),
  kWordnetSynonymsOptions,
  {},
};

inline constexpr OptionGroup kTokenizerSubgroups[] = {
  kFeaturesGroup,       kTextGroup,
  kNGramGroup,          kNearestNeighborsGroup,
  kStemmingGroup,       kStopwordsGroup,
  kClassificationGroup, kCollationGroup,
  kDelimiterGroup,      kMultiDelimiterGroup,
  kMinHashGroup,        kWildcardGroup,
  kShingleGroup,        kNormGroup,
  kSegmentationGroup,
  kPipelineGroup,       kPatternGroup,
  kPathHierarchyGroup,  kUnionGroup,
  kCopyFromGroup,       kGeoPointGroup,
  kGeoJsonGroup,        kKeywordGroup,
  kSolrSynonymsGroup,   kWordnetSynonymsGroup,
  kSparseNGramGroup,
};

}  // namespace sdb::pg::tokenizer_options
