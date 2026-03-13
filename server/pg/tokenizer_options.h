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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/utils/type_id.hpp>

#include "pg/option_help.h"

namespace sdb::pg::tokenizer_options {

using namespace std::string_view_literals;

// Features

inline constexpr OptionInfo kNormFeature{irs::Type<irs::Norm>::name(), false,
                                         "Enables norm feature in index"};

inline constexpr OptionInfo kFreqFeature{irs::Type<irs::FreqAttr>::name(),
                                         false,
                                         "Enables frequency feature in index"};

inline constexpr OptionInfo kPosFeature{irs::Type<irs::PosAttr>::name(), false,
                                        "Enables position feature in index"};

inline constexpr OptionInfo kOffsetFeature{
  irs::Type<irs::OffsAttr>::name(), false, "Enables offset feature in index"};

// Common

inline constexpr OptionInfo kLocale{"locale", ""sv,
                                    "ICU locale string (e.g. en_US.UTF-8)"};

inline constexpr OptionInfo kAccent{"accent", true, "Preserve accent marks"};

inline constexpr OptionInfo kCase{
  "case", "none"sv, "Text case conversion: none, lower, upper",
  [](const OptionInfo::DefaultValueT& value) {
    auto str = std::get<std::string_view>(value);
    return str == "none" || str == "lower" || str == "upper";
  }};

inline constexpr OptionInfo kModelLocation{"modellocation", ""sv,
                                           "Path to the ML model file"};

inline constexpr OptionInfo kTopK{"topk", 1, "Number of top results to return"};

// Text

inline constexpr OptionInfo kStemming{"stemming", true,
                                      "Apply stemming to tokens"};

inline constexpr OptionInfo kStopwords{
  "stopwords", ""sv, "Comma-separated list of inline stop words"};

inline constexpr OptionInfo kStopwordsPath{
  "stopwordspath", OptionInfo::Type::String,
  "Path to file containing stop words"};

// NGram

inline constexpr OptionInfo kMinGram{"mingram", 2, "Minimum n-gram length"};

inline constexpr OptionInfo kMaxGram{"maxgram", 3, "Maximum n-gram length"};

inline constexpr OptionInfo kPreserveOriginal{
  "preserveoriginal", false, "Emit the original token alongside n-grams"};

inline constexpr OptionInfo kInputType{"inputtype", "utf8"sv,
                                       "Input stream encoding: binary, utf8"};

inline constexpr OptionInfo kStartMarker{
  "startmarker", OptionInfo::Type::String,
  "Prefix marker appended at n-gram boundary"};

inline constexpr OptionInfo kEndMarker{
  "endmarker", OptionInfo::Type::String,
  "Suffix marker appended at n-gram boundary"};

// Classification

inline constexpr OptionInfo kThreshold{
  "threshold", 0.0, "Minimum confidence score (0.0 to 1.0)",
  [](const OptionInfo::DefaultValueT& value) {
    auto val = std::get<double>(value);
    return 0. <= val && val <= 1.;
  }};

// Stopwords tokenizer

inline constexpr OptionInfo kHex{"hex", false,
                                 "Treat stop words as hex-encoded strings"};

// MinHash

inline constexpr OptionInfo kNumHashes{"numhashes", 1,
                                       "Number of hash functions to use"};

// Segmentation

inline constexpr OptionInfo kBreak{
  "break", "alpha"sv, "Token boundary detection mode: all, graphic, alpha"};

// Delimiter

inline constexpr OptionInfo kDelimiter{"delimiter", OptionInfo::Type::String,
                                       "Token delimiter character or string"};

// Per-tokenizer option arrays

inline constexpr OptionInfo kFeaturesOptions[] = {kNormFeature, kOffsetFeature,
                                                  kPosFeature, kFreqFeature};

inline constexpr OptionInfo kTextOptions[] = {
  kLocale, kAccent, kStemming, kStopwords, kStopwordsPath, kCase};

inline constexpr OptionInfo kNGramOptions[] = {
  kMinGram, kMaxGram, kPreserveOriginal, kInputType, kStartMarker, kEndMarker};

inline constexpr OptionInfo kNearestNeighborsOptions[] = {kModelLocation,
                                                          kTopK};

inline constexpr OptionInfo kStemmingOptions[] = {kLocale};

inline constexpr OptionInfo kStopwordsTokenizerOptions[] = {kStopwords, kHex};

inline constexpr OptionInfo kClassificationOptions[] = {kModelLocation, kTopK,
                                                        kThreshold};

inline constexpr OptionInfo kCollationOptions[] = {kLocale};

inline constexpr OptionInfo kDelimiterOptions[] = {kDelimiter};

inline constexpr OptionInfo kMinHashOptions[] = {kNumHashes};

inline constexpr OptionInfo kNormOptions[] = {kLocale, kCase, kAccent};

inline constexpr OptionInfo kSegmentationOptions[] = {kCase, kBreak};

inline constexpr OptionInfo kEdgeNGramOptions[] = {kMinGram, kMaxGram,
                                                   kPreserveOriginal};

// Groups

inline constexpr OptionGroup kEdgeNGramGroup{
  "edgengram", kEdgeNGramOptions, {}};
inline constexpr OptionGroup kTextSubgroups[] = {kEdgeNGramGroup};
inline constexpr OptionGroup kFeaturesGroup{"features", kFeaturesOptions, {}};
inline constexpr OptionGroup kTextGroup{"text", kTextOptions, kTextSubgroups};
inline constexpr OptionGroup kNGramGroup{"ngram", kNGramOptions, {}};
inline constexpr OptionGroup kNearestNeighborsGroup{
  "nearest_neighbors", kNearestNeighborsOptions, {}};
inline constexpr OptionGroup kStemmingGroup{"stem", kStemmingOptions, {}};
inline constexpr OptionGroup kStopwordsGroup{
  "stopwords", kStopwordsTokenizerOptions, {}};
inline constexpr OptionGroup kClassificationGroup{
  "classification", kClassificationOptions, {}};
inline constexpr OptionGroup kCollationGroup{
  "collation", kCollationOptions, {}};
inline constexpr OptionGroup kDelimiterGroup{
  "delimiter", kDelimiterOptions, {}};
inline constexpr OptionGroup kMinHashGroup{"minhash", kMinHashOptions, {}};
inline constexpr OptionGroup kNormGroup{"norm", kNormOptions, {}};
inline constexpr OptionGroup kSegmentationGroup{
  "segmentation", kSegmentationOptions, {}};

inline constexpr OptionGroup kTokenizerSubgroups[] = {
  kFeaturesGroup,         kTextGroup,      kNGramGroup,
  kNearestNeighborsGroup, kStemmingGroup,  kStopwordsGroup,
  kClassificationGroup,   kCollationGroup, kDelimiterGroup,
  kMinHashGroup,          kNormGroup,      kSegmentationGroup};

}  // namespace sdb::pg::tokenizer_options
