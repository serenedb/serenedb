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

#pragma once

#include <memory>
#include <type_traits>
#include <utility>
#include <variant>

#include "analyzer.hpp"
#include "basics/serializer.h"
#include "classification_tokenizer.hpp"
#include "collation_tokenizer.hpp"
#include "delimited_tokenizer.hpp"
#include "geo_analyzer.hpp"
#include "minhash_tokenizer.hpp"
#include "multi_delimited_tokenizer.hpp"
#include "nearest_neighbors_tokenizer.hpp"
#include "ngram_tokenizer.hpp"
#include "normalizing_tokenizer.hpp"
#include "path_hierarchy_tokenizer.hpp"
#include "pattern_tokenizer.hpp"
#include "pipeline_tokenizer.hpp"
#include "segmentation_tokenizer.hpp"
#include "solr_synonyms_tokenizer.hpp"
#include "sparse_ngram_tokenizer.hpp"
#include "split_by_non_alpha_tokenizer.hpp"
#include "stemming_tokenizer.hpp"
#include "stopwords_tokenizer.hpp"
#include "text_tokenizer.hpp"
#include "tokenizers.hpp"
#include "union_tokenizer.hpp"
#include "wildcard_analyzer.hpp"
#include "wordnet_synonyms_tokenizer.hpp"

namespace irs::analysis {

struct TokenizerConfig {
  std::variant<StringTokenizer::Options, TextTokenizer::Options,
               StemmingTokenizer::Options, DelimitedTokenizer::Options,
               MultiDelimitedTokenizer::Options, PatternTokenizer::Options,
               PathHierarchyTokenizer::Options, NGramTokenizerBase::Options,
               NormalizingTokenizer::Options, SegmentationTokenizer::Options,
               StopwordsTokenizer::Options, ClassificationTokenizer::Options,
               CollationTokenizer::Options, SolrSynonymsTokenizer::Options,
               WordnetSynonymsTokenizer::Options,
               NearestNeighborsTokenizer::Options, GeoPointAnalyzer::Options,
               GeoJsonAnalyzer::Options, WildcardAnalyzer::Options,
               MinHashTokenizer::Options, PipelineTokenizer::Options,
               UnionTokenizer::Options, SparseNGramTokenizer::Options,
               SplitByNonAlphaTokenizer::Options>
    config;
};

template<typename Context>
void SerdeWrite(Context ctx, const TokenizerConfig& cfg) {
  sdb::basics::WriteTuple(ctx.io(), cfg.config, ctx.arg());
}

template<typename Context>
void SerdeRead(Context ctx, TokenizerConfig& cfg) {
  sdb::basics::ReadTuple(ctx.io(), cfg.config, ctx.arg());
}

TokenizerConfig Clone(const TokenizerConfig& cfg);

namespace detail {

inline std::unique_ptr<TokenizerConfig> CloneChild(
  const std::unique_ptr<TokenizerConfig>& child) {
  if (!child) {
    return nullptr;
  }
  return std::make_unique<TokenizerConfig>(Clone(*child));
}

}  // namespace detail

inline TokenizerConfig Clone(const TokenizerConfig& cfg) {
  TokenizerConfig out;
  std::visit(
    [&]<typename Options>(const Options& opts) {
      if constexpr (std::is_same_v<Options, PipelineTokenizer::Options> ||
                    std::is_same_v<Options, UnionTokenizer::Options>) {
        Options copy;
        copy.children.reserve(opts.children.size());
        for (const auto& child : opts.children) {
          copy.children.push_back(detail::CloneChild(child));
        }
        out.config = std::move(copy);
      } else if constexpr (std::is_same_v<Options, WildcardAnalyzer::Options>) {
        Options copy;
        copy.ngram_size = opts.ngram_size;
        copy.base_analyzer = detail::CloneChild(opts.base_analyzer);
        out.config = std::move(copy);
      } else if constexpr (std::is_same_v<Options, MinHashTokenizer::Options>) {
        Options copy;
        copy.num_hashes = opts.num_hashes;
        copy.analyzer = detail::CloneChild(opts.analyzer);
        out.config = std::move(copy);
      } else {
        out.config = opts;
      }
    },
    cfg.config);
  return out;
}

// Takes the config by value; callers preserving a stored config pass Clone().
inline Analyzer::ptr CreateAnalyzer(TokenizerConfig cfg) {
  return std::visit(
    [](auto&& opts) -> Analyzer::ptr {
      using Options = std::decay_t<decltype(opts)>;
      return Options::Owner::Make(std::move(opts));
    },
    std::move(cfg.config));
}

}  // namespace irs::analysis
