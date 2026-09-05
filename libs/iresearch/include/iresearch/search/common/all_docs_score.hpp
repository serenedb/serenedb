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

#include <optional>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/score_provider.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

inline AttributeProvider& NoAttributes() noexcept {
  static struct Provider final : AttributeProvider {
    Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }
  } provider;
  return provider;
}

inline const FieldMeta& NoField() noexcept { return FieldMeta::kEmpty; }

inline bool AllDocsConstant(const ScoreArgs& args) noexcept {
  return args.stats == nullptr || !ScoresPerDoc(args.scorer);
}

inline ScoreFunction AllDocsScorer(const SubReader& segment,
                                   const ScoreArgs& args) {
  SDB_ASSERT(args.scorer != nullptr);
  SDB_ASSERT(args.stats != nullptr);
  return args.scorer->PrepareScorer({
    .segment = segment,
    .field = NoField(),
    .doc_attrs = NoAttributes(),
    .fetcher = args.fetcher,
    .stats = args.stats,
    .boost = args.boost,
  });
}

inline score_t AllDocsScore(const SubReader& segment, const ScoreArgs& args) {
  SDB_ASSERT(args.scorer != nullptr);
  if (args.stats == nullptr) {
    return 0;
  }
  return AllDocsScorer(segment, args).Score();
}

inline score_t ConstantTermScore(const SubReader& segment,
                                 const TermReader& field,
                                 const ScoreArgs& args) {
  SDB_ASSERT(args.scorer != nullptr);
  LeafProvider provider;
  static constinit uint32_t gFreq = 1;
  provider.freq.value = &gFreq;
  auto score = args.scorer->PrepareScorer({
    .segment = segment,
    .field = field.meta(),
    .doc_attrs = provider,
    .fetcher = args.fetcher,
    .stats = args.stats,
    .boost = args.boost,
  });
  return score.Score();
}

inline std::optional<score_t> ConstantOf(const SubReader& segment,
                                         const TermReader& field,
                                         const ScoreArgs& args) {
  if (args.scorer == nullptr || args.stats == nullptr) {
    return score_t{0};
  }
  if (const auto value = args.scorer->Constant({
        .segment = segment,
        .field = field.meta(),
        .doc_attrs = NoAttributes(),
        .fetcher = args.fetcher,
        .stats = args.stats,
        .boost = args.boost,
      })) {
    return value;
  }
  const auto features = field.meta().index_features;
  if (!FeaturesHaveFreq(features) &&
      IndexFeatures::None !=
        (args.scorer->GetIndexFeatures() & IndexFeatures::Freq)) {
    SDB_ASSERT(IndexFeatures::None == (features & IndexFeatures::Norm));
    return ConstantTermScore(segment, field, args);
  }
  return std::nullopt;
}

inline score_t ConstantTermOf(const SubReader& segment, const TermReader& field,
                              const ScoreArgs& args) {
  if (args.scorer == nullptr || args.stats == nullptr) {
    return 0;
  }
  if (const auto value = args.scorer->Constant({
        .segment = segment,
        .field = field.meta(),
        .doc_attrs = NoAttributes(),
        .fetcher = args.fetcher,
        .stats = args.stats,
        .boost = args.boost,
      })) {
    return *value;
  }
  return ConstantTermScore(segment, field, args);
}

inline score_t SingleDocScore(const SubReader& segment, const TermReader& field,
                              doc_id_t doc, uint32_t freq,
                              const ScoreArgs& args) {
  if (args.stats == nullptr) {
    return 0;
  }
  SDB_ASSERT(args.scorer != nullptr);
  LeafProvider provider;
  if (IndexFeatures::None ==
      (field.meta().index_features & IndexFeatures::Freq)) {
    freq = 1;
  }
  provider.freq.value = &freq;
  auto score = args.scorer->PrepareScorer({
    .segment = segment,
    .field = field.meta(),
    .doc_attrs = provider,
    .fetcher = args.fetcher,
    .stats = args.stats,
    .boost = args.boost,
  });
  if (args.fetcher != nullptr) {
    args.fetcher->Fetch(doc);
  }
  return score.Score();
}

}  // namespace irs::search
