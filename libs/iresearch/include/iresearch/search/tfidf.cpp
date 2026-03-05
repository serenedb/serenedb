////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "tfidf.hpp"

#include <absl/container/inlined_vector.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>
#include <vpack/vpack.h>

#include <cmath>
#include <cstddef>
#include <string_view>

#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/wand_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/scorer_impl.hpp"
#include "iresearch/search/scorers.hpp"
#include "vpack/serializer.h"

namespace irs {
namespace {

template<typename T>
constexpr const T* TryGetValue(const T* value) noexcept {
  return value;
}

constexpr std::nullptr_t TryGetValue(utils::Empty /*value*/) noexcept {
  return nullptr;
}

struct TFIDFFieldCollector final : FieldCollector {
  // number of documents containing the matched field
  // (possibly without matching terms)
  uint64_t docs_with_field = 0;

  void collect(const SubReader& /*segment*/,
               const TermReader& field) noexcept final {
    docs_with_field += field.docs_count();
  }

  void reset() noexcept final { docs_with_field = 0; }

  void collect(bytes_view in) final {
    ByteRefIterator itr{in};
    const auto docs_with_field_value = vread<uint64_t>(itr);
    if (itr.pos != itr.end) {
      throw IoError{"input not read fully"};
    }
    docs_with_field += docs_with_field_value;
  }

  void write(DataOutput& out) const final { out.WriteV64(docs_with_field); }
};

Scorer::ptr MakeFromBool(const vpack::Slice slice) {
  SDB_ASSERT(slice.isBool());

  return std::make_unique<TFIDF>(slice.getBool());
}

struct Params {
  bool withNorms = TFIDF::WITH_NORMS();  // NOLINT
};

Scorer::ptr MakeFromObject(const vpack::Slice slice) {
  Params params;
  auto r = vpack::ReadObjectNothrow(slice, params,
                                    {
                                      .skip_unknown = true,
                                      .strict = false,
                                    });
  if (!r.ok()) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Error '", r.errorMessage(),
                   "' while constructing tfidf scorer from VPack arguments"));
    return {};
  }

  return std::make_unique<TFIDF>(params.withNorms);
}

Scorer::ptr MakeFromArray(const vpack::Slice slice) {
  Params params;
  auto r = vpack::ReadTupleNothrow(slice, params);
  if (!r.ok()) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Error '", r.errorMessage(),
                   "' while constructing bm25 scorer from VPack arguments"));
    return {};
  }

  return std::make_unique<TFIDF>(params.withNorms);
}

Scorer::ptr MakeVPack(const vpack::Slice slice) {
  switch (slice.type()) {
    case vpack::ValueType::Bool:
      return MakeFromBool(slice);
    case vpack::ValueType::Object:
      return MakeFromObject(slice);
    case vpack::ValueType::Array:
      return MakeFromArray(slice);
    default:  // wrong type
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Invalid VPack arguments passed while constructing tfidf scorer, "
        "arguments");
      return nullptr;
  }
}

Scorer::ptr MakeVPack(std::string_view args) {
  if (IsNull(args)) {
    // default args
    return std::make_unique<TFIDF>();
  } else {
    vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
    return MakeVPack(slice);
  }
}

Scorer::ptr MakeJson(std::string_view args) {
  if (IsNull(args)) {
    // default args
    return std::make_unique<TFIDF>();
  } else {
    try {
      auto vpack = vpack::Parser::fromJson(args.data(), args.size());
      return MakeVPack(vpack->slice());
    } catch (const vpack::Exception& ex) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Caught error '", ex.what(),
                     "' while constructing VPack from JSON for tfidf scorer"));
    } catch (...) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Caught error while constructing VPack from JSON for tfidf scorer");
    }
    return nullptr;
  }
}

// Helper functions

IRS_FORCE_INLINE score_t TfIdf(uint32_t freq, score_t idf) noexcept {
  // TODO(gnusi): do we need sqrt?
  return std::sqrt(static_cast<score_t>(freq)) * idf;
}

template<ScoreMergeType MergeType, bool HasNorm, bool HasBoost>
IRS_FORCE_INLINE void TfIdf(score_t* IRS_RESTRICT res, scores_size_t n,
                            const uint32_t* IRS_RESTRICT freq,
                            [[maybe_unused]] const uint32_t* IRS_RESTRICT norm,
                            [[maybe_unused]] const score_t* IRS_RESTRICT boost,
                            score_t idf) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const auto r = [&] IRS_FORCE_INLINE {
      if constexpr (HasNorm && HasBoost) {
        return boost[i] * TfIdf(freq[i], idf) /
               std::sqrt(static_cast<score_t>(norm[i]));
      } else if constexpr (HasNorm) {
        return TfIdf(freq[i], idf) / std::sqrt(static_cast<score_t>(norm[i]));
      } else if constexpr (HasBoost) {
        return boost[i] * TfIdf(freq[i], idf);
      } else {
        return TfIdf(freq[i], idf);
      }
    }();
    Merge<MergeType>(res[i], r);
  }
}

template<bool HasNorm, bool HasFilterBoost>
struct TfIdfScore : public ScoreOperator {
  TfIdfScore(const uint32_t* norm, score_t boost, TFIDFStats idf,
             const FreqBlockAttr* freq,
             const score_t* filter_boost = nullptr) noexcept
    : freq{freq},
      filter_boost{filter_boost},
      norm{norm},
      idf{boost * idf.value} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    TfIdf<MergeType, HasNorm, HasFilterBoost>(
      res, n, freq->value, TryGetValue(norm), TryGetValue(filter_boost), idf);
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

  const FreqBlockAttr* freq;
  [[no_unique_address]] utils::Need<HasFilterBoost, const score_t*>
    filter_boost;
  [[no_unique_address]] utils::Need<HasNorm, const uint32_t*> norm;
  score_t idf;  // precomputed : boost * idf
};

}  // namespace

void TFIDF::collect(byte_type* stats_buf, const FieldCollector* field,
                    const TermCollector* term) const {
  const auto* field_ptr = sdb::basics::downCast<TFIDFFieldCollector>(field);
  const auto* term_ptr = sdb::basics::downCast<TermCollectorImpl>(term);

  // nullptr possible if e.g. 'all' filter
  const auto docs_with_field = field_ptr ? field_ptr->docs_with_field : 0;
  // nullptr possible if e.g.'by_column_existence' filter
  const auto docs_with_term = term_ptr ? term_ptr->docs_with_term : 0;
  // TODO(mbkkt) SDB_ASSERT(docs_with_field >= docs_with_term);

  auto* idf = stats_cast(stats_buf);
  idf->value += static_cast<score_t>(
    std::log1p((docs_with_field + 1.0) / (docs_with_term + 1.0)));
  // TODO(mbkkt) SDB_ASSERT(idf.value >= 0.f);
}

ScoreFunction TFIDF::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);

  if (!freq) {
    if (!_boost_as_score || 0.f == ctx.boost) {
      return ScoreFunction::Default();
    }

    // if there is no frequency then all the
    // scores will be the same (e.g. filter irs::all)
    return ScoreFunction::Constant(ctx.boost);
  }

  auto* filter_boost = [&] {
    auto* attr = irs::get<BoostBlockAttr>(ctx.doc_attrs);
    return attr ? attr->value : nullptr;
  }();

  const uint32_t* norm = nullptr;
  if (_normalize) {
    norm = [&] {
      auto* attr = irs::get<Norm>(ctx.doc_attrs);
      return attr ? &attr->value : nullptr;
    }();

    // Fallback to reading from columnstore
    if (!norm && ctx.fetcher) {
      norm = ctx.fetcher->AddNorms(ctx.segment.column(ctx.field.norm));
    }
  }

  return ResolveBool(norm != nullptr, [&]<bool HasNorms>() {
    return ResolveBool(filter_boost != nullptr, [&]<bool HasBoost>() {
      const auto* stats = stats_cast(ctx.stats);
      return ScoreFunction::Make<TfIdfScore<HasNorms, HasBoost>>(
        norm, ctx.boost, *stats, freq, filter_boost);
    });
  });
}

TermCollector::ptr TFIDF::PrepareTermCollector() const {
  return std::make_unique<TermCollectorImpl>();
}

FieldCollector::ptr TFIDF::PrepareFieldCollector() const {
  return std::make_unique<TFIDFFieldCollector>();
}

WandWriter::ptr TFIDF::prepare_wand_writer(size_t max_levels) const {
  if (_normalize) {
    // idf * sqrt(tf) / sqrt(dl)
    // sqrt(tf) / sqrt(dl)
    // tf / dl
    return std::make_unique<FreqNormWriter<kWandTagDivNorm>>(max_levels);
  }
  return std::make_unique<FreqNormWriter<kWandTagMaxFreq>>(max_levels);
}

WandSource::ptr TFIDF::prepare_wand_source() const {
  if (_normalize) {
    return std::make_unique<FreqNormSource<kWandTagNorm>>();
  }
  return std::make_unique<FreqNormSource<kWandTagFreq>>();
}

Scorer::WandType TFIDF::wand_type() const noexcept {
  if (_normalize) {
    return WandType::DivNorm;
  }
  return WandType::MaxFreq;
}

bool TFIDF::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = sdb::basics::downCast<TFIDF>(other);
  return p._normalize == _normalize;
}

void TFIDF::init() {
  REGISTER_SCORER_JSON(TFIDF, MakeJson);
  REGISTER_SCORER_VPACK(TFIDF, MakeVPack);
}

}  // namespace irs
