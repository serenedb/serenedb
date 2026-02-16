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

#include "bm25.hpp"

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>
#include <vpack/vpack.h>

#include <cstdint>
#include <exception>
#include <ranges>
#include <utility>

#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/wand_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/scorer_impl.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/attribute_provider.hpp"
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

struct BM25FieldCollector final : FieldCollector {
  // number of documents containing the matched field
  // (possibly without matching terms)
  uint64_t docs_with_field = 0;
  // number of terms for processed field
  uint64_t total_term_freq = 0;

  void collect(const SubReader& /*segment*/,
               const TermReader& field) noexcept final {
    docs_with_field += field.docs_count();
    if (const auto* freq = irs::get<FreqAttr>(field)) {
      total_term_freq += freq->value;
    }
  }

  void reset() noexcept final {
    docs_with_field = 0;
    total_term_freq = 0;
  }

  void collect(bytes_view in) final {
    ByteRefIterator itr{in};
    const auto docs_with_field_value = vread<uint64_t>(itr);
    const auto total_term_freq_value = vread<uint64_t>(itr);
    if (itr.pos != itr.end) {
      throw IoError{"input not read fully"};
    }
    docs_with_field += docs_with_field_value;
    total_term_freq += total_term_freq_value;
  }

  void write(DataOutput& out) const final {
    out.WriteV64(docs_with_field);
    out.WriteV64(total_term_freq);
  }
};

struct ObjectParams {
  float_t k = BM25::K();
  float_t b = BM25::B();
  bool boost_as_score = BM25::BOOST_AS_SCORE();
  bool approximate = true;
};

Scorer::ptr MakeFromObject(const vpack::Slice slice) {
  ObjectParams params;
  auto r = vpack::ReadObjectNothrow(slice, params,
                                    {
                                      .skip_unknown = true,
                                      .strict = false,
                                    });
  if (!r.ok()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Error '", r.errorMessage(),
              "' while constructing bm25 scorer from VPack arguments");
    return {};
  }

  return std::make_unique<BM25>(params.k, params.b, params.boost_as_score,
                                params.approximate);
}

struct ArrayParams {
  float_t k = BM25::K();
  float_t b = BM25::B();
};

Scorer::ptr MakeFromArray(const vpack::Slice slice) {
  ArrayParams params;
  auto r = vpack::ReadTupleNothrow(slice, params);
  if (!r.ok()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Error '", r.errorMessage(),
              "' while constructing bm25 scorer from VPack arguments");
    return {};
  }

  return std::make_unique<BM25>(params.k, params.b);
}

Scorer::ptr MakeVPack(const vpack::Slice slice) {
  switch (slice.type()) {
    case vpack::ValueType::Object:
      return MakeFromObject(slice);
    case vpack::ValueType::Array:
      return MakeFromArray(slice);
    default:  // wrong type
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Invalid VPack arguments passed while constructing bm25 scorer");
      return nullptr;
  }
}

Scorer::ptr MakeVPack(std::string_view args) {
  if (IsNull(args)) {
    // default args
    return std::make_unique<irs::BM25>();
  }
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

Scorer::ptr MakeJson(std::string_view args) {
  if (IsNull(args)) {
    // default args
    return std::make_unique<irs::BM25>();
  }
  try {
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing VPack from JSON for bm25 scorer"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while constructing VPack from JSON for bm25 scorer");
  }
  return nullptr;
}

template<typename T>
IRS_FORCE_INLINE void Bm1Boost(T* IRS_RESTRICT res, size_t n,
                               const score_t* IRS_RESTRICT boost,
                               float_t num) noexcept {
  for (size_t i = 0; i < n; ++i) {
    res[i] = boost[i] * num;
  }
}

template<bool HasBoost, typename T>
IRS_FORCE_INLINE void Bm15(T* IRS_RESTRICT res, size_t n,
                           const uint32_t* IRS_RESTRICT freq,
                           [[maybe_unused]] const score_t* IRS_RESTRICT boost,
                           float_t num, float_t c1) noexcept {
  SDB_ASSERT(c1 != 0.f);
  for (size_t i = 0; i < n; ++i) {
    const auto c0 = [&] {
      if constexpr (HasBoost) {
        SDB_ASSERT(boost);
        return boost[i] * num;
      } else {
        return num;
      }
    }();
    res[i] = c0 - c0 / (1.f + static_cast<float_t>(freq[i]) / c1);
  }
}

template<bool HasBoost, typename T>
IRS_FORCE_INLINE void Bm25(T* res, size_t n, const uint32_t* IRS_RESTRICT freq,
                           const uint32_t* IRS_RESTRICT norm,
                           [[maybe_unused]] const score_t* IRS_RESTRICT boost,
                           float_t num, float_t norm_const,
                           float_t norm_length) noexcept {
  for (size_t i = 0; i < n; ++i) {
    const auto c0 = [&] {
      if constexpr (HasBoost) {
        SDB_ASSERT(boost);
        return boost[i] * num;
      } else {
        return num;
      }
    }();
    const float_t c1 = norm_const + norm_length * static_cast<float_t>(norm[i]);
    res[i] = c0 - c0 * c1 / (c1 + static_cast<float_t>(freq[i]));
  }
}

template<bool HasFilterBoost>
struct Bm1Score : public ScoreOperator {
  Bm1Score(float_t k, score_t boost, const BM25Stats& stats,
           const score_t* fb) noexcept
    : filter_boost{fb}, num{boost * (k + 1) * stats.idf} {}

  score_t Score() noexcept final {
    score_t res;
    if constexpr (HasFilterBoost) {
      Bm1Boost(&res, 1, filter_boost, num);
    } else {
      res = 0;
    }
    return res;
  }

  void Score(score_t* res, size_t n) noexcept final {
    if constexpr (HasFilterBoost) {
      Bm1Boost(res, n, filter_boost, num);
    } else {
      std::memset(res, 0, sizeof(score_t) * n);
    }
  }

  void ScoreBlock(score_t* res) noexcept final {
    if constexpr (HasFilterBoost) {
      Bm1Boost(res, kScoreBlock, filter_boost, num);
    } else {
      std::memset(res, 0, sizeof(score_t) * kScoreBlock);
    }
  }

  void ScorePostingBlock(score_t* res) noexcept final {
    if constexpr (HasFilterBoost) {
      Bm1Boost(res, kPostingBlock, filter_boost, num);
    } else {
      std::memset(res, 0, sizeof(score_t) * kPostingBlock);
    }
  }

  [[no_unique_address]] utils::Need<HasFilterBoost, const score_t*>
    filter_boost;
  [[no_unique_address]] utils::Need<HasFilterBoost, float_t>
    num;  // partially precomputed numerator : boost * (k + 1) * idf
};

template<bool HasFilterBoost>
struct Bm15Score : public ScoreOperator {
  Bm15Score(float_t k, score_t boost, const BM25Stats& stats,
            const FreqBlockAttr* freq, const score_t* fb) noexcept
    : filter_boost{fb},
      num{boost * (k + 1) * stats.idf},
      norm_const{stats.norm_const},
      freq{freq} {
    SDB_ASSERT(this->freq);
  }

  score_t Score() noexcept final {
    score_t res;
    Bm15<HasFilterBoost>(&res, 1, freq->value, TryGetValue(filter_boost), num,
                         norm_const);
    return res;
  }
  void Score(score_t* res, size_t n) noexcept final {
    Bm15<HasFilterBoost>(res, n, freq->value, TryGetValue(filter_boost), num,
                         norm_const);
  }
  void ScoreBlock(score_t* res) noexcept final {
    Bm15<HasFilterBoost>(res, kScoreBlock, freq->value,
                         TryGetValue(filter_boost), num, norm_const);
  }
  void ScorePostingBlock(score_t* res) noexcept final {
    Bm15<HasFilterBoost>(res, kPostingBlock, freq->value,
                         TryGetValue(filter_boost), num, norm_const);
  }

  [[no_unique_address]] utils::Need<HasFilterBoost, const score_t*>
    filter_boost;
  float_t num;  // partially precomputed numerator : boost * (k + 1) * idf
  float_t norm_const;         // 'k' factor
  const FreqBlockAttr* freq;  // document frequency
};

template<bool HasFilterBoost>
struct Bm25Score : public ScoreOperator {
  Bm25Score(float_t k, score_t boost, const BM25Stats& stats,
            const FreqBlockAttr* freq, const uint32_t* norm,
            const score_t* filter_boost) noexcept
    : filter_boost{filter_boost},
      num{boost * (k + 1) * stats.idf},
      norm_const{stats.norm_const},
      freq{freq},
      norm{norm},
      norm_length{stats.norm_length} {}

  score_t Score() noexcept final {
    score_t res;
    Bm25<HasFilterBoost>(&res, 1, freq->value, norm, TryGetValue(filter_boost),
                         num, norm_const, norm_length);
    return res;
  }

  void Score(score_t* res, size_t n) noexcept final {
    Bm25<HasFilterBoost>(res, n, freq->value, norm, TryGetValue(filter_boost),
                         num, norm_const, norm_length);
  }

  void ScoreBlock(score_t* res) noexcept final {
    Bm25<HasFilterBoost>(res, kScoreBlock, freq->value, norm,
                         TryGetValue(filter_boost), num, norm_const,
                         norm_length);
  }

  void ScorePostingBlock(score_t* res) noexcept final {
    Bm25<HasFilterBoost>(res, kScoreBlock, freq->value, norm,
                         TryGetValue(filter_boost), num, norm_const,
                         norm_length);
  }

  [[no_unique_address]] utils::Need<HasFilterBoost, const score_t*>
    filter_boost;
  float_t num;  // partially precomputed numerator : boost * (k + 1) * idf
  float_t norm_const;         // 'k' factor
  const FreqBlockAttr* freq;  // document frequency
  const uint32_t* norm;
  float_t norm_length;  // precomputed 'k*b/avg_dl'
};

}  // namespace

void BM25::collect(byte_type* stats_buf, const irs::FieldCollector* field,
                   const irs::TermCollector* term) const {
  auto* stats = stats_cast(stats_buf);

  const auto* field_ptr = sdb::basics::downCast<BM25FieldCollector>(field);
  const auto* term_ptr = sdb::basics::downCast<TermCollectorImpl>(term);

  // nullptr possible if e.g. 'all' filter
  const auto docs_with_field = field_ptr ? field_ptr->docs_with_field : 0;
  // nullptr possible if e.g.'by_column_existence' filter
  const auto docs_with_term = term_ptr ? term_ptr->docs_with_term : 0;
  // nullptr possible if e.g. 'all' filter
  const auto total_term_freq = field_ptr ? field_ptr->total_term_freq : 0;

  // precomputed idf value
  stats->idf += float_t(
    std::log1p((static_cast<double>(docs_with_field - docs_with_term) + 0.5) /
               (static_cast<double>(docs_with_term) + 0.5)));
  SDB_ASSERT(stats->idf >= 0.f);

  // - stats were already initialized
  if (!NeedsNorm()) {
    stats->norm_const = _k;
    return;
  }

  // precomputed length norm
  const float_t kb = _k * _b;

  stats->norm_const = _k - kb;
  if (total_term_freq && docs_with_field) {
    const auto avg_dl = static_cast<float_t>(total_term_freq) /
                        static_cast<float_t>(docs_with_field);
    stats->norm_length = kb / avg_dl;
  } else {
    stats->norm_length = kb;
  }
}

FieldCollector::ptr BM25::PrepareFieldCollector() const {
  return std::make_unique<BM25FieldCollector>();
}

ScoreFunction BM25::PrepareSingleScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqAttr>(ctx.doc_attrs);

  if (!freq) {
    if (!_boost_as_score || 0.f == ctx.boost) {
      return ScoreFunction::Default();
    }

    // if there is no frequency then all the scores
    // will be the same (e.g. filter irs::all)
    return ScoreFunction::Constant(ctx.boost);
  }

  auto* filter_boost = [&] {
    auto* attr = irs::get<FilterBoost>(ctx.doc_attrs);
    return attr ? &attr->value : nullptr;
  }();

  auto* stats = stats_cast(ctx.stats);

  return ResolveBool(filter_boost != nullptr, [&]<bool HasBoost>() {
    if (IsBM1()) {
      return ScoreFunction::Make<Bm1Score<HasBoost>>(_k, ctx.boost, *stats,
                                                     filter_boost);
    }

    if (IsBM15()) {
      return ScoreFunction::Make<Bm15Score<HasBoost>>(
        _k, ctx.boost, *stats, nullptr, filter_boost);  // TODO(gnusi): fix
    }

    auto* norm = [&] {
      auto* attr = irs::get<Norm>(ctx.doc_attrs);
      return attr ? &attr->value : nullptr;
    }();

    if (!norm) {
      static constexpr uint32_t kNorm = 1;
      norm = &kNorm;
    }

    return ScoreFunction::Make<Bm25Score<HasBoost>>(
      _k, ctx.boost, *stats, nullptr, norm, filter_boost);  // TODO(gnusi): fix
  });
}

ScoreFunction BM25::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);

  if (!freq) {
    if (!_boost_as_score || 0.f == ctx.boost) {
      return ScoreFunction::Default();
    }

    // if there is no frequency then all the scores
    // will be the same (e.g. filter irs::all)
    return ScoreFunction::Constant(ctx.boost);
  }

  auto* filter_boost = [&] {
    auto* attr = irs::get<BoostBlockAttr>(ctx.doc_attrs);
    return attr ? attr->value : nullptr;
  }();

  auto* stats = stats_cast(ctx.stats);

  return ResolveBool(filter_boost != nullptr, [&]<bool HasBoost>() {
    if (IsBM1()) {
      return ScoreFunction::Make<Bm1Score<HasBoost>>(_k, ctx.boost, *stats,
                                                     filter_boost);
    }

    if (IsBM15()) {
      return ScoreFunction::Make<Bm15Score<HasBoost>>(_k, ctx.boost, *stats,
                                                      freq, filter_boost);
    }

    const uint32_t* norm = nullptr;
    if (ctx.collector) {
      norm = ctx.collector->AddNorms(ctx.segment.column(ctx.field.norm));
    }

    if (!norm) {
      static constexpr auto kNorms = [] {
        std::array<uint32_t, kPostingBlock> norms;
        absl::c_fill(norms, 1);
        return norms;
      }();
      norm = kNorms.data();
    }

    return ScoreFunction::Make<Bm25Score<HasBoost>>(_k, ctx.boost, *stats, freq,
                                                    norm, filter_boost);
  });
}

WandWriter::ptr BM25::prepare_wand_writer(size_t max_levels) const {
  if (IsBM1()) {
    return {};
  }
  if (IsBM15()) {
    return std::make_unique<FreqNormWriter<kWandTagMaxFreq>>(max_levels);
  }
  if (IsBM11()) {
    // idf * (k + 1) * tf / (k * (1 - b + b * dl / avg_dl) + tf)
    // idf * (k + 1) -- doesn't affect compare
    // tf / (k * (1 - b + b * dl / avg_dl) + tf)
    // replacement tf = x * dl
    // x * dl / (k * (1 - b + b * dl / avg_dl) + x * dl)
    // divide by dl
    // x / (k * ((1 - b) / dl + b / avg_dl) + x)
    // b == 1
    // x / (k / avg_dl + x)
    return std::make_unique<FreqNormWriter<kWandTagDivNorm>>(max_levels);
  }
  if (_approximate) {
    // It's not precise if we have more than 1 segment.
    // But search is distributed and we don't compute cluster wide avg_dl,
    // so it's better to use this instead of kWandTagBM25.
    // But if we want precise wand info, we need to return kWandTagBM25 here.
    return std::make_unique<FreqNormWriter<kWandTagAvgDL>>(max_levels, _b);
  }
  // It's precise for any numbers of segments, even for distributed case.
  // In other words for this we don't need to know avg_dl in ahead.
  return std::make_unique<FreqNormWriter<kWandTagBM25>>(max_levels, _b);
}

WandSource::ptr BM25::prepare_wand_source() const {
  if (IsBM1()) {
    return {};
  }
  if (IsBM15()) {
    return std::make_unique<FreqNormSource<kWandTagFreq>>();
  }
  return std::make_unique<FreqNormSource<kWandTagNorm>>();
}

TermCollector::ptr BM25::PrepareTermCollector() const {
  return std::make_unique<TermCollectorImpl>();
}

Scorer::WandType BM25::wand_type() const noexcept {
  if (IsBM1()) {
    return WandType::None;
  }
  if (IsBM15()) {
    return WandType::MaxFreq;
  }
  if (IsBM11()) {
    return WandType::DivNorm;
  }
  return WandType::MinNorm;
}

bool BM25::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = sdb::basics::downCast<BM25>(other);
  return p._k == _k && p._b == _b;
}

void BM25::init() {
  REGISTER_SCORER_JSON(BM25, MakeJson);
  REGISTER_SCORER_VPACK(BM25, MakeVPack);
}

}  // namespace irs
