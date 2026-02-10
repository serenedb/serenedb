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

struct Params {
  float_t k = BM25::K();
  float_t b = BM25::B();
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
                   "' while constructing bm25 scorer from VPack arguments"));
    return {};
  }

  return std::make_unique<BM25>(params.k, params.b);
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
  } else {
    vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
    return MakeVPack(slice);
  }
}

Scorer::ptr MakeJson(std::string_view args) {
  if (IsNull(args)) {
    // default args
    return std::make_unique<irs::BM25>();
  } else {
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
}

struct BM1Context : public irs::ScoreCtx {
  BM1Context(float_t k, score_t boost, const BM25Stats& stats,
             const score_t* fb = nullptr) noexcept
    : filter_boost{fb}, num{boost * (k + 1) * stats.idf} {}

  const score_t* filter_boost;
  float_t num;  // partially precomputed numerator : boost * (k + 1) * idf
};

template<typename T>
void Bm1Boost(T* IRS_RESTRICT res, size_t n, const score_t* IRS_RESTRICT boost,
              float_t num) noexcept {
  for (size_t i = 0; i < n; ++i) {
    *GetScoreValue(res[i]) = boost[i] * num;
  }
}

struct BM15Context : public BM1Context {
  BM15Context(float_t k, score_t boost, const BM25Stats& stats,
              const FreqBlockAttr* freq, const score_t* fb = nullptr) noexcept
    : BM1Context{k, boost, stats, fb},
      norm_const{stats.norm_const},
      freq{freq} {
    SDB_ASSERT(this->freq);
  }

  float_t norm_const;         // 'k' factor
  const FreqBlockAttr* freq;  // document frequency
};

template<bool HasBoost, typename T>
void Bm15(T* IRS_RESTRICT res, size_t n, const uint32_t* IRS_RESTRICT freq,
          [[maybe_unused]] const score_t* IRS_RESTRICT boost, float_t num,
          float_t c1) noexcept {
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
    *GetScoreValue(res[i]) =
      c0 - c0 / (1.f + static_cast<float_t>(freq[i]) / c1);
  }
}

struct BM25Context : public BM15Context {
  BM25Context(float_t k, score_t boost, const BM25Stats& stats,
              const FreqBlockAttr* freq, const uint32_t* norm,
              const score_t* filter_boost = nullptr) noexcept
    : BM15Context{k, boost, stats, freq, filter_boost},
      norm{norm},
      norm_length{stats.norm_length} {}

  const uint32_t* norm;
  float_t norm_length;  // precomputed 'k*b/avg_dl'
};

template<bool HasBoost, typename T>
void Bm25(T* res, size_t n, const uint32_t* IRS_RESTRICT freq,
          const uint32_t* IRS_RESTRICT norm,
          [[maybe_unused]] const score_t* IRS_RESTRICT boost, float_t num,
          float_t norm_const, float_t norm_length) noexcept {
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
    *GetScoreValue(res[i]) =
      c0 - c0 * c1 / (c1 + static_cast<float_t>(freq[i]));
  }
}

}  // namespace

template<>
struct MakeScoreFunctionImpl<BM1Context> {
  using Ctx = BM1Context;

  template<bool HasFilterBoost, bool Single, typename... Args>
  static auto Make(Args&&... args) {
    if constexpr (HasFilterBoost) {
      return ScoreFunction::Make<Ctx>(
        [](irs::ScoreCtx* ctx, irs::score_t* res, size_t n) noexcept {
          SDB_ASSERT(res);
          SDB_ASSERT(ctx);
          auto& state = static_cast<Ctx&>(*ctx);
          Bm1Boost(res, n, state.filter_boost, state.num);
        },
        ScoreFunction::NoopMin, std::forward<Args>(args)...);
    } else {
      Ctx ctx{std::forward<Args>(args)...};
      return ScoreFunction::Constant(ctx.num);
    }
  }
};

template<>
struct MakeScoreFunctionImpl<BM15Context> {
  using Ctx = BM15Context;

  template<bool HasFilterBoost, bool Single, typename... Args>
  static auto Make(Args&&... args) {
    return ScoreFunction::Make<Ctx>(
      [](irs::ScoreCtx* ctx, irs::score_t* res, size_t n) noexcept {
        SDB_ASSERT(res);
        SDB_ASSERT(ctx);
        auto& state = *static_cast<Ctx*>(ctx);
        Bm15<HasFilterBoost>(res, n, state.freq->value, state.filter_boost,
                             state.num, state.norm_const);
      },
      ScoreFunction::NoopMin, std::forward<Args>(args)...);
  }
};

template<>
struct MakeScoreFunctionImpl<BM25Context> {
  using Ctx = BM25Context;

  template<bool HasFilterBoost, bool Single, typename... Args>
  static auto Make(Args&&... args) {
    return ScoreFunction::Make<Ctx>(
      [](ScoreCtx* ctx, score_t* res, size_t n) noexcept {
        SDB_ASSERT(res);
        SDB_ASSERT(ctx);
        auto& state = *static_cast<Ctx*>(ctx);
        Bm25<HasFilterBoost>(res, Single ? 1 : n, state.freq->value, state.norm,
                             state.filter_boost, state.num, state.norm_const,
                             state.norm_length);
      },
      ScoreFunction::NoopMin, std::forward<Args>(args)...);
  }
};

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

  if (IsBM1()) {
    return MakeScoreFunction<BM1Context, false>(filter_boost, _k, ctx.boost,
                                                *stats);
  }

  if (IsBM15()) {
    return MakeScoreFunction<BM15Context, false>(
      filter_boost, _k, ctx.boost, *stats, nullptr);  // TODO(gnusi): fix
  }

  auto* norm = [&] {
    auto* attr = irs::get<Norm>(ctx.doc_attrs);
    return attr ? &attr->value : nullptr;
  }();

  if (!norm) {
    static constexpr uint32_t kNorm = 1;
    norm = &kNorm;
  }

  return MakeScoreFunction<BM25Context, false>(
    filter_boost, _k, ctx.boost, *stats, nullptr, norm);  // TODO(gnusi): fix
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

  if (IsBM1()) {
    return MakeScoreFunction<BM1Context, false>(filter_boost, _k, ctx.boost,
                                                *stats);
  }

  if (IsBM15()) {
    return MakeScoreFunction<BM15Context, false>(filter_boost, _k, ctx.boost,
                                                 *stats, freq);
  }

  const uint32_t* norm = nullptr;
  if (ctx.collector) {
    norm = ctx.collector->AddNorms(ctx.segment.column(ctx.field.norm));
  }

  if (!norm) {
    static constexpr auto kNorms = [] {
      std::array<uint32_t, kMaxScoreBlock> norms;
      absl::c_fill(norms, 1);
      return norms;
    }();
    norm = kNorms.data();
  }

  return MakeScoreFunction<BM25Context, false>(filter_boost, _k, ctx.boost,
                                               *stats, freq, norm);
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
  // Approximation that suited for any BM25
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
