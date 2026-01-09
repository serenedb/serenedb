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
#include <iresearch/search/column_collector.hpp>
#include <iresearch/search/score.hpp>
#include <iresearch/search/score_function.hpp>
#include <string_view>

#include "basics/down_cast.h"
#include "basics/math_utils.hpp"
#include "basics/misc.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/wand_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/scorer_impl.hpp"
#include "iresearch/search/scorers.hpp"
#include "vpack/serializer.h"

namespace irs {
namespace {

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

const auto kSQRT = CacheFunc<uint32_t, 2048>(
  0, [](uint32_t i) noexcept { return std::sqrt(static_cast<float_t>(i)); });

const auto kRSQRT = CacheFunc<uint32_t, 2048>(1, [](uint32_t i) noexcept {
  return 1.f / std::sqrt(static_cast<float_t>(i));
});

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

IRS_FORCE_INLINE float_t Tfidf(uint32_t freq, float_t idf) noexcept {
  return kSQRT.get<true>(freq) * idf;
}

template<byte_type NormLength>
IRS_FORCE_INLINE float_t Rsqrt(uint32_t norm) noexcept {
  return kRSQRT.get<NormLength != sizeof(byte_type)>(norm);
}

struct TFIDFContext : public ScoreCtx {
  TFIDFContext(const Norm* norm, score_t boost, TFIDFStats idf,
               const uint32_t* freq,
               const score_t* filter_boost = nullptr) noexcept
    : source_freq{freq ? freq : &kEmptyFreq.value},
      source_filter_boost{filter_boost},
      source_norm{std::move(norm)},
      idf{boost * idf.value} {
    SDB_ASSERT(freq);
  }

  const uint32_t* source_freq;
  const score_t* source_filter_boost;
  const Norm* source_norm;

  absl::InlinedVector<uint32_t, kScoreWindow> freq;
  absl::InlinedVector<score_t, kScoreWindow> filter_boost;
  absl::InlinedVector<uint32_t, kScoreWindow> norm;
  float_t idf;  // precomputed : boost * idf
};

}  // namespace

template<byte_type NormLength>
struct TFIDFContextImpl : TFIDFContext {};

template<byte_type NormLength>
struct MakeScoreFunctionImpl<TFIDFContextImpl<NormLength>> {
  using Ctx = TFIDFContext;

  template<bool HasFilterBoost, typename... Args>
  static auto Make(Args&&... args) {
    return ScoreFunction::Make<Ctx>(
      [](ScoreCtx* ctx, score_t* res) noexcept {
        SDB_ASSERT(res);
        SDB_ASSERT(ctx);

        auto& state = *static_cast<Ctx*>(ctx);

        const auto size = state.freq.size();
        for (size_t i = 0; i < size; ++i) {
          float_t idf;
          if constexpr (HasFilterBoost) {
            idf = state.idf * state.filter_boost[i];
          } else {
            idf = state.idf;
          }

          if constexpr (NormLength != 0) {
            *res = Tfidf(state.freq[i], idf) * Rsqrt<NormLength>(state.norm[i]);
          } else {
            *res = Tfidf(state.freq[i], idf);
          }
        }

        // TODO(gnusi): optimize clear
        if constexpr (HasFilterBoost) {
          state.filter_boost.clear();
        }
        if constexpr (NormLength != 0) {
          state.norm.clear();
        }
        state.freq.clear();
      },
      [](ScoreCtx* ctx) noexcept {
        auto& state = *static_cast<Ctx*>(ctx);
        state.freq.emplace_back(*state.source_freq);
        if constexpr (HasFilterBoost) {
          SDB_ASSERT(state.source_filter_boost);
          state.filter_boost.emplace_back(*state.source_filter_boost);
        }
        if constexpr (NormLength != 0) {
          SDB_ASSERT(state.source_norm);
          state.norm.emplace_back(state.source_norm->value);
        }
      },
      ScoreFunction::NoopMin, std::forward<Args>(args)...);
  }
};

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
  idf->value += static_cast<float_t>(
    std::log1p((docs_with_field + 1.0) / (docs_with_term + 1.0)));
  // TODO(mbkkt) SDB_ASSERT(idf.value >= 0.f);
}

ScoreFunction TFIDF::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqAttr>(ctx.doc_attrs);

  if (!freq) {
    if (!_boost_as_score || 0.f == ctx.boost) {
      return ScoreFunction::Default();
    }

    // if there is no frequency then all the
    // scores will be the same (e.g. filter irs::all)
    return ScoreFunction::Constant(ctx.boost);
  }

  const auto* stats = stats_cast(ctx.stats);
  auto* filter_boost = irs::get<FilterBoost>(ctx.doc_attrs);

  const Norm* norm = nullptr;
  if (_normalize) {
    // Check if norms are present in attributes
    norm = irs::get<Norm>(ctx.doc_attrs);
    if (!norm && ctx.collector) {
      // Fallback to reading from columnstore
      norm = ctx.collector->AddNorm(ctx.segment.column(ctx.field.norm));
    }
  }

  auto make_scorer = [&]<size_t N> {
    return MakeScoreFunction<TFIDFContextImpl<N>>(filter_boost, norm, ctx.boost,
                                                  *stats, &freq->value);
  };

  if (!norm) {
    return make_scorer.template operator()<0>();
  }

  if (norm->num_bytes == sizeof(byte_type)) {
    return make_scorer.template operator()<sizeof(byte_type)>();
  }

  return make_scorer.template operator()<sizeof(uint32_t)>();
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
    return std::make_unique<FreqNormWriter<kWandTagDivNorm>>(max_levels, *this);
  }
  return std::make_unique<FreqNormWriter<kWandTagMaxFreq>>(max_levels, *this);
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
