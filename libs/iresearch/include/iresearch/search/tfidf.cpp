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

#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>
#include <vpack/vpack.h>

#include <cmath>
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

constexpr std::string_view kWithNormsParamName("withNorms");

Scorer::ptr MakeFromObject(const vpack::Slice slice) {
  SDB_ASSERT(slice.isObject());

  auto normalize = TFIDF::WITH_NORMS();

  if (auto v = slice.get(kWithNormsParamName); !v.isNone()) {
    if (!v.isBool()) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Non-boolean value in '", kWithNormsParamName,
                     "' while constructing tfidf scorer from VPack arguments"));
      return nullptr;
    }
    normalize = v.getBool();
  }

  return std::make_unique<TFIDF>(normalize);
}

Scorer::ptr MakeFromArray(const vpack::Slice slice) {
  SDB_ASSERT(slice.isArray());

  vpack::ArrayIterator array = vpack::ArrayIterator(slice);
  vpack::ValueLength size = array.size();

  if (size > 1) {
    // wrong number of arguments
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Wrong number of arguments while constructing tfidf scorer from VPack "
      "arguments (must be <= 1)");
    return nullptr;
  }

  // default args
  auto norms = TFIDF::WITH_NORMS();

  // parse `withNorms` optional argument
  for (auto arg_slice : array) {
    if (!arg_slice.isBool()) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Non-bool value on position `0` while constructing tfidf scorer from "
        "VPack arguments");
      return nullptr;
    }

    norms = arg_slice.getBool();
  }

  return std::make_unique<TFIDF>(norms);
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

template<typename Norm>
struct TFIDFContext final : public ScoreCtx {
  TFIDFContext(Norm&& norm, score_t boost, TFIDFStats idf, const FreqAttr* freq,
               const FilterBoost* filter_boost = nullptr) noexcept
    : freq{freq ? *freq : kEmptyFreq},
      filter_boost{filter_boost},
      idf{boost * idf.value},
      norm{std::move(norm)} {
    SDB_ASSERT(freq);
  }

  TFIDFContext(const TFIDFContext&) = delete;
  TFIDFContext& operator=(const TFIDFContext&) = delete;

  const FreqAttr& freq;
  const FilterBoost* filter_boost;
  float_t idf;  // precomputed : boost * idf
  [[no_unique_address]] Norm norm;
};

template<typename Reader, NormType Type>
struct TFIDFNormAdapter final {
  explicit TFIDFNormAdapter(Reader&& reader) : reader{std::move(reader)} {}

  IRS_FORCE_INLINE decltype(auto) operator()() {
    return kRSQRT.get<Type != NormType::NormTiny>(reader());
  }

  [[no_unique_address]] Reader reader;
};

template<NormType Type, typename Reader>
auto MakeTFIDFNormAdapter(Reader&& reader) {
  return TFIDFNormAdapter<Reader, Type>(std::move(reader));
}

}  // namespace

template<typename Norm>
struct MakeScoreFunctionImpl<TFIDFContext<Norm>> {
  using Ctx = TFIDFContext<Norm>;

  template<bool HasFilterBoost, typename... Args>
  static auto Make(Args&&... args) {
    return ScoreFunction::Make<Ctx>(
      [](ScoreCtx* ctx, score_t* res) noexcept {
        SDB_ASSERT(res);
        SDB_ASSERT(ctx);

        auto& state = *static_cast<Ctx*>(ctx);

        float_t idf;
        if constexpr (HasFilterBoost) {
          SDB_ASSERT(state.filter_boost);
          idf = state.idf * state.filter_boost->value;
        } else {
          idf = state.idf;
        }

        if constexpr (std::is_same_v<Norm, utils::Empty>) {
          *res = Tfidf(state.freq.value, idf);
        } else {
          *res = Tfidf(state.freq.value, idf) * state.norm();
        }
      },
      ScoreFunction::DefaultMin, std::forward<Args>(args)...);
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
  // TODO(mbkkt) SEARCH-464 SDB_ASSERT(docs_with_field >= docs_with_term);

  auto* idf = stats_cast(stats_buf);
  idf->value += static_cast<float_t>(
    std::log1p((docs_with_field + 1.0) / (docs_with_term + 1.0)));
  // TODO(mbkkt) SEARCH-444 SDB_ASSERT(idf.value >= 0.f);
}

ScoreFunction TFIDF::PrepareScorer(const ColumnProvider& segment,
                                   const FieldProperties& meta,
                                   const byte_type* stats_buf,
                                   const AttributeProvider& doc_attrs,
                                   score_t boost) const {
  auto* freq = irs::get<FreqAttr>(doc_attrs);

  if (!freq) {
    if (!_boost_as_score || 0.f == boost) {
      return ScoreFunction::Default(1);
    }

    // if there is no frequency then all the
    // scores will be the same (e.g. filter irs::all)
    return ScoreFunction::Constant(boost);
  }

  const auto* stats = stats_cast(stats_buf);
  auto* filter_boost = irs::get<FilterBoost>(doc_attrs);

  // add norm attribute if requested
  if (_normalize) {
    auto prepare_norm_scorer =
      [&]<typename Norm>(Norm&& norm) -> ScoreFunction {
      return MakeScoreFunction<TFIDFContext<Norm>>(
        filter_boost, std::move(norm), boost, *stats, freq);
    };

    // Check if norms are present in attributes
    if (auto* norm = irs::get<Norm>(doc_attrs); norm) {
      return prepare_norm_scorer(MakeTFIDFNormAdapter<NormType::Norm>(
        [norm]() noexcept { return norm->value; }));
    }

    // Fallback to reading from columnstore
    auto* doc = irs::get<DocAttr>(doc_attrs);

    if (!doc) [[unlikely]] {
      // we need 'document' attribute to be exposed
      return ScoreFunction::Default(1);
    }

    if (field_limits::valid(meta.norm)) {
      if (Norm::Context ctx; ctx.Reset(segment, meta.norm, *doc)) {
        if (ctx.max_num_bytes == sizeof(byte_type)) {
          return Norm::MakeReader(std::move(ctx), [&](auto&& reader) {
            return prepare_norm_scorer(
              MakeTFIDFNormAdapter<NormType::NormTiny>(std::move(reader)));
          });
        }

        return Norm::MakeReader(std::move(ctx), [&](auto&& reader) {
          return prepare_norm_scorer(
            MakeTFIDFNormAdapter<NormType::Norm>(std::move(reader)));
        });
      }
    }
  }

  return MakeScoreFunction<TFIDFContext<utils::Empty>>(
    filter_boost, utils::Empty{}, boost, *stats, freq);
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
