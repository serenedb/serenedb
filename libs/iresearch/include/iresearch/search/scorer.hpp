////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/strings/str_cat.h>

#include <limits>
#include <optional>

#include "basics/assert.h"
#include "basics/math_utils.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/attribute_provider.hpp"

namespace irs {

class DataOutput;
struct IndexReader;
class MemoryIndexOutput;
class IndexOutput;
struct SubReader;
struct ScorerOptions;
struct NormProvider;
struct TermReader;
class ColumnArgsFetcher;

inline constexpr score_t kNoBoost{1.f};

inline IRS_FORCE_INLINE score_t TermCountToScore(uint32_t count) noexcept {
  return static_cast<score_t>(static_cast<int32_t>(count));
}

inline constexpr uint32_t kMaxFreq = std::numeric_limits<int32_t>::max();

struct Scorer;
struct FieldCollector;
struct TermCollector;

struct ScoreBoundSource : AttributeProvider {
  using ptr = std::unique_ptr<ScoreBoundSource>;

  virtual void Read(DataInput& in, size_t size) = 0;
};

struct ScoreBoundWriter {
  using ptr = std::unique_ptr<ScoreBoundWriter>;

  static constexpr byte_type kMaxSize = 127;

  virtual ~ScoreBoundWriter() = default;

  virtual bool Prepare(const NormProvider& norms, const FieldProperties& field,
                       const AttributeProvider& attrs) = 0;

  virtual void Reset() = 0;

  virtual void Update() = 0;

  virtual void Write(size_t level, MemoryIndexOutput& out) = 0;
  virtual void WriteRoot(size_t level, IndexOutput& out) = 0;

  virtual byte_type Size(size_t level) const = 0;
  virtual byte_type SizeRoot(size_t level) = 0;
};

struct ScoreContext {
  const NormProvider& segment;
  const FieldProperties& field;
  const AttributeProvider& doc_attrs;
  ColumnArgsFetcher* fetcher = nullptr;
  const byte_type* stats = nullptr;
  score_t boost = kNoBoost;
};

struct Scorer {
  using ptr = std::unique_ptr<Scorer>;

  virtual ~Scorer() = default;

  virtual void collect(byte_type* stats, const FieldCollector* field,
                       const TermCollector* term) const = 0;

  virtual IndexFeatures GetIndexFeatures() const = 0;

  virtual ScoreFunction PrepareScorer(const ScoreContext& ctx) const = 0;

  virtual bool ScoresPerDoc() const noexcept { return true; }

  virtual std::optional<score_t> Constant(const ScoreContext& ctx) const {
    if (ScoresPerDoc()) {
      return std::nullopt;
    }
    return PrepareScorer(ctx).Score();
  }

  virtual ScoreBoundWriter::ptr PrepareScoreBoundWriter(
    size_t max_levels) const = 0;

  virtual ScoreBoundSource::ptr PrepareScoreBoundSource() const = 0;

  virtual bool HasScoreBounds() const noexcept { return false; }

  enum class ScoreBoundType : uint8_t {
    None = 0,
    DivNorm = 1,
    MaxFreq = 2,
    MinNorm = 3,
  };

  virtual bool Compatible(const ScorerOptions&) const noexcept { return false; }

  virtual size_t stats_size() const = 0;

  virtual bool equals(const Scorer& other) const noexcept {
    return type() == other.type();
  }

  virtual std::string ToString() const {
    return absl::StrCat(type()().name(), "()");
  }

  virtual TypeInfo::type_id type() const noexcept = 0;
};

template<typename Visitor>
IRS_FORCE_INLINE auto ResolveMergeType(ScoreMergeType type, Visitor&& visitor) {
  SDB_ASSERT(type != ScoreMergeType::Noop);
  if (type == ScoreMergeType::Max) {
    return visitor.template operator()<ScoreMergeType::Max>();
  }
  return visitor.template operator()<ScoreMergeType::Sum>();
}

inline bool ScoresPerDoc(const Scorer* scorer) noexcept {
  return scorer != nullptr && scorer->ScoresPerDoc();
}

inline bool HasScoreBounds(const Scorer* scorer) noexcept {
  return scorer != nullptr && scorer->HasScoreBounds();
}

template<typename Impl, typename StatsType = void>
class ScorerBase : public Scorer {
 public:
  static_assert(std::is_void_v<StatsType> ||
                std::is_trivially_constructible_v<StatsType>);

  ScoreBoundWriter::ptr PrepareScoreBoundWriter(size_t) const override {
    return nullptr;
  }

  ScoreBoundSource::ptr PrepareScoreBoundSource() const override {
    return nullptr;
  }

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<Impl>::id();
  }

  void collect(byte_type*, const FieldCollector*,
               const TermCollector*) const override {}

  IRS_FORCE_INLINE static const StatsType* stats_cast(
    const byte_type* buf) noexcept {
    SDB_ASSERT(buf);
    return reinterpret_cast<const StatsType*>(buf);
  }

  IRS_FORCE_INLINE static StatsType* stats_cast(byte_type* buf) noexcept {
    return const_cast<StatsType*>(
      stats_cast(const_cast<const byte_type*>(buf)));
  }

  IRS_FORCE_INLINE size_t stats_size() const noexcept final {
    if constexpr (std::is_same_v<StatsType, void>) {
      return 0;
    } else {
      static_assert(alignof(StatsType) <= alignof(std::max_align_t));
      static_assert(math::IsPower2(alignof(StatsType)));

      return memory::AlignUp(sizeof(StatsType), alignof(StatsType));
    }
  }
};

template<ScoreMergeType MergeType, typename T>
IRS_FORCE_INLINE void Merge(score_t& bucket, T arg) noexcept {
  if constexpr (MergeType == ScoreMergeType::Sum) {
    bucket += arg;
  } else if constexpr (MergeType == ScoreMergeType::Max) {
    bucket = std::max<score_t>(bucket, arg);
  } else {
    static_assert(MergeType == ScoreMergeType::Noop);
    bucket = arg;
  }
}

template<ScoreMergeType MergeType, typename T>
IRS_FORCE_INLINE void Merge(score_t* IRS_RESTRICT res,
                            const T* IRS_RESTRICT args,
                            scores_size_t n) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    Merge<MergeType>(res[i], args[i]);
  }
}

template<ScoreMergeType MergeType, typename I>
IRS_FORCE_INLINE void Merge(score_t* IRS_RESTRICT res,
                            const I* IRS_RESTRICT hits,
                            const score_t* IRS_RESTRICT args,
                            scores_size_t n) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const auto bucket_index = hits[i];
    Merge<MergeType>(res[bucket_index], args[i]);
  }
}

template<ScoreMergeType MergeType, typename I>
IRS_FORCE_INLINE void Merge(score_t* IRS_RESTRICT res,
                            const I* IRS_RESTRICT hits, I base,
                            const score_t* IRS_RESTRICT args,
                            scores_size_t n) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const auto bucket_index = hits[i] - base;
    Merge<MergeType>(res[bucket_index], args[i]);
  }
}

template<ScoreMergeType MergeType, size_t N>
IRS_FORCE_INLINE void Merge(score_t* res, std::span<score_t, N> args) noexcept {
  Merge<MergeType>(res, args.data(), args.size());
}

template<ScoreMergeType MergeType, typename I, size_t N>
IRS_FORCE_INLINE void Merge(score_t* res, std::span<const I, N> hits,
                            std::span<const score_t, N> args) noexcept {
  SDB_ASSERT(hits.size() <= args.size());
  Merge<MergeType>(res, hits.data(), args.data(), hits.size());
}

}  // namespace irs
