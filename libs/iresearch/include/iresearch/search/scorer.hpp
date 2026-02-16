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
struct ColumnProvider;
struct TermReader;
class ColumnArgsFetcher;

// Represents no boost value.
inline constexpr score_t kNoBoost{1.f};

// Represents an addition to score from filter specific to a particular
// document. May vary from document to document.
struct FilterBoost final : Attribute {
  static constexpr std::string_view type_name() noexcept {
    return "filter_boost";
  }

  score_t value{kNoBoost};
};

// Caller writes value, iterator reads at block boundaries to skip
// blocks whose max score is below the threshold.
struct ScoreThresholdAttr final : Attribute {
  static constexpr std::string_view type_name() noexcept {
    return "score_threshold";
  }

  score_t value = std::numeric_limits<score_t>::min();
};

// Object used for collecting index statistics, for a specific matched
// field, that are required by the scorer for scoring individual
// documents.
class FieldCollector {
 public:
  using ptr = std::unique_ptr<FieldCollector>;

  virtual ~FieldCollector() = default;

  // Collect field related statistics, i.e. field used in the filter
  // `segment` is the segment being processed (e.g. for columnstore).
  // `field` is  the field matched by the filter in the 'segment'.
  // Called once for every field matched by a filter per each segment.
  // Always called on each matched 'field' irrespective of if it
  // contains a matching 'term'.
  virtual void collect(const SubReader& segment, const TermReader& field) = 0;

  // Clear collected stats
  virtual void reset() = 0;

  // Collect field related statistics from a serialized
  // representation as produced by write(...) below.
  virtual void collect(bytes_view in) = 0;

  // Serialize the internal data representation into 'out'.
  virtual void write(DataOutput& out) const = 0;
};

// Object used for collecting index statistics, for a specific matched
// term of a field, that are required by the scorer for scoring
// individual documents.
struct TermCollector {
  using ptr = std::unique_ptr<TermCollector>;

  virtual ~TermCollector() = default;

  // Collect term related statistics, i.e. term used in the filter.
  // `segment` is the segment being processed (e.g. for columnstore)
  // `field` is the the field matched by the filter in the 'segment'.
  // `term_attrs` is the attributes of the matched term in the field.
  // Called once for every term matched by a filter in the 'field'
  // per each segment.
  // Only called on a matched 'term' in the 'field' in the 'segment'.
  virtual void collect(const SubReader& segment, const TermReader& field,
                       const AttributeProvider& term_attrs) = 0;

  // Clear collected stats
  virtual void reset() = 0;

  // Collect term related statistics from a serialized
  // representation as produced by write(...) below.
  virtual void collect(bytes_view in) = 0;

  // Serialize the internal data representation into 'out'.
  virtual void write(DataOutput& out) const = 0;
};

struct WandSource : AttributeProvider {
  using ptr = std::unique_ptr<WandSource>;

  virtual void Read(DataInput& in, size_t size) = 0;
};

struct WandWriter {
  using ptr = std::unique_ptr<WandWriter>;

  static constexpr byte_type kMaxSize = 127;

  virtual ~WandWriter() = default;

  virtual bool Prepare(const ColumnProvider& reader,
                       const FieldProperties& field,
                       const AttributeProvider& attrs) = 0;

  virtual void Reset() = 0;

  virtual void Update() = 0;

  virtual void Write(size_t level, MemoryIndexOutput& out) = 0;
  virtual void WriteRoot(size_t level, IndexOutput& out) = 0;

  virtual byte_type Size(size_t level) const = 0;
  virtual byte_type SizeRoot(size_t level) = 0;
};

struct ScoreContext {
  const ColumnProvider& segment;
  const FieldProperties& field;
  const AttributeProvider& doc_attrs;
  ColumnArgsFetcher* fetcher = nullptr;
  const byte_type* stats = nullptr;
  score_t boost = kNoBoost;
};

// Base class for all scorers.
// Stats are meant to be trivially constructible and will be
// zero initialized before usage.
struct Scorer {
  using ptr = std::unique_ptr<Scorer>;

  virtual ~Scorer() = default;

  // Store collected index statistics into 'stats' of the
  // current 'filter'
  // `filter_attrs` is out-parameter to store statistics for later use in
  // calls to score(...).
  // `field` is the field level statistics collector as returned from
  //  PrepareFieldCollector()
  // `term` is the term level statistics collector as returned from
  //  PrepareTermCollector()
  // Called once on the 'index' for every field+term matched by the
  // filter.
  // Called once on the 'index' for every field with null term stats
  // if term is not applicable, e.g. by_column_existence filter.
  // Called exactly once if field/term collection is not applicable,
  // e.g. collecting statistics over the columnstore.
  // Called after all calls to collector::collect(...) on each segment.
  virtual void collect(byte_type* stats, const FieldCollector* field,
                       const TermCollector* term) const = 0;

  // The index features required for proper operation of this sort::Prepared
  virtual IndexFeatures GetIndexFeatures() const = 0;

  // Create an object to be used for collecting index statistics, one
  // instance per matched field.
  // Returns nullptr == no statistics collection required
  virtual FieldCollector::ptr PrepareFieldCollector() const = 0;

  // Create a stateful scorer used for computation of document scores
  virtual ScoreFunction PrepareScorer(const ScoreContext& ctx) const = 0;

  // Create an object to be used for collecting index statistics, one
  // instance per matched term.
  // Returns nullptr == no statistics collection required.
  virtual TermCollector::ptr PrepareTermCollector() const = 0;

  // Create an object to be used for writing wand entries to the skip list.
  // max_levels - max number of levels in the skip list
  virtual WandWriter::ptr prepare_wand_writer(size_t max_levels) const = 0;

  virtual WandSource::ptr prepare_wand_source() const = 0;

  enum class WandType : uint8_t {
    None = 0,
    DivNorm = 1,
    MaxFreq = 2,
    MinNorm = 3,
  };

  virtual WandType wand_type() const noexcept { return WandType::None; }

  // 0 -- not compatible
  // x -- degree of compatibility
  // 255 -- compatible, same types
  static uint8_t compatible(WandType lhs, WandType rhs) noexcept;

  // Number of bytes required to store stats (already aligned).
  virtual size_t stats_size() const = 0;

  virtual bool equals(const Scorer& other) const noexcept {
    return type() == other.type();
  }

  virtual TypeInfo::type_id type() const noexcept = 0;
};

// Possible variants of merging multiple scores
enum class ScoreMergeType {
  // Do nothing
  Noop = 0,

  // Sum multiple scores
  Sum,

  // Find max amongst multiple scores
  Max,
};

template<typename Visitor>
IRS_FORCE_INLINE auto ResolveMergeType(ScoreMergeType type, Visitor&& visitor) {
  switch (type) {
    case ScoreMergeType::Sum:
      return visitor.template operator()<ScoreMergeType::Sum>();
    case ScoreMergeType::Max:
      return visitor.template operator()<ScoreMergeType::Max>();
    case ScoreMergeType::Noop:
      return visitor.template operator()<ScoreMergeType::Noop>();
  }
}

// Template score for base class for all prepared(compiled) sort entries
template<typename Impl, typename StatsType = void>
class ScorerBase : public Scorer {
 public:
  static_assert(std::is_void_v<StatsType> ||
                std::is_trivially_constructible_v<StatsType>);

  WandWriter::ptr prepare_wand_writer(size_t) const override { return nullptr; }

  WandSource::ptr prepare_wand_source() const override { return nullptr; }

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<Impl>::id();
  }

  FieldCollector::ptr PrepareFieldCollector() const override { return nullptr; }

  TermCollector::ptr PrepareTermCollector() const override { return nullptr; }

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

  // Returns number of bytes required to store stats (already aligned).
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

template<ScoreMergeType MergeType>
IRS_FORCE_INLINE void Merge(score_t& bucket, score_t arg) noexcept {
  if constexpr (MergeType == ScoreMergeType::Sum) {
    bucket += arg;
  } else if constexpr (MergeType == ScoreMergeType::Max) {
    bucket = std::max(bucket, arg);
  } else {
    static_assert(false);
  }
}

template<ScoreMergeType MergeType, typename T>
IRS_FORCE_INLINE void Merge(T* IRS_RESTRICT res,
                            const score_t* IRS_RESTRICT args,
                            size_t n) noexcept {
  for (size_t i = 0; i < n; ++i) {
    Merge<MergeType>(res[i], args[i]);
  }
}

template<ScoreMergeType MergeType, typename T, typename I>
IRS_FORCE_INLINE void Merge(T* IRS_RESTRICT res, const I* IRS_RESTRICT hits,
                            const score_t* IRS_RESTRICT args,
                            size_t n) noexcept {
  for (size_t i = 0; i < n; ++i) {
    const auto bucket_index = hits[i];
    Merge<MergeType>(res[bucket_index], args[i]);
  }
}

template<ScoreMergeType MergeType, typename T, typename I>
IRS_FORCE_INLINE void Merge(T* IRS_RESTRICT res, const I* IRS_RESTRICT hits,
                            I base, const score_t* IRS_RESTRICT args,
                            size_t n) noexcept {
  for (size_t i = 0; i < n; ++i) {
    const auto bucket_index = hits[i] - base;
    Merge<MergeType>(res[bucket_index], args[i]);
  }
}

template<ScoreMergeType MergeType, typename T, size_t N>
IRS_FORCE_INLINE void Merge(score_t* res, std::span<score_t, N> args) noexcept {
  Merge<MergeType>(res, args.data(), args.size());
}

template<ScoreMergeType MergeType, typename T, typename I, size_t N>
IRS_FORCE_INLINE void Merge(T* res, std::span<I, N> hits,
                            std::span<score_t, N> args) noexcept {
  SDB_ASSERT(hits.size() <= args.size());
  Merge<MergeType>(res, hits.data(), args.data(), hits.size());
}

}  // namespace irs
