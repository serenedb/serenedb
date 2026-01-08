////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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

#include <algorithm>

#include "basics/containers/small_vector.h"
#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace irs {

template<typename Producer>
class WandWriterImpl final : public WandWriter {
  using EntryType = typename Producer::Entry;

 public:
  template<typename... Args>
  WandWriterImpl(size_t max_levels, const Scorer& scorer, Args&&... args)
    : _levels{max_levels + 1}, _producer{scorer, std::forward<Args>(args)...} {
    SDB_ASSERT(max_levels != 0);
  }

  bool Prepare(const ColumnProvider& reader, const FieldProperties& meta,
               const AttributeProvider& attrs) final {
    return _producer.Prepare(reader, meta, attrs);
  }

  void Reset() noexcept final {
    for (auto& entry : _levels) {
      entry = {};
    }
  }

  void Update() noexcept final {
    SDB_ASSERT(!_levels.empty());
    _producer.Produce(_levels.front());
  }

  void Write(size_t level, MemoryIndexOutput& out) final {
    SDB_ASSERT(level + 1 < _levels.size());
    auto& entry = _levels[level];
    _producer.Produce(entry, _levels[level + 1]);
    Producer::Write(entry, out);
    entry = {};
  }

  void WriteRoot(size_t level, IndexOutput& out) final {
    SDB_ASSERT(level < _levels.size());
    auto& entry = _levels[level];
    Producer::Write(entry, out);
  }

  uint8_t Size(size_t level) const noexcept final {
    SDB_ASSERT(level + 1 < _levels.size());
    const auto& entry = _levels[level];
    return Producer::Size(entry);
  }

  uint8_t SizeRoot(size_t level) noexcept final {
    SDB_ASSERT(level < _levels.size());
    auto it = _levels.begin();
    for (auto end = it + level; it != end;) {
      const auto& from = *it;
      _producer.Produce(from, *++it);
    }
    return Producer::Size(*it);
  }

 private:
  // 9 -- current max skip list levels
  // 1 -- for whole skip list level
  sdb::containers::SmallVector<EntryType, 9 + 1> _levels;
  [[no_unique_address]] Producer _producer;
};

template<bool NeedScore>
struct WandScorer {
  explicit WandScorer(const Scorer& /*scorer*/) noexcept {}

  constexpr bool Prepare(const ColumnProvider& /*reader*/,
                         const FieldProperties& /*meta*/,
                         const AttributeProvider& /*attrs*/) noexcept {
    return true;
  }
};

template<>
struct WandScorer<true> {
  explicit WandScorer(const Scorer& scorer) : _scorer{scorer} {
    // TODO(mbkkt) alignment could be incorrect here
    _stats.resize(scorer.stats_size().first);
    scorer.collect(_stats.data(), nullptr, nullptr);
  }

  bool Prepare(const ColumnProvider& reader, const FieldProperties& meta,
               const AttributeProvider& attrs) {
    _func = _scorer.PrepareScorer(reader, meta, _stats.data(), attrs, kNoBoost);
    return !_func.IsDefault();
  }

  score_t GetScore() const noexcept {
    score_t score{};
    _func(&score);
    return score;
  }

 private:
  const Scorer& _scorer;
  ScoreFunction _func;
  sdb::containers::SmallVector<byte_type, 16> _stats;
};

enum WandTag : uint32_t {
  // What will be written?
  kWandTagFreq = 0U,
  kWandTagNorm = 1U << 0U,
  // How to Produce best Entry?
  // Produce max freq
  kWandTagMaxFreq = 1U << 1U,
  // Produce max freq, min norm, but norm >= freq
  kWandTagMinNorm = 1U << 2U,
  // Produce max freq/norm
  kWandTagDivNorm = 1U << 3U,
  // Produce best freq, norm for BM25 with specified b -- (0...1)
  kWandTagBM25 = 1U << 4U,
  // Produce max score
  kWandTagMaxScore = 1U << 5U,
};

template<uint32_t Tag>
class FreqNormProducer {
  static constexpr bool kMaxScore = (Tag & kWandTagMaxScore) != 0;
  static constexpr bool kBm25 = (Tag & kWandTagBM25) != 0;
  static constexpr bool kDivNorm = (Tag & kWandTagDivNorm) != 0;
  static constexpr bool kMinNorm = (Tag & kWandTagMinNorm) != 0;
  static constexpr bool kMaxFreq = kMinNorm || (Tag & kWandTagMaxFreq) != 0;

  static constexpr bool kNorm =
    kBm25 || kDivNorm || kMinNorm || (Tag & kWandTagNorm) != 0;

  static constexpr score_t kMinAvgDL = 1.f;
  static constexpr score_t kMaxAvgDL = 4294967296.f;

  static IRS_FORCE_INLINE auto CmpBm25(score_t avg_dl, score_t b, uint32_t tf_1,
                                       uint32_t dl_1, uint32_t tf_2,
                                       uint32_t dl_2) noexcept {
    SDB_ASSERT(0.f < b);
    SDB_ASSERT(b < 1.f);
    SDB_ASSERT(0.f < avg_dl);
    SDB_ASSERT(0 < tf_1);
    SDB_ASSERT(0 < dl_1);
    SDB_ASSERT(tf_1 <= dl_1);
    SDB_ASSERT(0 < tf_2);
    SDB_ASSERT(0 < dl_2);
    SDB_ASSERT(tf_2 <= dl_2);
    // idf * (k + 1) * tf / (k * (1 - b + b * dl / avg_dl) + tf)
    // 1. idf * (k + 1) -- doesn't affect compare
    // tf_1 / (k * (1 - b + b * dl_1 / avg_dl) + tf_1)
    // tf_2 / (k * (1 - b + b * dl_2 / avg_dl) + tf_2)
    // 2. replace division by multiply
    // tf_1 * (k * (1 - b + b * dl_2 / avg_dl) + tf_2)
    // tf_2 * (k * (1 - b + b * dl_1 / avg_dl) + tf_1)
    // 3. simplify
    // tf_1 * k * (1 - b + b * dl_2 / avg_dl) + tf_1 * tf_2
    // tf_2 * k * (1 - b + b * dl_1 / avg_dl) + tf_2 * tf_1
    // 4. remove tf_1 * tf_2 and k
    // tf_1 * (1 - b + b * dl_2 / avg_dl)
    // tf_2 * (1 - b + b * dl_1 / avg_dl)
    // 5. multiply on avg_dl TODO(mbkkt) this step could give worse precision
    const auto x = (1.f - b) * avg_dl;
    const auto lhs = tf_1 * (x + b * dl_2);
    const auto rhs = tf_2 * (x + b * dl_1);
    return lhs <=> rhs;
  }

 public:
  struct Entry {
    [[no_unique_address]] utils::Need<kMaxScore, score_t> score{0.f};
    uint32_t freq{1};
    [[no_unique_address]] utils::Need<kNorm, uint32_t> norm{
      std::numeric_limits<uint32_t>::max()};
  };

  IRS_FORCE_INLINE void Produce(const Entry& from, Entry& to) noexcept {
    if constexpr (kBm25) {
      ProduceBM25(_b, from.freq, from.norm, to);
    } else if constexpr (kMaxFreq) {
      to.freq = from.freq > to.freq ? from.freq : to.freq;
      if constexpr (kMinNorm) {
        to.norm = from.norm < to.norm ? from.norm : to.norm;
        to.norm = to.norm < to.freq ? to.freq : to.norm;
      }
    } else if constexpr (kDivNorm) {
      ProduceDivNorm(from.freq, from.norm, to);
    } else if constexpr (kMaxScore) {
      if (from.score > to.score) {
        to.score = from.score;
        to.freq = from.freq;
        to.norm = from.norm;
      }
    }
  }

  template<typename Output>
  static void Write(Entry entry, Output& out) {
    // TODO(mbkkt) Compute difference second time looks unnecessary.
    SDB_ASSERT(entry.freq >= 1);
    out.WriteV32(entry.freq);
    if constexpr (kNorm) {
      SDB_ASSERT(entry.norm >= entry.freq);
      if (entry.norm != entry.freq) {
        out.WriteV32(entry.norm - entry.freq);
      }
    }
  }

  static uint8_t Size(Entry entry) noexcept {
    SDB_ASSERT(entry.freq >= 1);
    size_t size = bytes_io<uint32_t>::vsize(entry.freq);
    if constexpr (kNorm) {
      SDB_ASSERT(entry.norm >= entry.freq);
      if (entry.norm != entry.freq) {
        size += bytes_io<uint32_t>::vsize(entry.norm - entry.freq);
      }
    }
    return size;
  }

  explicit FreqNormProducer(const Scorer& scorer, score_t b = 0.f)
    : _scorer{scorer}, _b{b} {}

  bool Prepare(const ColumnProvider& reader, const FieldProperties& meta,
               const AttributeProvider& attrs) {
    _freq = irs::get<FreqAttr>(attrs);

    if (_freq == nullptr) [[unlikely]] {
      return false;
    }

    if constexpr (kNorm) {
      const auto* doc = irs::get<DocAttr>(attrs);

      if (doc == nullptr) [[unlikely]] {
        return false;
      }

      if (!field_limits::valid(meta.norm)) [[unlikely]] {
        return false;
      }

      Norm::Context ctx;
      if (!ctx.Reset(reader, meta.norm, *doc)) [[unlikely]] {
        return false;
      }

      _norm = Norm::MakeReader(std::move(ctx), [&](auto&& reader) {
        return absl::AnyInvocable<uint32_t() noexcept>{std::move(reader)};
      });
    }

    return _scorer.Prepare(reader, meta, attrs);
  }

  IRS_FORCE_INLINE void Produce(Entry& to) noexcept {
    if constexpr (kBm25 || kDivNorm) {
      const auto freq = _freq->value;
      const auto norm = _norm();
      if constexpr (kBm25) {
        ProduceBM25(_b, freq, norm, to);
      } else {
        ProduceDivNorm(freq, norm, to);
      }
    } else if constexpr (kMaxFreq) {
      const auto freq = _freq->value;
      to.freq = freq > to.freq ? freq : to.freq;
      if constexpr (kMinNorm) {
        const auto norm = _norm();
        to.norm = norm < to.norm ? norm : to.norm;
        to.norm = to.norm < to.freq ? to.freq : to.norm;
      }
    } else if constexpr (kMaxScore) {
      const auto score = _scorer.GetScore();
      if (score > to.score) {
        to.score = score;
        to.freq = _freq->value;
        if constexpr (kNorm) {
          to.norm = _norm();
        }
      }
    }
  }

 private:
  static IRS_FORCE_INLINE void ProduceDivNorm(uint32_t freq, uint32_t norm,
                                              Entry& to) noexcept {
    if (static_cast<uint64_t>(freq) * to.norm >
        static_cast<uint64_t>(to.freq) * norm) {
      to.freq = freq;
      to.norm = norm;
    }
  }

  static IRS_NO_INLINE void ProduceBM25(score_t b, uint32_t freq, uint32_t norm,
                                        Entry& to) noexcept {
    // try to choose best document for any avg_dl
    const auto min = CmpBm25(kMinAvgDL, b, freq, norm, to.freq, to.norm);
    const auto max = CmpBm25(kMaxAvgDL, b, freq, norm, to.freq, to.norm);
    if (min <= 0 && max <= 0) {
      return;
    }
    if (min >= 0 && max >= 0) {
      to.freq = freq;
      to.norm = norm;
      return;
    }
    // fallback, create virtual document
    to.freq = freq > to.freq ? freq : to.freq;
    to.norm = norm < to.norm ? norm : to.norm;
    to.norm = to.norm < to.freq ? to.freq : to.norm;
  }

  const irs::FreqAttr* _freq{};
  [[no_unique_address]]
  utils::Need<kNorm, absl::AnyInvocable<uint32_t() noexcept>> _norm;
  [[no_unique_address]] WandScorer<kMaxScore> _scorer;
  [[no_unique_address]] utils::Need<kBm25, score_t> _b;
};

template<uint32_t Tag>
using FreqNormWriter = WandWriterImpl<FreqNormProducer<Tag>>;

template<uint32_t Tag>
class FreqNormSource final : public WandSource {
  static constexpr bool kNorm = (Tag & kWandTagNorm) != 0;

 public:
  Attribute* GetMutable(TypeInfo::type_id type) final {
    if (irs::Type<FreqAttr>::id() == type) {
      return &_freq;
    }
    if constexpr (kNorm) {
      if (irs::Type<Norm>::id() == type) {
        return &_norm;
      }
    }
    return nullptr;
  }

  void Read(DataInput& in, size_t size) final {
    _freq.value = in.ReadV32();
    // TODO(mbkkt) don't compute vsize here
    const auto read = bytes_io<uint32_t>::vsize(_freq.value);
    // We need to always try to read norm, because we have compatibility
    // between BM25 in the index and TFIDF in the query
    [[maybe_unused]] auto norm = _freq.value;
    SDB_ASSERT(read <= size);
    if (read != size) {
      // TODO(mbkkt) if (!kNorm) in.skip(read - size);
      norm += in.ReadV32();
    }
    if constexpr (kNorm) {
      _norm.value = norm;
    }
  }

 private:
  FreqAttr _freq;
  [[no_unique_address]] utils::Need<kNorm, Norm> _norm;
};

}  // namespace irs
