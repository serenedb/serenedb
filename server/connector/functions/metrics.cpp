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

#include "connector/functions/metrics.h"

#include <absl/algorithm/container.h>
#include <absl/numeric/int128.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <core_functions/aggregate/sum_helpers.hpp>
#include <cstdint>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/aggregate_executor.hpp>
#include <duckdb/function/aggregate_function.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar/nested_functions.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

// Aggregates over OpenTelemetry metrics. Users see them in
// docs/sql/functions/aggregates/index.md, "Counter and Histogram Aggregates".
//
// Each aggregate collects the rows of a group into its state, sorts them by
// time in Finalize, and computes one result:
//
//   1. Counters and gauges, (value, ts):
//        CounterOperation<Kind>        otel_increase, otel_rate, otel_irate,
//                                      otel_delta, otel_idelta, otel_deriv,
//                                      otel_changes, otel_resets
//        PredictOperation              otel_predict_linear(value, ts, seconds)
//   2. Explicit-bound histograms, (bucket_counts, explicit_bounds):
//        BucketsOperation              otel_increase(bucket_counts, ts)
//        HistogramOperation<Shape>     otel_histogram_quantile, _fraction,
//                                      _stddev, _stdvar
//   3. Exponential histograms, packed by otel_exponential_histogram(...):
//        ExponentialOperation<Shape>   the same four histogram functions
//        ExponentialIncreaseOperation  otel_increase(histogram, ts)
//
// Shared rules: rows with a NULL or NaN value or an infinite time are skipped;
// of samples with the same time the largest counts; a counter that goes down
// was restarted.

namespace sdb::connector {
namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr double kInfinity = std::numeric_limits<double>::infinity();

bool IsFinite(int64_t ts) {
  return duckdb::Value::IsFinite(duckdb::timestamp_t{ts});
}

// Two finite timestamps can be further apart than int64 reaches.
double Elapsed(int64_t from, int64_t to) {
  return static_cast<double>(absl::int128{to} - from);
}

bool SameValue(double lhs, double rhs) {
  return lhs == rhs || (std::isnan(lhs) && std::isnan(rhs));
}

// ---- Shared: samples, states, ordering by time ----

struct Sample {
  int64_t ts;
  double value;
};

struct BucketSample {
  int64_t ts;
  double total = 0;
  std::vector<double> counts;
};

// An aggregate state is one pointer: DuckDB copies states as raw bytes, so
// they can't hold a vector. Allocated on the first row, freed in Destroy.
template<typename T>
struct Boxed {
  T* data;
};

using SeriesState = Boxed<std::vector<Sample>>;
using BucketsState = Boxed<std::vector<BucketSample>>;

template<typename T>
T& Unbox(Boxed<T>& state) {
  if (!state.data) {
    state.data = new T;
  }
  return *state.data;
}

// Combine may move the source's data out instead of copying it: DuckDB then
// combines each source once and never reads it again.
bool MayMove(const duckdb::AggregateInputData& input) {
  return input.combine_type ==
           duckdb::AggregateCombineType::ALLOW_DESTRUCTIVE &&
         !input.combine_multiplicities;
}

template<typename T>
T* Take(T& source, const duckdb::AggregateInputData& input) {
  return MayMove(input) ? new T{std::move(source)} : new T{source};
}

template<typename T>
void Append(std::vector<T>& source, std::vector<T>& target,
            const duckdb::AggregateInputData& input) {
  if (!MayMove(input)) {
    target.insert(target.end(), source.begin(), source.end());
    return;
  }
  if (target.size() < source.size()) {
    std::swap(target, source);
  }
  target.insert(target.end(), std::make_move_iterator(source.begin()),
                std::make_move_iterator(source.end()));
  source = std::vector<T>{};
}

template<typename T>
void Append(const Boxed<std::vector<T>>& source, Boxed<std::vector<T>>& target,
            const duckdb::AggregateInputData& input) {
  if (!source.data) {
    return;
  }
  if (!target.data) {
    target.data = Take(*source.data, input);
    return;
  }
  Append(*source.data, *target.data, input);
}

template<typename T, typename Rank>
void Normalize(std::vector<T>& samples, Rank rank) {
  absl::c_sort(samples, [&](const T& lhs, const T& rhs) {
    return lhs.ts != rhs.ts ? lhs.ts < rhs.ts : rank(lhs) > rank(rhs);
  });
  const auto [first, last] = std::ranges::unique(
    samples, [](const T& lhs, const T& rhs) { return lhs.ts == rhs.ts; });
  samples.erase(first, last);
}

double Increase(double previous, double current) {
  return current >= previous ? current - previous : current;
}

struct Line {
  double slope;
  double intercept;
};

// Least-squares line through the samples, x in seconds since `origin`.
// Kahan sums keep it precise for large timestamps.
Line Regression(std::span<const Sample> samples, int64_t origin,
                double ticks_per_second) {
  const auto first = samples.front().value;
  if (absl::c_all_of(
        samples, [&](const Sample& sample) { return sample.value == first; })) {
    return std::isinf(first) ? Line{.slope = kNaN, .intercept = kNaN}
                             : Line{.slope = 0, .intercept = first};
  }
  const auto seconds = [&](const Sample& sample) {
    return Elapsed(origin, sample.ts) / ticks_per_second;
  };
  std::array<double, 4> sums{};
  std::array<double, 4> errors{};
  for (const auto& sample : samples) {
    duckdb::KahanAddInternal(seconds(sample), sums[0], errors[0]);
    duckdb::KahanAddInternal(sample.value, sums[1], errors[1]);
  }
  const auto n = static_cast<double>(samples.size());
  const auto mean_x = (sums[0] - errors[0]) / n;
  const auto mean_y = (sums[1] - errors[1]) / n;
  for (const auto& sample : samples) {
    const auto dx = seconds(sample) - mean_x;
    duckdb::KahanAddInternal(dx * (sample.value - mean_y), sums[2], errors[2]);
    duckdb::KahanAddInternal(dx * dx, sums[3], errors[3]);
  }
  const auto slope = (sums[2] - errors[2]) / (sums[3] - errors[3]);
  return {.slope = slope, .intercept = mean_y - slope * mean_x};
}

struct BoxedOperation {
  template<typename State>
  static void Initialize(State& state) {
    state.data = nullptr;
  }

  template<typename State>
  static void Destroy(State& state, duckdb::AggregateInputData&) {
    delete state.data;
  }

  static bool IgnoreNull() { return true; }
};

// ---- 1. Counters and gauges ----

enum class Counter : uint8_t {
  Increase,
  Rate,
  InstantRate,
  Delta,
  InstantDelta,
  Deriv,
  Changes,
  Resets,
};

constexpr bool CountsEvents(Counter kind) {
  return kind == Counter::Changes || kind == Counter::Resets;
}

template<int64_t TicksPerSecond>
double Seconds(int64_t from, int64_t to) {
  return Elapsed(from, to) / TicksPerSecond;
}

template<Counter Kind, int64_t TicksPerSecond>
double Measure(std::span<const Sample> samples) {
  const auto& previous = samples[samples.size() - 2];
  const auto& current = samples.back();
  if constexpr (Kind == Counter::InstantRate) {
    return Increase(previous.value, current.value) /
           Seconds<TicksPerSecond>(previous.ts, current.ts);
  } else if constexpr (Kind == Counter::InstantDelta) {
    return current.value - previous.value;
  } else if constexpr (Kind == Counter::Delta) {
    return current.value - samples.front().value;
  } else if constexpr (Kind == Counter::Deriv) {
    return Regression(samples, samples.front().ts, TicksPerSecond).slope;
  } else {
    double increase = 0;
    for (size_t i = 1; i < samples.size(); ++i) {
      increase += Increase(samples[i - 1].value, samples[i].value);
    }
    if constexpr (Kind == Counter::Rate) {
      increase /=
        Seconds<TicksPerSecond>(samples.front().ts, samples.back().ts);
    }
    return increase;
  }
}

template<Counter Kind, int64_t TicksPerSecond>
struct CounterOperation : BoxedOperation {
  template<typename A, typename B, typename State, typename Op>
  static void Operation(State& state, const A& value, const B& ts,
                        duckdb::AggregateBinaryInput&) {
    if (!std::isnan(value) && IsFinite(ts)) {
      Unbox(state).emplace_back(ts, value);
    }
  }

  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData& input) {
    Append(source, target, input);
  }

  template<typename Result, typename State>
  static void Finalize(State& state, Result& target,
                       duckdb::AggregateFinalizeData& finalize) {
    if (!state.data) {
      finalize.ReturnNull();
      return;
    }
    auto& samples = *state.data;
    Normalize(samples, [](const Sample& sample) { return sample.value; });
    if constexpr (CountsEvents(Kind)) {
      Result events = 0;
      for (size_t i = 1; i < samples.size(); ++i) {
        const auto previous = samples[i - 1].value;
        const auto current = samples[i].value;
        events +=
          Kind == Counter::Changes ? current != previous : current < previous;
      }
      target = events;
    } else if (samples.size() < 2) {
      finalize.ReturnNull();
    } else {
      target = Measure<Kind, TicksPerSecond>(samples);
    }
  }
};

struct Prediction {
  double seconds = 0;
  std::vector<Sample> samples;
};

using PredictionState = Boxed<Prediction>;

[[noreturn]] void ThrowMixedSeconds() {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("otel_predict_linear(): all rows of a group must have "
            "the same seconds"));
}

template<int64_t TicksPerSecond>
struct PredictOperation : BoxedOperation {
  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData& input) {
    if (!source.data) {
      return;
    }
    if (!target.data) {
      target.data = Take(*source.data, input);
      return;
    }
    if (!SameValue(source.data->seconds, target.data->seconds)) {
      ThrowMixedSeconds();
    }
    Append(source.data->samples, target.data->samples, input);
  }

  template<typename Result, typename State>
  static void Finalize(State& state, Result& target,
                       duckdb::AggregateFinalizeData& finalize) {
    if (!state.data) {
      finalize.ReturnNull();
      return;
    }
    auto& samples = state.data->samples;
    Normalize(samples, [](const Sample& sample) { return sample.value; });
    if (samples.size() < 2) {
      finalize.ReturnNull();
      return;
    }
    const auto line = Regression(samples, samples.back().ts, TicksPerSecond);
    target = line.intercept + line.slope * state.data->seconds;
  }
};

void PredictUpdate(duckdb::Vector inputs[], duckdb::AggregateInputData&,
                   duckdb::idx_t, duckdb::Vector& states, duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat values;
  duckdb::UnifiedVectorFormat times;
  duckdb::UnifiedVectorFormat horizons;
  duckdb::UnifiedVectorFormat targets;
  inputs[0].ToUnifiedFormat(values);
  inputs[1].ToUnifiedFormat(times);
  inputs[2].ToUnifiedFormat(horizons);
  states.ToUnifiedFormat(targets);
  const auto* value = duckdb::UnifiedVectorFormat::GetData<double>(values);
  const auto* ts = duckdb::UnifiedVectorFormat::GetData<int64_t>(times);
  const auto* seconds = duckdb::UnifiedVectorFormat::GetData<double>(horizons);
  const auto* state =
    duckdb::UnifiedVectorFormat::GetData<PredictionState*>(targets);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto v = values.sel->get_index(i);
    const auto t = times.sel->get_index(i);
    const auto s = horizons.sel->get_index(i);
    if (!values.validity.RowIsValid(v) || !times.validity.RowIsValid(t) ||
        !horizons.validity.RowIsValid(s) || std::isnan(value[v]) ||
        !IsFinite(ts[t])) {
      continue;
    }
    auto*& data = state[targets.sel->get_index(i)]->data;
    if (!data) {
      data = new Prediction{.seconds = seconds[s]};
    } else if (!SameValue(data->seconds, seconds[s])) {
      ThrowMixedSeconds();
    }
    data->samples.emplace_back(ts[t], value[v]);
  }
}

// ---- 2. Explicit-bound histograms ----

struct BucketsOperation : BoxedOperation {
  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData& input) {
    Append(source, target, input);
  }

  template<typename Result, typename State>
  static void Finalize(State& state, Result& target,
                       duckdb::AggregateFinalizeData& finalize) {
    if (!state.data) {
      finalize.ReturnNull();
      return;
    }
    auto& samples = *state.data;
    Normalize(samples, [](const BucketSample& sample) { return sample.total; });
    if (samples.size() < 2) {
      finalize.ReturnNull();
      return;
    }
    std::vector<double> increase(samples.front().counts.size());
    for (size_t i = 1; i < samples.size(); ++i) {
      const auto& previous = samples[i - 1].counts;
      const auto& current = samples[i].counts;
      if (current.size() != increase.size()) {
        increase = current;
        continue;
      }
      bool reset = false;
      for (size_t j = 0; !reset && j != current.size(); ++j) {
        reset = current[j] < previous[j];
      }
      for (size_t j = 0; j != current.size(); ++j) {
        increase[j] += reset ? current[j] : current[j] - previous[j];
      }
    }
    auto& child = duckdb::ListVector::GetChildMutable(finalize.result);
    const auto offset = duckdb::ListVector::GetListSize(finalize.result);
    duckdb::ListVector::Reserve(finalize.result, offset + increase.size());
    absl::c_copy(increase,
                 duckdb::FlatVector::GetDataMutable<double>(child) + offset);
    target.offset = offset;
    target.length = increase.size();
    duckdb::ListVector::SetListSize(finalize.result, offset + increase.size());
  }
};

void BucketsUpdate(duckdb::Vector inputs[], duckdb::AggregateInputData&,
                   duckdb::idx_t, duckdb::Vector& states, duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat lists;
  duckdb::UnifiedVectorFormat times;
  duckdb::UnifiedVectorFormat values;
  duckdb::UnifiedVectorFormat targets;
  inputs[0].ToUnifiedFormat(lists);
  inputs[1].ToUnifiedFormat(times);
  duckdb::ListVector::GetChild(inputs[0]).ToUnifiedFormat(values);
  states.ToUnifiedFormat(targets);
  const auto* entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(lists);
  const auto* ts = duckdb::UnifiedVectorFormat::GetData<int64_t>(times);
  const auto* counts = duckdb::UnifiedVectorFormat::GetData<double>(values);
  const auto* state =
    duckdb::UnifiedVectorFormat::GetData<BucketsState*>(targets);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto list = lists.sel->get_index(i);
    const auto time = times.sel->get_index(i);
    if (!lists.validity.RowIsValid(list) || !times.validity.RowIsValid(time) ||
        !IsFinite(ts[time])) {
      continue;
    }
    const auto& entry = entries[list];
    BucketSample sample{.ts = ts[time]};
    sample.counts.reserve(entry.length);
    for (duckdb::idx_t j = 0; j != entry.length; ++j) {
      const auto value = values.sel->get_index(entry.offset + j);
      const double bucket =
        values.validity.RowIsValid(value) ? counts[value] : 0;
      sample.counts.push_back(bucket);
      sample.total += bucket;
    }
    if (std::isnan(sample.total)) {
      continue;
    }
    Unbox(*state[targets.sel->get_index(i)]).push_back(std::move(sample));
  }
}

// What a histogram function asks; Arity is its leading scalar arguments:
// q for Quantile, lower and upper for Fraction.
enum class Shape : uint8_t {
  Quantile,
  Fraction,
  StdDev,
  StdVar,
};

constexpr size_t Arity(Shape shape) {
  switch (shape) {
    case Shape::Quantile:
      return 1;
    case Shape::Fraction:
      return 2;
    case Shape::StdDev:
    case Shape::StdVar:
      return 0;
  }
  return 0;
}

constexpr std::string_view Name(Shape shape) {
  switch (shape) {
    case Shape::Quantile:
      return "otel_histogram_quantile";
    case Shape::Fraction:
      return "otel_histogram_fraction";
    case Shape::StdDev:
      return "otel_histogram_stddev";
    case Shape::StdVar:
      return "otel_histogram_stdvar";
  }
  return {};
}

constexpr std::string_view ExplicitArguments(Shape shape) {
  switch (shape) {
    case Shape::Quantile:
      return "quantile and explicit_bounds";
    case Shape::Fraction:
      return "lower, upper and explicit_bounds";
    case Shape::StdDev:
    case Shape::StdVar:
      return "explicit_bounds";
  }
  return {};
}

constexpr std::string_view ExponentialArguments(Shape shape) {
  return shape == Shape::Quantile ? "quantile" : "lower and upper";
}

using Arguments = std::array<double, 2>;

template<Shape Kind>
bool SameArguments(const Arguments& lhs, const Arguments& rhs) {
  for (size_t i = 0; i != Arity(Kind); ++i) {
    if (!SameValue(lhs[i], rhs[i])) {
      return false;
    }
  }
  return true;
}

// The bucket counts of a group added up. Its rows must share the arguments and
// explicit_bounds.
struct Histogram {
  Arguments arguments{};
  std::vector<double> bounds;
  std::vector<double> counts;
};

using HistogramState = Boxed<Histogram>;

template<Shape Kind>
[[noreturn]] void ThrowMixedHistograms() {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG(Name(Kind), "(): all rows of a group must have the same ",
            ExplicitArguments(Kind)));
}

template<Shape Kind>
void Merge(const Histogram& source, Histogram& target) {
  if (!SameArguments<Kind>(source.arguments, target.arguments) ||
      source.bounds != target.bounds) {
    ThrowMixedHistograms<Kind>();
  }
  for (size_t i = 0; i != source.counts.size(); ++i) {
    target.counts[i] += source.counts[i];
  }
}

// Bucket i holds values up to bounds[i], the last one values above the last
// bound. Observations are taken as spread evenly inside a bucket, and the
// first bucket starts at 0 unless its bound is 0 or less.
std::optional<double> BucketQuantile(double quantile,
                                     std::span<const double> bounds,
                                     std::span<const double> counts) {
  if (std::isnan(quantile)) {
    return quantile;
  }
  if (quantile < 0) {
    return -kInfinity;
  }
  if (quantile > 1) {
    return kInfinity;
  }
  const double total = absl::c_accumulate(counts, 0.0);
  if (bounds.empty() || !(total > 0)) {
    return std::nullopt;
  }
  const double rank = quantile * total;
  double below = 0;
  for (size_t i = 0; i != bounds.size(); ++i) {
    if (below + counts[i] >= rank) {
      if (i == 0 && bounds[0] <= 0) {
        return bounds[0];
      }
      const double lower = i == 0 ? 0 : bounds[i - 1];
      if (counts[i] == 0) {
        return lower;
      }
      return lower + (bounds[i] - lower) * (rank - below) / counts[i];
    }
    below += counts[i];
  }
  return bounds.back();
}

double BucketsBelow(double value, std::span<const double> bounds,
                    std::span<const double> counts) {
  double below = 0;
  for (size_t i = 0; i != counts.size(); ++i) {
    const double lower = i != 0          ? bounds[i - 1]
                         : bounds[0] > 0 ? 0
                                         : -kInfinity;
    const double upper = i != bounds.size() ? bounds[i] : kInfinity;
    if (value >= upper) {
      below += counts[i];
      continue;
    }
    if (value > lower && std::isinf(lower)) {
      below += counts[i];
    } else if (value > lower && !std::isinf(upper)) {
      below += counts[i] * (value - lower) / (upper - lower);
    }
    break;
  }
  return below;
}

std::optional<double> BucketFraction(double lower, double upper,
                                     std::span<const double> bounds,
                                     std::span<const double> counts) {
  const double total = absl::c_accumulate(counts, 0.0);
  if (bounds.empty() || !(total > 0)) {
    return std::nullopt;
  }
  if (std::isnan(lower) || std::isnan(upper)) {
    return kNaN;
  }
  if (lower >= upper) {
    return 0.0;
  }
  return (BucketsBelow(upper, bounds, counts) -
          BucketsBelow(lower, bounds, counts)) /
         total;
}

// Each observation taken at the middle of its bucket.
std::optional<double> BucketVariance(std::span<const double> bounds,
                                     std::span<const double> counts) {
  const double total = absl::c_accumulate(counts, 0.0);
  if (bounds.empty() || !(total > 0)) {
    return std::nullopt;
  }
  const auto representative = [&](size_t i) {
    if (i == 0) {
      return bounds[0] > 0 ? bounds[0] / 2 : bounds[0];
    }
    if (i == bounds.size()) {
      return bounds.back();
    }
    return (bounds[i - 1] + bounds[i]) / 2;
  };
  double mean = 0;
  for (size_t i = 0; i != counts.size(); ++i) {
    mean += counts[i] * representative(i);
  }
  mean /= total;
  double variance = 0;
  for (size_t i = 0; i != counts.size(); ++i) {
    const auto delta = representative(i) - mean;
    variance += counts[i] * delta * delta;
  }
  return variance / total;
}

template<Shape Kind>
std::optional<double> FinishVariance(std::optional<double> variance) {
  if (variance && Kind == Shape::StdDev) {
    *variance = std::sqrt(*variance);
  }
  return variance;
}

template<Shape Kind>
std::optional<double> Evaluate(const Histogram& histogram) {
  if constexpr (Kind == Shape::Quantile) {
    return BucketQuantile(histogram.arguments[0], histogram.bounds,
                          histogram.counts);
  } else if constexpr (Kind == Shape::Fraction) {
    return BucketFraction(histogram.arguments[0], histogram.arguments[1],
                          histogram.bounds, histogram.counts);
  } else {
    return FinishVariance<Kind>(
      BucketVariance(histogram.bounds, histogram.counts));
  }
}

template<Shape Kind>
struct HistogramOperation : BoxedOperation {
  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData& input) {
    if (!source.data) {
      return;
    }
    if (!target.data) {
      target.data = Take(*source.data, input);
      return;
    }
    Merge<Kind>(*source.data, *target.data);
  }

  template<typename Result, typename State>
  static void Finalize(State& state, Result& target,
                       duckdb::AggregateFinalizeData& finalize) {
    if (!state.data) {
      finalize.ReturnNull();
      return;
    }
    const auto result = Evaluate<Kind>(*state.data);
    if (!result) {
      finalize.ReturnNull();
      return;
    }
    target = *result;
  }
};

class ArgumentRows {
 public:
  explicit ArgumentRows(duckdb::Vector inputs[], size_t arity) : _arity{arity} {
    for (size_t i = 0; i != arity; ++i) {
      inputs[i].ToUnifiedFormat(_formats[i]);
    }
  }

  [[nodiscard]] bool Read(duckdb::idx_t row, Arguments& out) const {
    for (size_t i = 0; i != _arity; ++i) {
      const auto index = _formats[i].sel->get_index(row);
      if (!_formats[i].validity.RowIsValid(index)) {
        return false;
      }
      out[i] = duckdb::UnifiedVectorFormat::GetData<double>(_formats[i])[index];
    }
    return true;
  }

 private:
  size_t _arity;
  std::array<duckdb::UnifiedVectorFormat, 2> _formats;
};

// The first row of a group fixes and validates the bounds; later rows must
// repeat them.
template<Shape Kind>
void HistogramUpdate(duckdb::Vector inputs[], duckdb::AggregateInputData&,
                     duckdb::idx_t, duckdb::Vector& states,
                     duckdb::idx_t count) {
  constexpr auto kArity = Arity(Kind);
  const ArgumentRows arguments{inputs, kArity};
  duckdb::UnifiedVectorFormat count_lists;
  duckdb::UnifiedVectorFormat bound_lists;
  duckdb::UnifiedVectorFormat count_values;
  duckdb::UnifiedVectorFormat bound_values;
  duckdb::UnifiedVectorFormat targets;
  inputs[kArity].ToUnifiedFormat(count_lists);
  inputs[kArity + 1].ToUnifiedFormat(bound_lists);
  duckdb::ListVector::GetChild(inputs[kArity]).ToUnifiedFormat(count_values);
  duckdb::ListVector::GetChild(inputs[kArity + 1])
    .ToUnifiedFormat(bound_values);
  states.ToUnifiedFormat(targets);
  const auto* count_entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(count_lists);
  const auto* bound_entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(bound_lists);
  const auto* counts =
    duckdb::UnifiedVectorFormat::GetData<double>(count_values);
  const auto* bounds =
    duckdb::UnifiedVectorFormat::GetData<double>(bound_values);
  const auto* state =
    duckdb::UnifiedVectorFormat::GetData<HistogramState*>(targets);
  const auto bound = [&](const duckdb::list_entry_t& entry, duckdb::idx_t j) {
    const auto value = bound_values.sel->get_index(entry.offset + j);
    if (!bound_values.validity.RowIsValid(value)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
        ERR_MSG(Name(Kind), "(): explicit_bounds must not contain NULL"));
    }
    return bounds[value];
  };
  Arguments row_arguments{};
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto c = count_lists.sel->get_index(i);
    const auto b = bound_lists.sel->get_index(i);
    if (!arguments.Read(i, row_arguments) ||
        !count_lists.validity.RowIsValid(c) ||
        !bound_lists.validity.RowIsValid(b) || count_entries[c].length == 0) {
      continue;
    }
    const auto& count_entry = count_entries[c];
    const auto& bound_entry = bound_entries[b];
    if (count_entry.length != bound_entry.length + 1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG(Name(Kind),
                              "(): bucket_counts must have one element "
                              "more than explicit_bounds, got ",
                              count_entry.length, " and ", bound_entry.length));
    }
    bool nan = false;
    for (duckdb::idx_t j = 0; !nan && j != count_entry.length; ++j) {
      const auto value = count_values.sel->get_index(count_entry.offset + j);
      nan =
        count_values.validity.RowIsValid(value) && std::isnan(counts[value]);
    }
    if (nan) {
      continue;
    }
    auto& histogram = Unbox(*state[targets.sel->get_index(i)]);
    if (histogram.counts.empty()) {
      histogram.arguments = row_arguments;
      histogram.bounds.reserve(bound_entry.length);
      for (duckdb::idx_t j = 0; j != bound_entry.length; ++j) {
        const auto value = bound(bound_entry, j);
        if (std::isnan(value) ||
            (!histogram.bounds.empty() && !(histogram.bounds.back() < value))) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG(Name(Kind),
                    "(): explicit_bounds must be strictly ascending numbers"));
        }
        histogram.bounds.push_back(value);
      }
      histogram.counts.assign(count_entry.length, 0);
    } else {
      bool same = SameArguments<Kind>(row_arguments, histogram.arguments) &&
                  histogram.bounds.size() == bound_entry.length;
      for (duckdb::idx_t j = 0; same && j != bound_entry.length; ++j) {
        same = histogram.bounds[j] == bound(bound_entry, j);
      }
      if (!same) {
        ThrowMixedHistograms<Kind>();
      }
    }
    for (duckdb::idx_t j = 0; j != count_entry.length; ++j) {
      const auto value = count_values.sel->get_index(count_entry.offset + j);
      if (count_values.validity.RowIsValid(value)) {
        histogram.counts[j] += counts[value];
      }
    }
  }
}

// ---- 3. Exponential histograms ----
//
// OTel's base-2 layout: on each side, bucket i covers
// (2^(i * 2^-scale), 2^((i + 1) * 2^-scale)]. The negative side holds absolute
// values, zeros are counted apart. One scale lower merges every two
// neighbouring buckets into one.

// Rows are added at a coarser scale when one side would need more buckets.
constexpr int64_t kMaxExponentialBuckets = 4096;
// The scales OTel SDKs produce.
constexpr int32_t kMinExponentialScale = -10;
constexpr int32_t kMaxExponentialScale = 20;

// counts[k] is bucket offset + k.
struct ExponentialBuckets {
  int64_t offset = 0;
  std::vector<double> counts;
};

int64_t Last(const ExponentialBuckets& buckets) {
  return buckets.offset + static_cast<int64_t>(buckets.counts.size()) - 1;
}

double At(const ExponentialBuckets& buckets, int64_t index) {
  return index >= buckets.offset && index <= Last(buckets)
           ? buckets.counts[static_cast<size_t>(index - buckets.offset)]
           : 0;
}

struct ExponentialHistogram {
  int32_t scale = 0;
  double zero = 0;
  ExponentialBuckets positive;
  ExponentialBuckets negative;
};

int64_t Shifted(int64_t index, int32_t shift) {
  return index >> std::min<int32_t>(shift, 62);
}

constexpr int64_t kMinExponentialIndex =
  -(int64_t{1075} << kMaxExponentialScale);
constexpr int64_t kMaxExponentialIndex =
  (int64_t{1024} << kMaxExponentialScale) - 1;

// Buckets beyond kMin/MaxExponentialIndex, the range of DOUBLE at the finest
// scale, can't hold an observation; rejecting them keeps every result at
// scale >= kMinExponentialScale with offsets that fit INTEGER.
bool Representable(const ExponentialBuckets& buckets, int32_t scale) {
  const auto shift = kMaxExponentialScale - scale;
  return buckets.counts.empty() ||
         (buckets.offset >= Shifted(kMinExponentialIndex, shift) &&
          Last(buckets) <= Shifted(kMaxExponentialIndex, shift));
}

void Cover(ExponentialBuckets& buckets, int64_t first, int64_t last) {
  if (buckets.counts.empty()) {
    buckets.offset = first;
    buckets.counts.assign(static_cast<size_t>(last - first) + 1, 0);
    return;
  }
  if (first < buckets.offset) {
    buckets.counts.insert(buckets.counts.begin(),
                          static_cast<size_t>(buckets.offset - first), 0);
    buckets.offset = first;
  }
  if (last > Last(buckets)) {
    buckets.counts.resize(static_cast<size_t>(last - buckets.offset) + 1, 0);
  }
}

void Add(ExponentialBuckets& target, const ExponentialBuckets& source,
         int32_t shift, double sign) {
  if (source.counts.empty()) {
    return;
  }
  Cover(target, Shifted(source.offset, shift), Shifted(Last(source), shift));
  for (size_t i = 0; i != source.counts.size(); ++i) {
    const auto index =
      Shifted(source.offset + static_cast<int64_t>(i), shift) - target.offset;
    target.counts[static_cast<size_t>(index)] += sign * source.counts[i];
  }
}

void Downscale(ExponentialBuckets& buckets, int32_t shift) {
  if (shift == 0 || buckets.counts.empty()) {
    return;
  }
  ExponentialBuckets downscaled;
  Add(downscaled, buckets, shift, 1);
  buckets = std::move(downscaled);
}

void Downscale(ExponentialHistogram& histogram, int32_t scale) {
  Downscale(histogram.positive, histogram.scale - scale);
  Downscale(histogram.negative, histogram.scale - scale);
  histogram.scale = scale;
}

struct Extent {
  int64_t first = std::numeric_limits<int64_t>::max();
  int64_t last = std::numeric_limits<int64_t>::min();
};

void Include(Extent& extent, const ExponentialBuckets& buckets, int32_t shift) {
  if (buckets.counts.empty()) {
    return;
  }
  extent.first = std::min(extent.first, Shifted(buckets.offset, shift));
  extent.last = std::max(extent.last, Shifted(Last(buckets), shift));
}

bool Fits(const Extent& extent) {
  return extent.first > extent.last ||
         extent.last - extent.first < kMaxExponentialBuckets;
}

// The finest scale at which every histogram fits kMaxExponentialBuckets per
// side.
int32_t CommonScale(std::span<const ExponentialHistogram* const> histograms) {
  auto scale = std::numeric_limits<int32_t>::max();
  for (const auto* histogram : histograms) {
    scale = std::min(scale, histogram->scale);
  }
  while (true) {
    Extent positive;
    Extent negative;
    for (const auto* histogram : histograms) {
      Include(positive, histogram->positive, histogram->scale - scale);
      Include(negative, histogram->negative, histogram->scale - scale);
    }
    if (Fits(positive) && Fits(negative)) {
      return scale;
    }
    --scale;
  }
}

void Merge(ExponentialHistogram& target, const ExponentialHistogram& source,
           double sign) {
  const std::array<const ExponentialHistogram*, 2> both{&target, &source};
  Downscale(target, CommonScale(both));
  target.zero += sign * source.zero;
  Add(target.positive, source.positive, source.scale - target.scale, sign);
  Add(target.negative, source.negative, source.scale - target.scale, sign);
}

// The value `fraction` of the way through bucket `index`, measured in powers
// of 2, since the buckets grow geometrically.
double Boundary(int64_t index, int32_t scale, double fraction) {
  return std::exp2(std::ldexp(static_cast<double>(index) + fraction, -scale));
}

enum class Side : uint8_t {
  Negative,
  Zero,
  Positive,
};

struct ExponentialBucket {
  Side side;
  int64_t index;
  double count;
};

// Visits the buckets from the lowest value up: the negative side, the zeros,
// the positive side. Stops when `visit` returns false.
template<typename Visit>
void Ascending(const ExponentialHistogram& histogram, Visit visit) {
  const auto& negative = histogram.negative;
  for (auto i = Last(negative);
       !negative.counts.empty() && i >= negative.offset; --i) {
    if (!visit(ExponentialBucket{
          .side = Side::Negative, .index = i, .count = At(negative, i)})) {
      return;
    }
  }
  if (!visit(ExponentialBucket{Side::Zero, 0, histogram.zero})) {
    return;
  }
  const auto& positive = histogram.positive;
  for (auto i = positive.offset;
       !positive.counts.empty() && i <= Last(positive); ++i) {
    if (!visit(ExponentialBucket{
          .side = Side::Positive, .index = i, .count = At(positive, i)})) {
      return;
    }
  }
}

double Total(const ExponentialHistogram& histogram) {
  double total = 0;
  Ascending(histogram, [&](const ExponentialBucket& bucket) {
    total += bucket.count;
    return true;
  });
  return total;
}

std::optional<double> ExponentialQuantile(
  double quantile, const ExponentialHistogram& histogram) {
  if (std::isnan(quantile)) {
    return quantile;
  }
  if (quantile < 0) {
    return -kInfinity;
  }
  if (quantile > 1) {
    return kInfinity;
  }
  const double total = Total(histogram);
  if (!(total > 0)) {
    return std::nullopt;
  }
  const double rank = quantile * total;
  double below = 0;
  double result = kNaN;
  Ascending(histogram, [&](const ExponentialBucket& bucket) {
    if (bucket.count > 0) {
      const auto fraction = (rank - below) / bucket.count;
      switch (bucket.side) {
        case Side::Negative:
          result = -Boundary(bucket.index, histogram.scale, 1 - fraction);
          break;
        case Side::Zero:
          result = 0;
          break;
        case Side::Positive:
          result = Boundary(bucket.index, histogram.scale, fraction);
          break;
      }
    }
    below += bucket.count;
    return bucket.count <= 0 || below < rank;
  });
  return result;
}

double ExponentialBelow(double value, const ExponentialHistogram& histogram) {
  double below = 0;
  Ascending(histogram, [&](const ExponentialBucket& bucket) {
    double lower = 0;
    double upper = 0;
    switch (bucket.side) {
      case Side::Negative:
        lower = -Boundary(bucket.index, histogram.scale, 1);
        upper = -Boundary(bucket.index, histogram.scale, 0);
        break;
      case Side::Zero:
        break;
      case Side::Positive:
        lower = Boundary(bucket.index, histogram.scale, 0);
        upper = Boundary(bucket.index, histogram.scale, 1);
        break;
    }
    if (value >= upper) {
      below += bucket.count;
      return true;
    }
    if (value > lower) {
      const auto position =
        std::ldexp(std::log2(std::abs(value)), histogram.scale) - bucket.index;
      below += bucket.count *
               (bucket.side == Side::Positive ? position : 1 - position);
    }
    return false;
  });
  return below;
}

std::optional<double> ExponentialFraction(
  double lower, double upper, const ExponentialHistogram& histogram) {
  const double total = Total(histogram);
  if (!(total > 0)) {
    return std::nullopt;
  }
  if (std::isnan(lower) || std::isnan(upper)) {
    return kNaN;
  }
  if (lower >= upper) {
    return 0.0;
  }
  return (ExponentialBelow(upper, histogram) -
          ExponentialBelow(lower, histogram)) /
         total;
}

// Each observation taken at the geometric middle of its bucket, zeros at 0.
std::optional<double> ExponentialVariance(
  const ExponentialHistogram& histogram) {
  const double total = Total(histogram);
  if (!(total > 0)) {
    return std::nullopt;
  }
  const auto representative = [&](const ExponentialBucket& bucket) {
    switch (bucket.side) {
      case Side::Negative:
        return -Boundary(bucket.index, histogram.scale, 0.5);
      case Side::Zero:
        return 0.0;
      case Side::Positive:
        return Boundary(bucket.index, histogram.scale, 0.5);
    }
    return 0.0;
  };
  double mean = 0;
  Ascending(histogram, [&](const ExponentialBucket& bucket) {
    if (bucket.count != 0) {
      mean += bucket.count * representative(bucket);
    }
    return true;
  });
  mean /= total;
  double variance = 0;
  Ascending(histogram, [&](const ExponentialBucket& bucket) {
    if (bucket.count != 0) {
      const auto delta = representative(bucket) - mean;
      variance += bucket.count * delta * delta;
    }
    return true;
  });
  return variance / total;
}

// Fields of the packed histogram STRUCT, in otel_exponential_histogram's
// argument order.
constexpr size_t kScale = 0;
constexpr size_t kZeroCount = 1;
constexpr size_t kPositiveOffset = 2;
constexpr size_t kPositiveCounts = 3;
constexpr size_t kNegativeOffset = 4;
constexpr size_t kNegativeCounts = 5;

duckdb::LogicalType ExponentialHistogramType() {
  const auto counts = duckdb::LogicalType::LIST(duckdb::LogicalType::DOUBLE);
  return duckdb::LogicalType::STRUCT({
    {"scale", duckdb::LogicalType::INTEGER},
    {"zero_count", duckdb::LogicalType::DOUBLE},
    {"positive_offset", duckdb::LogicalType::INTEGER},
    {"positive_bucket_counts", counts},
    {"negative_offset", duckdb::LogicalType::INTEGER},
    {"negative_bucket_counts", counts},
  });
}

// One packed histogram per row; a NULL histogram or scale skips the row.
class ExponentialRows {
 public:
  explicit ExponentialRows(const duckdb::Vector& input,
                           std::string_view function)
    : _function{function} {
    duckdb::Vector::RecursiveToUnifiedFormat(input, _format);
  }

  [[nodiscard]] bool Read(duckdb::idx_t row, ExponentialHistogram& out) const {
    const auto index = _format.unified.sel->get_index(row);
    if (!_format.unified.validity.RowIsValid(index)) {
      return false;
    }
    const auto& scale = _format.children[kScale].unified;
    const auto scale_index = scale.sel->get_index(index);
    if (!scale.validity.RowIsValid(scale_index)) {
      return false;
    }
    out.scale =
      duckdb::UnifiedVectorFormat::GetData<int32_t>(scale)[scale_index];
    if (out.scale < kMinExponentialScale || out.scale > kMaxExponentialScale) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(_function, "(): scale must be between ", kMinExponentialScale,
                " and ", kMaxExponentialScale, ", got ", out.scale));
    }
    out.zero = Number(_format.children[kZeroCount].unified, index);
    ReadBuckets(_format.children[kPositiveOffset].unified,
                _format.children[kPositiveCounts], index, out.positive);
    ReadBuckets(_format.children[kNegativeOffset].unified,
                _format.children[kNegativeCounts], index, out.negative);
    if (!Representable(out.positive, out.scale) ||
        !Representable(out.negative, out.scale)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(_function, "(): buckets beyond the range of DOUBLE at scale ",
                out.scale));
    }
    return true;
  }

 private:
  static double Number(const duckdb::UnifiedVectorFormat& format,
                       duckdb::idx_t index) {
    const auto value = format.sel->get_index(index);
    return format.validity.RowIsValid(value)
             ? duckdb::UnifiedVectorFormat::GetData<double>(format)[value]
             : 0;
  }

  static void ReadBuckets(const duckdb::UnifiedVectorFormat& offset,
                          const duckdb::RecursiveUnifiedVectorFormat& counts,
                          duckdb::idx_t index, ExponentialBuckets& out) {
    out.counts.clear();
    out.offset = 0;
    const auto list = counts.unified.sel->get_index(index);
    if (!counts.unified.validity.RowIsValid(list)) {
      return;
    }
    const auto& entry =
      duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
        counts.unified)[list];
    if (entry.length == 0) {
      return;
    }
    const auto start = offset.sel->get_index(index);
    if (offset.validity.RowIsValid(start)) {
      out.offset = duckdb::UnifiedVectorFormat::GetData<int32_t>(offset)[start];
    }
    const auto& values = counts.children[0].unified;
    const auto* data = duckdb::UnifiedVectorFormat::GetData<double>(values);
    out.counts.reserve(entry.length);
    for (duckdb::idx_t j = 0; j != entry.length; ++j) {
      const auto value = values.sel->get_index(entry.offset + j);
      out.counts.push_back(values.validity.RowIsValid(value) ? data[value] : 0);
    }
  }

  duckdb::RecursiveUnifiedVectorFormat _format;
  std::string_view _function;
};

// The histograms of a group added up. Its rows must share the arguments.
struct ExponentialAggregate {
  Arguments arguments{};
  ExponentialHistogram histogram;
};

using ExponentialState = Boxed<ExponentialAggregate>;

template<Shape Kind>
[[noreturn]] void ThrowMixedArguments() {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG(Name(Kind), "(): all rows of a group must have the same ",
            ExponentialArguments(Kind)));
}

template<Shape Kind>
std::optional<double> Evaluate(const ExponentialAggregate& aggregate) {
  if constexpr (Kind == Shape::Quantile) {
    return ExponentialQuantile(aggregate.arguments[0], aggregate.histogram);
  } else if constexpr (Kind == Shape::Fraction) {
    return ExponentialFraction(aggregate.arguments[0], aggregate.arguments[1],
                               aggregate.histogram);
  } else {
    return FinishVariance<Kind>(ExponentialVariance(aggregate.histogram));
  }
}

template<Shape Kind>
struct ExponentialOperation : BoxedOperation {
  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData& input) {
    if (!source.data) {
      return;
    }
    if (!target.data) {
      target.data = Take(*source.data, input);
      return;
    }
    if (!SameArguments<Kind>(source.data->arguments, target.data->arguments)) {
      ThrowMixedArguments<Kind>();
    }
    Merge(target.data->histogram, source.data->histogram, 1);
  }

  template<typename Result, typename State>
  static void Finalize(State& state, Result& target,
                       duckdb::AggregateFinalizeData& finalize) {
    if (!state.data) {
      finalize.ReturnNull();
      return;
    }
    const auto result = Evaluate<Kind>(*state.data);
    if (!result) {
      finalize.ReturnNull();
      return;
    }
    target = *result;
  }
};

template<Shape Kind>
void ExponentialUpdate(duckdb::Vector inputs[], duckdb::AggregateInputData&,
                       duckdb::idx_t, duckdb::Vector& states,
                       duckdb::idx_t count) {
  constexpr auto kArity = Arity(Kind);
  const ArgumentRows arguments{inputs, kArity};
  const ExponentialRows rows{inputs[kArity], Name(Kind)};
  duckdb::UnifiedVectorFormat targets;
  states.ToUnifiedFormat(targets);
  const auto* state =
    duckdb::UnifiedVectorFormat::GetData<ExponentialState*>(targets);
  Arguments row_arguments{};
  ExponentialHistogram row;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (!arguments.Read(i, row_arguments) || !rows.Read(i, row) ||
        std::isnan(Total(row))) {
      continue;
    }
    auto*& data = state[targets.sel->get_index(i)]->data;
    if (!data) {
      data =
        new ExponentialAggregate{.arguments = row_arguments, .histogram = row};
      continue;
    }
    if (!SameArguments<Kind>(row_arguments, data->arguments)) {
      ThrowMixedArguments<Kind>();
    }
    Merge(data->histogram, row, 1);
  }
}

// otel_increase(histogram, ts): the increase of every bucket, as a histogram
// at the coarsest scale among the samples. Any bucket or the zeros going down
// means a restart.
struct ExponentialSample {
  int64_t ts;
  double total;
  ExponentialHistogram histogram;
};

using ExponentialSeriesState = Boxed<std::vector<ExponentialSample>>;

bool Decreased(const ExponentialBuckets& previous,
               const ExponentialBuckets& current) {
  if (previous.counts.empty()) {
    return false;
  }
  for (auto i = previous.offset; i <= Last(previous); ++i) {
    if (At(current, i) < At(previous, i)) {
      return true;
    }
  }
  return false;
}

void Trim(ExponentialBuckets& buckets) {
  auto& counts = buckets.counts;
  size_t first = 0;
  while (first != counts.size() && counts[first] == 0) {
    ++first;
  }
  if (first == counts.size()) {
    counts.clear();
    buckets.offset = 0;
    return;
  }
  auto end = counts.size();
  while (counts[end - 1] == 0) {
    --end;
  }
  counts.erase(counts.begin() + static_cast<ptrdiff_t>(end), counts.end());
  counts.erase(counts.begin(), counts.begin() + static_cast<ptrdiff_t>(first));
  buckets.offset += static_cast<int64_t>(first);
}

template<typename T>
void WriteList(duckdb::Vector& list, duckdb::idx_t row,
               std::span<const T> values) {
  auto& child = duckdb::ListVector::GetChildMutable(list);
  const auto offset = duckdb::ListVector::GetListSize(list);
  duckdb::ListVector::Reserve(list, offset + values.size());
  absl::c_copy(values, duckdb::FlatVector::GetDataMutable<T>(child) + offset);
  duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(list)[row] = {
    offset, values.size()};
  duckdb::ListVector::SetListSize(list, offset + values.size());
}

void WriteHistogram(duckdb::Vector& result, duckdb::idx_t row,
                    const ExponentialHistogram& histogram) {
  auto& fields = duckdb::StructVector::GetEntries(result);
  duckdb::FlatVector::GetDataMutable<int32_t>(fields[kScale])[row] =
    histogram.scale;
  duckdb::FlatVector::GetDataMutable<double>(fields[kZeroCount])[row] =
    histogram.zero;
  duckdb::FlatVector::GetDataMutable<int32_t>(fields[kPositiveOffset])[row] =
    static_cast<int32_t>(histogram.positive.offset);
  WriteList<double>(fields[kPositiveCounts], row, histogram.positive.counts);
  duckdb::FlatVector::GetDataMutable<int32_t>(fields[kNegativeOffset])[row] =
    static_cast<int32_t>(histogram.negative.offset);
  WriteList<double>(fields[kNegativeCounts], row, histogram.negative.counts);
}

struct ExponentialIncreaseOperation : BoxedOperation {
  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData& input) {
    Append(source, target, input);
  }

  template<typename State>
  static void Finalize(State& state, duckdb::AggregateFinalizeData& finalize) {
    if (!state.data) {
      finalize.ReturnNull();
      return;
    }
    auto& samples = *state.data;
    Normalize(samples,
              [](const ExponentialSample& sample) { return sample.total; });
    if (samples.size() < 2) {
      finalize.ReturnNull();
      return;
    }
    std::vector<const ExponentialHistogram*> histograms;
    histograms.reserve(samples.size());
    for (const auto& sample : samples) {
      histograms.push_back(&sample.histogram);
    }
    const auto scale = CommonScale(histograms);
    for (auto& sample : samples) {
      Downscale(sample.histogram, scale);
    }
    ExponentialHistogram increase{.scale = scale};
    for (size_t i = 1; i < samples.size(); ++i) {
      const auto& previous = samples[i - 1].histogram;
      const auto& current = samples[i].histogram;
      Merge(increase, current, 1);
      if (current.zero >= previous.zero &&
          !Decreased(previous.positive, current.positive) &&
          !Decreased(previous.negative, current.negative)) {
        Merge(increase, previous, -1);
      }
    }
    Trim(increase.positive);
    Trim(increase.negative);
    WriteHistogram(finalize.result, finalize.result_idx, increase);
  }
};

void ExponentialIncreaseUpdate(duckdb::Vector inputs[],
                               duckdb::AggregateInputData&, duckdb::idx_t,
                               duckdb::Vector& states, duckdb::idx_t count) {
  const ExponentialRows rows{inputs[0], "otel_increase"};
  duckdb::UnifiedVectorFormat times;
  duckdb::UnifiedVectorFormat targets;
  inputs[1].ToUnifiedFormat(times);
  states.ToUnifiedFormat(targets);
  const auto* ts = duckdb::UnifiedVectorFormat::GetData<int64_t>(times);
  const auto* state =
    duckdb::UnifiedVectorFormat::GetData<ExponentialSeriesState*>(targets);
  ExponentialHistogram row;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto time = times.sel->get_index(i);
    if (!times.validity.RowIsValid(time) || !IsFinite(ts[time]) ||
        !rows.Read(i, row)) {
      continue;
    }
    const auto total = Total(row);
    if (std::isnan(total)) {
      continue;
    }
    Unbox(*state[targets.sel->get_index(i)]).emplace_back(ts[time], total, row);
  }
}

// otel_exponential_histogram(...) is struct_pack with fixed field names, which
// is what StructPackFunction expects to find in the bind data.
duckdb::unique_ptr<duckdb::FunctionData> BindExponentialHistogram(
  duckdb::BindScalarFunctionInput&) {
  return duckdb::make_uniq<duckdb::VariableReturnBindData>(
    ExponentialHistogramType());
}

// ---- Registration ----

duckdb::LogicalType Buckets() {
  return duckdb::LogicalType::LIST(duckdb::LogicalType::DOUBLE);
}

constexpr std::array<duckdb::LogicalTypeId, 3> kTimestamps{
  duckdb::LogicalTypeId::TIMESTAMP_NS,
  duckdb::LogicalTypeId::TIMESTAMP,
  duckdb::LogicalTypeId::TIMESTAMP_TZ,
};

template<typename State, typename Op>
duckdb::AggregateFunction MakeAggregate(
  duckdb::vector<duckdb::LogicalType> arguments, duckdb::LogicalType result,
  duckdb::aggregate_update_t update, duckdb::aggregate_finalize_t finalize) {
  duckdb::AggregateFunction function{
    std::move(arguments),
    std::move(result),
    duckdb::AggregateFunction::StateSize<State>,
    duckdb::AggregateFunction::StateInitialize<State, Op>,
    update,
    duckdb::AggregateFunction::StateCombine<State, Op>,
    finalize,
    duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING,
    nullptr,
    nullptr,
    duckdb::AggregateFunction::StateDestroy<State, Op>,
  };
  function.SetOrderDependent(
    duckdb::AggregateOrderDependent::NOT_ORDER_DEPENDENT);
  return function;
}

template<Counter Kind, int64_t TicksPerSecond>
duckdb::AggregateFunction CounterFunction(const duckdb::LogicalType& ts) {
  using Op = CounterOperation<Kind, TicksPerSecond>;
  using Result = std::conditional_t<CountsEvents(Kind), int64_t, double>;
  return MakeAggregate<SeriesState, Op>(
    {duckdb::LogicalType::DOUBLE, ts},
    CountsEvents(Kind) ? duckdb::LogicalType::BIGINT
                       : duckdb::LogicalType::DOUBLE,
    duckdb::AggregateFunction::BinaryScatterUpdate<SeriesState, double, int64_t,
                                                   Op>,
    duckdb::AggregateFunction::StateFinalize<SeriesState, Result, Op>);
}

template<int64_t TicksPerSecond>
duckdb::AggregateFunction PredictFunction(const duckdb::LogicalType& ts) {
  using Op = PredictOperation<TicksPerSecond>;
  return MakeAggregate<PredictionState, Op>(
    {duckdb::LogicalType::DOUBLE, ts, duckdb::LogicalType::DOUBLE},
    duckdb::LogicalType::DOUBLE, PredictUpdate,
    duckdb::AggregateFunction::StateFinalize<PredictionState, double, Op>);
}

template<Counter Kind>
duckdb::AggregateFunctionSet CounterFunctions(std::string_view name) {
  duckdb::AggregateFunctionSet set{duckdb::Identifier{name}};
  set.AddFunction(CounterFunction<Kind, duckdb::Interval::NANOS_PER_SEC>(
    duckdb::LogicalType::TIMESTAMP_NS));
  set.AddFunction(CounterFunction<Kind, duckdb::Interval::MICROS_PER_SEC>(
    duckdb::LogicalType::TIMESTAMP));
  set.AddFunction(CounterFunction<Kind, duckdb::Interval::MICROS_PER_SEC>(
    duckdb::LogicalType::TIMESTAMP_TZ));
  return set;
}

duckdb::AggregateFunctionSet PredictFunctions() {
  duckdb::AggregateFunctionSet set{duckdb::Identifier{"otel_predict_linear"}};
  set.AddFunction(PredictFunction<duckdb::Interval::NANOS_PER_SEC>(
    duckdb::LogicalType::TIMESTAMP_NS));
  set.AddFunction(PredictFunction<duckdb::Interval::MICROS_PER_SEC>(
    duckdb::LogicalType::TIMESTAMP));
  set.AddFunction(PredictFunction<duckdb::Interval::MICROS_PER_SEC>(
    duckdb::LogicalType::TIMESTAMP_TZ));
  return set;
}

template<Shape Kind>
duckdb::AggregateFunctionSet HistogramFunctions() {
  duckdb::vector<duckdb::LogicalType> arguments(Arity(Kind),
                                                duckdb::LogicalType::DOUBLE);
  auto explicit_arguments = arguments;
  explicit_arguments.push_back(Buckets());
  explicit_arguments.push_back(Buckets());
  arguments.push_back(ExponentialHistogramType());
  duckdb::AggregateFunctionSet set{duckdb::Identifier{Name(Kind)}};
  set.AddFunction(MakeAggregate<HistogramState, HistogramOperation<Kind>>(
    std::move(explicit_arguments), duckdb::LogicalType::DOUBLE,
    HistogramUpdate<Kind>,
    duckdb::AggregateFunction::StateFinalize<HistogramState, double,
                                             HistogramOperation<Kind>>));
  set.AddFunction(MakeAggregate<ExponentialState, ExponentialOperation<Kind>>(
    std::move(arguments), duckdb::LogicalType::DOUBLE, ExponentialUpdate<Kind>,
    duckdb::AggregateFunction::StateFinalize<ExponentialState, double,
                                             ExponentialOperation<Kind>>));
  return set;
}

}  // namespace

void RegisterMetricsFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  auto increase = CounterFunctions<Counter::Increase>("otel_increase");
  for (const auto id : kTimestamps) {
    const duckdb::LogicalType ts{id};
    increase.AddFunction(MakeAggregate<BucketsState, BucketsOperation>(
      {Buckets(), ts}, Buckets(), BucketsUpdate,
      duckdb::AggregateFunction::StateFinalize<
        BucketsState, duckdb::list_entry_t, BucketsOperation>));
    increase.AddFunction(
      MakeAggregate<ExponentialSeriesState, ExponentialIncreaseOperation>(
        {ExponentialHistogramType(), ts}, ExponentialHistogramType(),
        ExponentialIncreaseUpdate,
        duckdb::AggregateFunction::StateVoidFinalize<
          ExponentialSeriesState, ExponentialIncreaseOperation>));
  }
  loader.RegisterFunction(increase);
  loader.RegisterFunction(CounterFunctions<Counter::Rate>("otel_rate"));
  loader.RegisterFunction(CounterFunctions<Counter::InstantRate>("otel_irate"));
  loader.RegisterFunction(CounterFunctions<Counter::Delta>("otel_delta"));
  loader.RegisterFunction(
    CounterFunctions<Counter::InstantDelta>("otel_idelta"));
  loader.RegisterFunction(CounterFunctions<Counter::Deriv>("otel_deriv"));
  loader.RegisterFunction(CounterFunctions<Counter::Changes>("otel_changes"));
  loader.RegisterFunction(CounterFunctions<Counter::Resets>("otel_resets"));

  loader.RegisterFunction(PredictFunctions());

  loader.RegisterFunction(HistogramFunctions<Shape::Quantile>());
  loader.RegisterFunction(HistogramFunctions<Shape::Fraction>());
  loader.RegisterFunction(HistogramFunctions<Shape::StdDev>());
  loader.RegisterFunction(HistogramFunctions<Shape::StdVar>());

  duckdb::ScalarFunction pack{
    "otel_exponential_histogram",
    {
      duckdb::LogicalType::INTEGER,
      duckdb::LogicalType::DOUBLE,
      duckdb::LogicalType::INTEGER,
      Buckets(),
      duckdb::LogicalType::INTEGER,
      Buckets(),
    },
    ExponentialHistogramType(),
    duckdb::StructPackFunction,
    BindExponentialHistogram,
  };
  pack.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  loader.RegisterFunction(pack);
}

}  // namespace sdb::connector
