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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector_operations/aggregate_executor.hpp>
#include <duckdb/function/aggregate_function.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <numeric>
#include <optional>
#include <span>
#include <vector>

namespace sdb::connector {
namespace {

constexpr int64_t kNanosPerSecond = 1'000'000'000;
constexpr int64_t kMicrosPerSecond = 1'000'000;

struct Sample {
  int64_t ts;
  double value;
};

struct BucketSample {
  int64_t ts;
  double total = 0;
  std::vector<double> counts;
};

struct Histogram {
  double quantile = 0;
  std::vector<double> bounds;
  std::vector<double> counts;
};

template<typename T>
struct Boxed {
  T* data;
};

using SeriesState = Boxed<std::vector<Sample>>;
using BucketsState = Boxed<std::vector<BucketSample>>;
using HistogramState = Boxed<Histogram>;

template<typename T>
T& Unbox(Boxed<T>& state) {
  if (!state.data) {
    state.data = new T;
  }
  return *state.data;
}

template<typename T>
void Append(const Boxed<std::vector<T>>& source,
            Boxed<std::vector<T>>& target) {
  if (!source.data) {
    return;
  }
  auto& samples = Unbox(target);
  samples.insert(samples.end(), source.data->begin(), source.data->end());
}

template<typename T, typename Rank>
void Normalize(std::vector<T>& samples, Rank rank) {
  absl::c_sort(samples, [&](const T& lhs, const T& rhs) {
    return lhs.ts != rhs.ts ? lhs.ts < rhs.ts : rank(lhs) > rank(rhs);
  });
  const auto [first, last] = std::ranges::unique(samples, {}, &T::ts);
  samples.erase(first, last);
}

double Increase(double previous, double current) {
  return current >= previous ? current - previous : current;
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

enum class Counter : uint8_t {
  Increase,
  Rate,
  InstantRate,
};

template<Counter Kind, int64_t TicksPerSecond>
struct CounterOperation : BoxedOperation {
  template<typename A, typename B, typename State, typename Op>
  static void Operation(State& state, const A& value, const B& ts,
                        duckdb::AggregateBinaryInput&) {
    if (!std::isnan(value)) {
      Unbox(state).push_back({.ts = ts, .value = value});
    }
  }

  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData&) {
    Append(source, target);
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
    if (samples.size() < 2) {
      finalize.ReturnNull();
      return;
    }
    if constexpr (Kind == Counter::InstantRate) {
      const auto& previous = samples[samples.size() - 2];
      const auto& current = samples.back();
      target = Increase(previous.value, current.value) /
               Seconds(current.ts - previous.ts);
    } else {
      double increase = 0;
      for (size_t i = 1; i < samples.size(); ++i) {
        increase += Increase(samples[i - 1].value, samples[i].value);
      }
      if constexpr (Kind == Counter::Rate) {
        increase /= Seconds(samples.back().ts - samples.front().ts);
      }
      target = increase;
    }
  }

  static double Seconds(int64_t ticks) {
    return static_cast<double>(ticks) / TicksPerSecond;
  }
};

struct BucketsOperation : BoxedOperation {
  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData&) {
    Append(source, target);
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
      bool reset = previous.size() != current.size();
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
    if (!lists.validity.RowIsValid(list) || !times.validity.RowIsValid(time)) {
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
    Unbox(*state[targets.sel->get_index(i)]).push_back(std::move(sample));
  }
}

[[noreturn]] void ThrowMixedHistograms() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("histogram_quantile(): all rows of a group must "
                          "have the same quantile and explicit_bounds"));
}

bool SameQuantile(double lhs, double rhs) {
  return lhs == rhs || (std::isnan(lhs) && std::isnan(rhs));
}

void Merge(const Histogram& source, Histogram& target) {
  if (!SameQuantile(source.quantile, target.quantile) ||
      source.bounds != target.bounds) {
    ThrowMixedHistograms();
  }
  for (size_t i = 0; i != source.counts.size(); ++i) {
    target.counts[i] += source.counts[i];
  }
}

std::optional<double> BucketQuantile(double quantile,
                                     std::span<const double> bounds,
                                     std::span<const double> counts) {
  if (std::isnan(quantile)) {
    return quantile;
  }
  if (quantile < 0) {
    return -std::numeric_limits<double>::infinity();
  }
  if (quantile > 1) {
    return std::numeric_limits<double>::infinity();
  }
  const double total = std::accumulate(counts.begin(), counts.end(), 0.0);
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

struct QuantileOperation : BoxedOperation {
  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData&) {
    if (!source.data) {
      return;
    }
    if (!target.data) {
      target.data = new Histogram{*source.data};
      return;
    }
    Merge(*source.data, *target.data);
  }

  template<typename Result, typename State>
  static void Finalize(State& state, Result& target,
                       duckdb::AggregateFinalizeData& finalize) {
    if (!state.data) {
      finalize.ReturnNull();
      return;
    }
    const auto& histogram = *state.data;
    const auto quantile =
      BucketQuantile(histogram.quantile, histogram.bounds, histogram.counts);
    if (!quantile) {
      finalize.ReturnNull();
      return;
    }
    target = *quantile;
  }
};

void QuantileUpdate(duckdb::Vector inputs[], duckdb::AggregateInputData&,
                    duckdb::idx_t, duckdb::Vector& states,
                    duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat quantiles;
  duckdb::UnifiedVectorFormat count_lists;
  duckdb::UnifiedVectorFormat bound_lists;
  duckdb::UnifiedVectorFormat count_values;
  duckdb::UnifiedVectorFormat bound_values;
  duckdb::UnifiedVectorFormat targets;
  inputs[0].ToUnifiedFormat(quantiles);
  inputs[1].ToUnifiedFormat(count_lists);
  inputs[2].ToUnifiedFormat(bound_lists);
  duckdb::ListVector::GetChild(inputs[1]).ToUnifiedFormat(count_values);
  duckdb::ListVector::GetChild(inputs[2]).ToUnifiedFormat(bound_values);
  states.ToUnifiedFormat(targets);
  const auto* quantile =
    duckdb::UnifiedVectorFormat::GetData<double>(quantiles);
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
    return bound_values.validity.RowIsValid(value)
             ? bounds[value]
             : std::numeric_limits<double>::quiet_NaN();
  };
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto q = quantiles.sel->get_index(i);
    const auto c = count_lists.sel->get_index(i);
    const auto b = bound_lists.sel->get_index(i);
    if (!quantiles.validity.RowIsValid(q) ||
        !count_lists.validity.RowIsValid(c) ||
        !bound_lists.validity.RowIsValid(b) || count_entries[c].length == 0) {
      continue;
    }
    const auto& count_entry = count_entries[c];
    const auto& bound_entry = bound_entries[b];
    if (count_entry.length != bound_entry.length + 1) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("histogram_quantile(): bucket_counts must have one element "
                "more than explicit_bounds, got ",
                count_entry.length, " and ", bound_entry.length));
    }
    auto& histogram = Unbox(*state[targets.sel->get_index(i)]);
    if (histogram.counts.empty()) {
      histogram.quantile = quantile[q];
      histogram.bounds.reserve(bound_entry.length);
      for (duckdb::idx_t j = 0; j != bound_entry.length; ++j) {
        histogram.bounds.push_back(bound(bound_entry, j));
      }
      histogram.counts.assign(count_entry.length, 0);
    } else {
      bool same = SameQuantile(quantile[q], histogram.quantile) &&
                  histogram.bounds.size() == bound_entry.length;
      for (duckdb::idx_t j = 0; same && j != bound_entry.length; ++j) {
        same = histogram.bounds[j] == bound(bound_entry, j);
      }
      if (!same) {
        ThrowMixedHistograms();
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

duckdb::LogicalType Buckets() {
  return duckdb::LogicalType::LIST(duckdb::LogicalType::DOUBLE);
}

template<Counter Kind, int64_t TicksPerSecond>
duckdb::AggregateFunction CounterFunction(const duckdb::LogicalType& ts) {
  using Op = CounterOperation<Kind, TicksPerSecond>;
  duckdb::AggregateFunction function{
    {duckdb::LogicalType::DOUBLE, ts},
    duckdb::LogicalType::DOUBLE,
    duckdb::AggregateFunction::StateSize<SeriesState>,
    duckdb::AggregateFunction::StateInitialize<SeriesState, Op>,
    duckdb::AggregateFunction::BinaryScatterUpdate<SeriesState, double, int64_t,
                                                   Op>,
    duckdb::AggregateFunction::StateCombine<SeriesState, Op>,
    duckdb::AggregateFunction::StateFinalize<SeriesState, double, Op>,
    duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING,
    nullptr,
    nullptr,
    duckdb::AggregateFunction::StateDestroy<SeriesState, Op>};
  function.SetOrderDependent(
    duckdb::AggregateOrderDependent::NOT_ORDER_DEPENDENT);
  return function;
}

duckdb::AggregateFunction BucketsFunction(const duckdb::LogicalType& ts) {
  duckdb::AggregateFunction function{
    {Buckets(), ts},
    Buckets(),
    duckdb::AggregateFunction::StateSize<BucketsState>,
    duckdb::AggregateFunction::StateInitialize<BucketsState, BucketsOperation>,
    BucketsUpdate,
    duckdb::AggregateFunction::StateCombine<BucketsState, BucketsOperation>,
    duckdb::AggregateFunction::StateFinalize<BucketsState, duckdb::list_entry_t,
                                             BucketsOperation>,
    duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING,
    nullptr,
    nullptr,
    duckdb::AggregateFunction::StateDestroy<BucketsState, BucketsOperation>};
  function.SetOrderDependent(
    duckdb::AggregateOrderDependent::NOT_ORDER_DEPENDENT);
  return function;
}

template<Counter Kind>
duckdb::AggregateFunctionSet CounterFunctions(std::string_view name) {
  duckdb::AggregateFunctionSet set{duckdb::Identifier{name}};
  set.AddFunction(
    CounterFunction<Kind, kNanosPerSecond>(duckdb::LogicalType::TIMESTAMP_NS));
  set.AddFunction(
    CounterFunction<Kind, kMicrosPerSecond>(duckdb::LogicalType::TIMESTAMP));
  set.AddFunction(
    CounterFunction<Kind, kMicrosPerSecond>(duckdb::LogicalType::TIMESTAMP_TZ));
  return set;
}

}  // namespace

void RegisterMetricsFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  auto increase = CounterFunctions<Counter::Increase>("increase");
  for (const auto& ts :
       {duckdb::LogicalType::TIMESTAMP_NS, duckdb::LogicalType::TIMESTAMP,
        duckdb::LogicalType::TIMESTAMP_TZ}) {
    increase.AddFunction(BucketsFunction(ts));
  }
  loader.RegisterFunction(increase);
  loader.RegisterFunction(CounterFunctions<Counter::Rate>("rate"));
  loader.RegisterFunction(CounterFunctions<Counter::InstantRate>("irate"));

  duckdb::AggregateFunction quantile{
    {duckdb::LogicalType::DOUBLE, Buckets(), Buckets()},
    duckdb::LogicalType::DOUBLE,
    duckdb::AggregateFunction::StateSize<HistogramState>,
    duckdb::AggregateFunction::StateInitialize<HistogramState,
                                               QuantileOperation>,
    QuantileUpdate,
    duckdb::AggregateFunction::StateCombine<HistogramState, QuantileOperation>,
    duckdb::AggregateFunction::StateFinalize<HistogramState, double,
                                             QuantileOperation>,
    duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING,
    nullptr,
    nullptr,
    duckdb::AggregateFunction::StateDestroy<HistogramState, QuantileOperation>};
  quantile.SetOrderDependent(
    duckdb::AggregateOrderDependent::NOT_ORDER_DEPENDENT);
  duckdb::AggregateFunctionSet quantiles{
    duckdb::Identifier{"histogram_quantile"}};
  quantiles.AddFunction(quantile);
  loader.RegisterFunction(quantiles);
}

}  // namespace sdb::connector
