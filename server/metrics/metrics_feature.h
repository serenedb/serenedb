////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <map>
#include <shared_mutex>

#include "app/options/program_options.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "metrics/batch.h"
#include "metrics/builder.h"
#include "metrics/collect_mode.h"
#include "metrics/ibatch.h"
#include "metrics/metric.h"
#include "metrics/metric_key.h"
#include "metrics/metrics_parts.h"
#include "rest_server/serened.h"

namespace sdb::metrics {

class MetricsFeature final : public SerenedFeature {
 public:
  enum class UsageTrackingMode {
    // no tracking
    Disabled,
    // tracking per shard (one-dimensional)
    EnabledPerShard,
    // tracking per shard and per user (two-dimensional)
    EnabledPerShardPerUser,
  };

  static constexpr std::string_view name() noexcept { return "Metrics"; }

  explicit MetricsFeature(Server& server);

  bool exportAPI() const noexcept;
  bool ensureWhitespace() const noexcept;
  UsageTrackingMode usageTrackingMode() const noexcept;

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;

  // tries to add metric. throws if such metric already exists
  template<typename MetricBuilder>
  auto add(MetricBuilder&& builder) -> typename MetricBuilder::MetricT& {
    return static_cast<typename MetricBuilder::MetricT&>(*doAdd(builder));
  }

  template<typename MetricBuilder>
  auto addShared(MetricBuilder&& builder)  // TODO(mbkkt) Remove this method
    -> std::shared_ptr<typename MetricBuilder::MetricT> {
    return std::static_pointer_cast<typename MetricBuilder::MetricT>(
      doAdd(builder));
  }

  // tries to add dynamic metric. does not fail if such metric already exists
  template<typename MetricBuilder>
  auto addDynamic(MetricBuilder&& builder) -> typename MetricBuilder::MetricT& {
    return static_cast<typename MetricBuilder::MetricT&>(
      *doAddDynamic(builder));
  }

  Metric* get(const MetricKeyView& key) const;
  bool remove(const Builder& builder);

  void toPrometheus(std::string& result, MetricsParts metrics_parts,
                    CollectMode mode) const;

  //////////////////////////////////////////////////////////////////////////////
  /// That used for collect some metrics
  /// to array for ClusterMetricsFeature
  //////////////////////////////////////////////////////////////////////////////
  void toVPack(vpack::Builder& builder, MetricsParts metrics_parts) const;

  template<typename MetricType>
  MetricType& batchAdd(std::string_view name, std::string_view labels) {
    std::unique_lock lock{_mutex};
    auto& ibatch = _batch[name];
    if (!ibatch) {
      ibatch = std::make_unique<metrics::Batch<MetricType>>();
    }
    return basics::downCast<metrics::Batch<MetricType>>(*ibatch).add(labels);
  }
  std::pair<std::shared_lock<absl::Mutex>, metrics::IBatch*> getBatch(
    std::string_view name) const;
  void batchRemove(std::string_view name, std::string_view labels);

 private:
  std::shared_ptr<Metric> doAdd(Builder& builder);
  std::shared_ptr<Metric> doAddDynamic(Builder& builder);
  std::shared_lock<absl::Mutex> initGlobalLabels() const;

  mutable absl::Mutex _mutex;

  // TODO(mbkkt) abseil btree map? or hashmap<name, hashmap<labels, Metric>>?
  std::map<MetricKeyView, std::shared_ptr<Metric>> _registry;

  containers::FlatHashMap<std::string_view, std::unique_ptr<IBatch>> _batch;

  mutable std::string _globals;
  mutable bool _has_shortname = false;
  mutable bool _has_role = false;

  bool _export;
  bool _export_read_write_metrics;
  // ensure that there is whitespace before the reported value, regardless
  // of whether it is preceeded by labels or not.
  bool _ensure_whitespace;

  std::string _usage_tracking_mode_string;
  UsageTrackingMode _usage_tracking_mode;
};

MetricsFeature& GetMetrics();

template<typename F>
auto& AddMetric(F&& builder) {
  return GetMetrics().add(std::forward<F>(builder));
}

}  // namespace sdb::metrics
