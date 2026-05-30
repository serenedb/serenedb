////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <map>

#include "metrics/builder.h"
#include "metrics/metric.h"
#include "metrics/metric_key.h"
#include "rest_server/serened.h"

namespace sdb::metrics {

// Thin registry. No more Prometheus / VPack rendering, no per-server-shortname
// label, no batch / dynamic metric surface — that machinery served ArangoDB's
// /_admin/metrics endpoint, which is gone. Counters/Gauges/Histograms still
// keep state via this registry so AddMetric() returns a stable reference; a
// proper Prometheus exposition story is a follow-up.
class MetricsFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Metrics"; }

  explicit MetricsFeature(Server& server);

  template<typename MetricBuilder>
  auto add(MetricBuilder&& builder) -> typename MetricBuilder::MetricT& {
    return static_cast<typename MetricBuilder::MetricT&>(*doAdd(builder));
  }

  Metric* get(const MetricKeyView& key) const;

 private:
  std::shared_ptr<Metric> doAdd(Builder& builder);

  mutable absl::Mutex _mutex;
  std::map<MetricKeyView, std::shared_ptr<Metric>> _registry;
};

MetricsFeature& GetMetrics();

template<typename F>
auto& AddMetric(F&& builder) {
  return GetMetrics().add(std::forward<F>(builder));
}

}  // namespace sdb::metrics
