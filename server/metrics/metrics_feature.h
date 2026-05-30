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

#include <absl/synchronization/mutex.h>

#include <map>
#include <memory>

#include "metrics/builder.h"
#include "metrics/metric.h"
#include "metrics/metric_key.h"

namespace sdb::metrics {

// Thin process-wide registry. No Prometheus / VPack rendering, no
// per-server-shortname label, no batch / dynamic metric surface. A proper
// Prometheus exposition story is a follow-up. The registry is a singleton
// so metric definitions outside the AppServer lifecycle work uniformly.
class MetricsFeature {
 public:
  static MetricsFeature& instance();

  template<typename MetricBuilder>
  auto add(MetricBuilder&& builder) -> typename MetricBuilder::MetricT& {
    return static_cast<typename MetricBuilder::MetricT&>(*doAdd(builder));
  }

  Metric* get(const MetricKeyView& key) const;

 private:
  MetricsFeature() = default;
  std::shared_ptr<Metric> doAdd(Builder& builder);

  mutable absl::Mutex _mutex;
  std::map<MetricKeyView, std::shared_ptr<Metric>> _registry;
};

inline MetricsFeature& GetMetrics() { return MetricsFeature::instance(); }

template<typename F>
auto& AddMetric(F&& builder) {
  return GetMetrics().add(std::forward<F>(builder));
}

}  // namespace sdb::metrics
