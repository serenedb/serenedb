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

#include "metrics/metrics_feature.h"

#include <absl/strings/str_cat.h>

#include <mutex>

#include "basics/debugging.h"
#include "basics/exceptions.h"

namespace sdb::metrics {

MetricsFeature& MetricsFeature::instance() {
  static MetricsFeature gInstance;
  return gInstance;
}

std::shared_ptr<Metric> MetricsFeature::doAdd(Builder& builder) {
  auto metric = builder.build();
  SDB_ASSERT(metric != nullptr);
  MetricKeyView key{metric->name(), metric->labels()};
  std::lock_guard lock{_mutex};
  auto [it, inserted] = _registry.try_emplace(key, metric);
  if (!inserted) {
    SDB_THROW(sdb::ERROR_INTERNAL,
              absl::StrCat(builder.type(), " ", metric->name(), ":",
                           metric->labels(), " already exists"));
  }
  return it->second;
}

Metric* MetricsFeature::get(const MetricKeyView& key) const {
  absl::ReaderMutexLock lock{&_mutex};
  if (auto it = _registry.find(key); it != _registry.end()) {
    return it->second.get();
  }
  return nullptr;
}

}  // namespace sdb::metrics
