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

#include "metrics/metric.h"

namespace sdb::metrics {

Metric::Metric(std::string_view name, std::string_view labels)
  : _name{name}, _labels{labels} {}

std::string_view Metric::name() const noexcept { return _name; }
std::string_view Metric::labels() const noexcept { return _labels; }

Metric::~Metric() = default;

}  // namespace sdb::metrics
