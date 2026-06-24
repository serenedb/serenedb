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

#include "sdb_metrics.h"

#include "basics/metrics.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&SdbMetrics::metric),
  GetIndex(&SdbMetrics::value),
  GetIndex(&SdbMetrics::description),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<SdbMetrics>::GetTableData() {
  std::vector<SdbMetrics> values;
  values.reserve(metrics::kGaugeCount);
  for (size_t i = 0; i < metrics::kGaugeCount; ++i) {
    const auto gauge = static_cast<metrics::Gauge>(i);
    values.push_back({
      .metric = std::string_view{metrics::Name(gauge)},
      .value = static_cast<uint64_t>(metrics::Get(gauge)),
      .description = std::string_view{metrics::Description(gauge)},
    });
  }

  auto result = CreateColumns<SdbMetrics>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
