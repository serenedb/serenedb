////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include <functional>

#include "iresearch/utils/compression.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

enum class HNSWMetric : uint8_t {
  L2Sqr = 0,
  NegativeIP,
  Cosine,
  L1,
};

struct HNSWInfo {
  doc_id_t max_doc = 0;

  // dimensionality of the data
  int d = 0;

  // HNSW M parameter
  int m = 32;

  // HNSW metric
  HNSWMetric metric = HNSWMetric::L2Sqr;

  // expansion factor at construction time
  int ef_construction = 40;
};

}  // namespace irs
