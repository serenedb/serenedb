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

#pragma once

#include <memory>

#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct TermReader;
class ColumnReader;
class QuantizerCodebook;

struct VectorState {
  explicit VectorState(IResourceManager& memory) noexcept
    : cookies{{memory}},
      pay_starts{{memory}},
      cluster_counts{{memory}},
      cluster_centroids{{memory}} {}

  const TermReader* reader = nullptr;
  const ColumnReader* vector_column = nullptr;
  ManagedVector<SeekCookie::ptr> cookies;
  CostAttr::Type estimation = 0;

  VectorQuantization quant = VectorQuantization::None;
  uint32_t d = 0;
  ManagedVector<uint64_t> pay_starts;
  ManagedVector<uint32_t> cluster_counts;

  std::shared_ptr<const QuantizerCodebook> codebook;
  ManagedVector<float> cluster_centroids;
};

}  // namespace irs
