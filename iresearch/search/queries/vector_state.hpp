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

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs {

struct TermReader;
class ColReader;
class ColumnReader;
class QuantizerCodebook;

struct VectorState {
  const TermReader* reader = nullptr;
  const ColumnReader* vector_column = nullptr;
  const ColReader* col_reader = nullptr;
  TrackedVector<PostingMeta> cookies;
  std::unique_ptr<IndexInput> payload;
  uint64_t estimation = 0;

  VectorQuantization quant = VectorQuantization::None;
  uint32_t d = 0;
  uint32_t top_k = 0;
  float rescore_width = 0.f;
  TrackedVector<uint64_t> pay_starts;
  TrackedVector<uint32_t> pay_lanes;
  TrackedVector<uint32_t> cluster_counts;

  std::shared_ptr<const QuantizerCodebook> codebook;
  TrackedVector<float> cluster_centroids;
};

}  // namespace irs
