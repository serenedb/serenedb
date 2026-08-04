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

#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct TermReader;
class ColumnReader;

// One row group's slice of a cluster's payload. Row-group partitioned postings
// cut a cluster into one run per row group it touches; an unpartitioned
// dictionary gives a cluster exactly one run at `rg == 0`. The cluster's lanes
// are contiguous, so `doc_offset` -- how many documents the runs before this
// one hold -- is also how many lanes past the cluster it starts.
struct ClusterRun {
  uint32_t rg = 0;
  uint32_t docs_count = 0;
  uint32_t doc_offset = 0;
};

struct VectorState {
  // A probed cluster's whole payload: the lane its first document sits at and
  // how many lanes it owns from there.
  struct ClusterPay {
    uint64_t first_lane;
    uint32_t docs_count;
  };

  explicit VectorState(IResourceManager& memory) noexcept
    : cookies{{memory}},
      pay_runs{{memory}},
      pay_run_begin{{memory}},
      clusters{{memory}},
      cluster_centroids{{memory}} {}

  const TermReader* reader = nullptr;
  const ColumnReader* vector_column = nullptr;
  ManagedVector<TermCookie> cookies;
  CostAttr::Type estimation = 0;

  VectorQuantization quant = VectorQuantization::None;
  uint32_t d = 0;
  // Where the field's code stream starts in `.pay`; lanes count from there.
  uint64_t pay_base = 0;
  // Cluster `c` owns `pay_runs[pay_run_begin[c] .. pay_run_begin[c + 1])`, in
  // cluster document order; `clusters[c]` is the whole cluster.
  ManagedVector<ClusterRun> pay_runs;
  ManagedVector<uint32_t> pay_run_begin;
  ManagedVector<ClusterPay> clusters;

  std::shared_ptr<const QuantizerCodebook> codebook;
  ManagedVector<float> cluster_centroids;
};

}  // namespace irs
