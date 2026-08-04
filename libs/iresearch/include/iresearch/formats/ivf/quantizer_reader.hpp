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

#include <cstddef>
#include <cstdint>

#include "iresearch/types.hpp"

namespace irs {

// Its own header because the search layer owns these per worker: a reader is a
// decode cursor, so ClusterReaderCache holds them, and that cache reaches the
// search layer's core filter header. Nothing else of the quantizer -- the
// writer, the codebook, the stats blob and the block reader they need -- has
// any business being compiled there.
class QuantizerReader {
 public:
  virtual ~QuantizerReader() = default;

  // The cluster is the unit every query-time decision is taken over, so it is
  // started once over its whole lane range however many runs the postings were
  // cut into: multi-bit RaBitQ picks its refine candidates against a threshold
  // taken from the cluster's scores, and that selection has to be the same set
  // of documents under any cut. `ComputeBlock`'s offset is cluster-wide -- a
  // per-run caller adds the number of documents in the runs before its own.
  virtual void StartCluster(uint64_t first_lane, size_t num_docs,
                            const float* centroid) = 0;

  virtual void ComputeBlock(size_t offset, size_t length, score_t* out) = 0;
};

}  // namespace irs
