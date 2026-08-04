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
#include <memory>
#include <vector>

#include "iresearch/formats/ivf/quantizer_reader.hpp"

namespace irs {

// The quantized vector path's cluster readers, owned by the worker that runs
// the row group. One reader per (segment, cluster), built on the cluster's
// first row group from its whole run list and reused for the rest: the decode
// and, for multi-bit RaBitQ, the refine-candidate threshold are cluster-scoped,
// so one reader per (cluster, row group) would redo the whole cluster once per
// row group. The reader is a decode cursor, so it cannot live in the prepared
// query -- several workers share that query while they run different row
// groups of one segment. `owner` keys the cache to the query state it holds
// readers for, so a worker that moves to another segment refills it.
class ClusterReaderCache {
 public:
  std::unique_ptr<QuantizerReader>& Slot(const void* owner, size_t cluster,
                                         size_t clusters) {
    if (_owner != owner || _readers.size() != clusters) {
      _readers.clear();
      _readers.resize(clusters);
      _owner = owner;
    }
    return _readers[cluster];
  }

 private:
  const void* _owner = nullptr;
  std::vector<std::unique_ptr<QuantizerReader>> _readers;
};

}  // namespace irs
