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

#include "iresearch/search/cluster_reader_cache.hpp"

namespace irs {

// One executing worker of a scan: the scratch it owns and, because there is
// exactly one per worker, the identity anything that has to cache per worker
// keys on. A prepared query is shared by every worker running a row group of
// its segment, so nothing that is a cursor rather than a plan may live in it.
struct WorkerContext {
  ClusterReaderCache cluster_readers;
};

}  // namespace irs
