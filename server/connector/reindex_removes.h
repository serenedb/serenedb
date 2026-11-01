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

#include <cstdint>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "connector/file_manifest.h"

namespace sdb::connector {

std::shared_ptr<const irs::DocRemoval> ResolveRemoves(
  const irs::DirectoryReader& snapshot, std::vector<uint64_t> deleted_files,
  std::vector<IcebergObserve::DeleteMask> masks,
  std::vector<std::pair<uint64_t, int64_t>> eq_rows);

}  // namespace sdb::connector
