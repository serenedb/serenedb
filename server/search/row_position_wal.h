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

#include <iresearch/index/doc_removal.hpp>
#include <memory>
#include <vector>

namespace duckdb {

class WALEntryBuffer;

}  // namespace duckdb
namespace sdb::search {

void WriteRemoval(duckdb::WALEntryBuffer& entries,
                  const irs::DocRemoval& removal);
void WriteDocMaps(duckdb::WALEntryBuffer& entries,
                  const std::vector<irs::DocMap>& maps);

std::shared_ptr<const irs::DocRemoval> ReplayRemoval(
  irs::DocRemoval removal, const irs::DirectoryReader& recovered,
  const irs::containers::FlatHashMap<uint64_t, std::vector<irs::DocMap>>&
    doc_maps);

}  // namespace sdb::search
