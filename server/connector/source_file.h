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
#include <duckdb/common/open_file_info.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <string>
#include <vector>

#include "search/source_files.h"

namespace duckdb {

class ClientContext;
class MultiFileList;

}  // namespace duckdb
namespace sdb::connector {

struct SourceListing {
  duckdb::vector<duckdb::OpenFileInfo> files;
  std::vector<std::string> versions;
  uint64_t digest = 0;
};

SourceListing ListSource(duckdb::ClientContext& context,
                         const duckdb::MultiFileList& list, bool versioned);

std::vector<search::SourceFile> ListedFiles(const SourceListing& listing,
                                            uint64_t first_id);

struct HeldFile {
  std::vector<uint64_t> ids;
  std::string version;
};

struct HeldFiles {
  irs::containers::NodeHashMap<std::string, HeldFile> by_path;
};

HeldFiles CollectHeldFiles(const irs::IndexReader& reader,
                           const search::SourceFiles& files);

HeldFiles KnownFiles(const search::SourceFiles& files);

}  // namespace sdb::connector
