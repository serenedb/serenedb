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
#include <iresearch/utils/string.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

class ClientContext;
class MultiFileList;

}  // namespace duckdb
namespace sdb::connector {

struct SourceFile {
  uint64_t id = 0;
  std::string_view path;
  std::string_view version;
};

std::string SourceFileTerm(uint64_t id, std::string_view path,
                           std::string_view version);

SourceFile ParseSourceFileTerm(irs::bytes_view term);

uint64_t SourceFileTermId(std::string_view term);

struct SourceListing {
  duckdb::vector<duckdb::OpenFileInfo> files;
  std::vector<std::string> versions;
  uint64_t digest = 0;
};

SourceListing ListSource(duckdb::ClientContext& context,
                         const duckdb::MultiFileList& list, bool versioned);

std::vector<std::string> SourceFileTerms(const SourceListing& listing);

struct HeldFile {
  std::vector<uint64_t> ids;
  std::string version;
};

struct HeldFiles {
  irs::containers::NodeHashMap<std::string, HeldFile> by_path;
  uint64_t next_id = 0;
};

HeldFiles CollectHeldFiles(const irs::IndexReader& reader);

std::optional<std::string> FindSourceFilePath(const irs::IndexReader& reader,
                                              uint64_t id);

}  // namespace sdb::connector
