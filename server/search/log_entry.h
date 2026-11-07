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
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <span>
#include <string>
#include <string_view>

#include "catalog/entry/search_table.h"
#include "search/table.h"

namespace sdb::search {

std::string LogTruncate();
std::string LogAddSegments(std::span<const OwnSegment> segments);
std::string LogHotRows(uint64_t segment, irs::doc_id_t first,
                       duckdb::DataChunk& chunk);
std::string LogRemoval(const irs::DocRemoval& removal);
std::string LogCooled(const ReplacedSegment& segment);
std::string LogReplace(std::span<const uint64_t> replaced,
                       std::span<const ReplacedSegment> segments,
                       std::span<const irs::DocMap> maps);
std::string LogState(std::span<const StateUnit> units);

void ReplayLogEntry(duckdb::ClientContext& context,
                    catalog::SearchTableEntry& table, std::string& entry,
                    Lsn lsn);

}  // namespace sdb::search
