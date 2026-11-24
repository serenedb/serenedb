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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/main/client_context.hpp>

#include "catalog/log.h"
#include "search/table.h"
#include "search/table_replay.h"

namespace sdb::search {

inline constexpr uint32_t kLogFrameBytes = uint32_t{256} << 10;

void PackRows(LoggedRows& rows, uint32_t frame_bytes,
              irs::ParallelFor parallel);
void PackPatches(std::span<UnitPatch> patches, uint32_t frame_bytes,
                 irs::ParallelFor parallel);
void PackDelta(TableDelta& delta, uint32_t frame_bytes,
               irs::ParallelFor parallel);

void WriteTableDelta(duckdb::BinarySerializer& object,
                     catalog::RecordBuffer& out, const TableDelta& delta);

void ReplayTableDelta(duckdb::ClientContext& context, TableReplay* replay,
                      duckdb::Catalog* catalog,
                      duckdb::BinaryDeserializer& object,
                      duckdb::MemoryStream& stream);

}  // namespace sdb::search
