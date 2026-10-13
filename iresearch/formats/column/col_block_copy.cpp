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

#include "iresearch/formats/column/col_block_copy.hpp"

#include <absl/algorithm/container.h>

#include <duckdb/main/config.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/col_block_sink.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"

namespace irs {
namespace {

void CopyBlocks(ReadContext& ctx, std::span<const ColumnBlockMeta> blocks,
                ColBlockSink& sink, std::vector<ColumnBlockMeta>& out) {
  std::string bytes;
  for (const auto& block : blocks) {
    auto& copy = out.emplace_back(ColumnBlockMeta{block.statistics.Copy()});
    copy.tuple_count = block.tuple_count;
    copy.byte_size = block.byte_size;
    copy.codec = block.codec;
    if (block.byte_size == 0) {
      continue;
    }
    bytes.resize(block.byte_size);
    ctx.Read(block.file_offset,
             reinterpret_cast<duckdb::data_ptr_t>(bytes.data()),
             block.byte_size);
    const std::string_view view{bytes};
    copy.file_offset = sink.AppendAligned({&view, 1});
  }
}

}  // namespace

bool BlocksRelocatable(const ColumnReader& column) {
  if (column.HasDictionaries()) {
    return false;
  }
  if (column.Type().InternalType() != duckdb::PhysicalType::VARCHAR) {
    return true;
  }
  return absl::c_none_of(column.DataBlocks(), [](const ColumnBlockMeta& block) {
    return block.codec->type ==
           duckdb::CompressionType::COMPRESSION_UNCOMPRESSED;
  });
}

ColumnMeta CopyColumnBlocks(const ColReader& reader, const ColumnReader& column,
                            bool validity, ColBlockSink& sink) {
  ColumnMeta meta;
  meta.id = column.Id();
  meta.type = column.Type();
  ReadContext ctx{reader};
  CopyBlocks(ctx, column.DataBlocks(), sink, meta.data);
  if (!validity) {
    return meta;
  }
  if (const auto* stored = column.Validity()) {
    CopyBlocks(ctx, stored->DataBlocks(), sink, meta.validity);
    return meta;
  }
  const duckdb::LogicalType type{duckdb::LogicalTypeId::VALIDITY};
  const auto* empty =
    duckdb::DBConfig::GetConfig(reader.Database())
      .TryGetCompressionFunction(duckdb::CompressionType::COMPRESSION_EMPTY,
                                 type.InternalType())
      .get();
  for (const auto& block : meta.data) {
    auto& rows = meta.validity.emplace_back(
      ColumnBlockMeta{duckdb::BaseStatistics::CreateEmpty(type)});
    rows.tuple_count = block.tuple_count;
    rows.codec = empty;
  }
  return meta;
}

}  // namespace irs
