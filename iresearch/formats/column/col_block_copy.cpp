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

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/main/config.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/col_block_sink.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {
namespace {

void PatchDictionary(std::string& bytes,
                     std::span<const uint16_t> dictionaries) {
  auto* data = reinterpret_cast<duckdb::data_ptr_t>(bytes.data());
  auto header = codecs::Header::Parse(data, bytes.size());
  if (header.flags != codecs::kTrainedDictionary) {
    return;
  }
  SDB_ASSERT(header.dictionary <= dictionaries.size());
  header.dictionary = dictionaries[header.dictionary - 1];
  header.Write(data);
}

bool CopyBlocks(ReadContext& ctx, std::span<const ColumnBlockMeta> blocks,
                std::span<const uint16_t> dictionaries, ColBlockSink& sink,
                std::vector<ColumnBlockMeta>& out,
                absl::FunctionRef<bool()> progress) {
  std::string bytes;
  for (const auto& block : blocks) {
    if (!progress()) {
      return false;
    }
    auto& copy = out.emplace_back(ColumnBlockMeta{block.statistics.Copy()});
    copy.tuple_count = block.tuple_count;
    copy.byte_size = block.byte_size;
    copy.codec = block.codec;
    if (block.byte_size == 0) {
      continue;
    }
    const bool patch = !dictionaries.empty() &&
                       duckdb::IsSereneDBCompressionType(block.codec->type);
    std::string_view view;
    if (const auto* stable =
          patch ? nullptr
                : ctx.TryReadStable(block.file_offset, block.byte_size)) {
      view = {reinterpret_cast<const char*>(stable), block.byte_size};
    } else {
      bytes.resize(block.byte_size);
      ctx.Read(block.file_offset,
               reinterpret_cast<duckdb::data_ptr_t>(bytes.data()),
               block.byte_size);
      if (patch) {
        PatchDictionary(bytes, dictionaries);
      }
      view = bytes;
    }
    copy.file_offset = sink.AppendAligned({&view, 1});
  }
  return true;
}

}  // namespace

std::vector<std::string> ReadDictionaries(const ColReader& reader,
                                          const ColumnReader& column) {
  std::vector<std::string> out;
  if (!column.HasDictionaries()) {
    return out;
  }
  ReadContext ctx{reader};
  for (const auto& meta : column.DictionaryMetas()) {
    auto& bytes = out.emplace_back(meta.byte_size, '\0');
    ctx.Read(meta.file_offset,
             reinterpret_cast<duckdb::data_ptr_t>(bytes.data()),
             meta.byte_size);
  }
  return out;
}

bool BlocksRelocatable(const ColumnReader& column) {
  if (column.Type().InternalType() != duckdb::PhysicalType::VARCHAR) {
    return true;
  }
  return absl::c_all_of(column.DataBlocks(), [](const ColumnBlockMeta& block) {
    switch (const auto type = block.codec->type) {
      case duckdb::CompressionType::COMPRESSION_CONSTANT:
      case duckdb::CompressionType::COMPRESSION_EMPTY:
      case duckdb::CompressionType::COMPRESSION_DICTIONARY:
      case duckdb::CompressionType::COMPRESSION_FSST:
      case duckdb::CompressionType::COMPRESSION_DICT_FSST:
        return true;
      default:
        return duckdb::IsSereneDBCompressionType(type);
    }
  });
}

std::optional<ColumnMeta> CopyColumnBlocks(
  const ColReader& reader, const ColumnReader& column, bool validity,
  std::span<const uint16_t> dictionaries, ColBlockSink& sink,
  absl::FunctionRef<bool()> progress) {
  ColumnMeta meta;
  meta.id = column.Id();
  meta.type = column.Type();
  ReadContext ctx{reader};
  if (!CopyBlocks(ctx, column.DataBlocks(), dictionaries, sink, meta.data,
                  progress)) {
    return std::nullopt;
  }
  if (!validity) {
    return meta;
  }
  if (const auto* stored = column.Validity()) {
    if (!CopyBlocks(ctx, stored->DataBlocks(), {}, sink, meta.validity,
                    progress)) {
      return std::nullopt;
    }
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
