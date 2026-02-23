////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "sst_sink_writer.hpp"

#include <absl/base/internal/endian.h>

#include <filesystem>
#include <string>

#include "basics/assert.h"
#include "basics/random/random_generator.h"
#include "basics/string_utils.h"
#include "basics/system-compiler.h"
#include "catalog/table_options.h"
#include "iresearch/utils/bytes_utils.hpp"
#include "key_utils.hpp"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

namespace {

template<typename Buffer>
inline void AppendSize(Buffer& buf, size_t additional) {
  buf.resize(buf.size() + additional);
}

}  // namespace

static constexpr size_t kInternalKeyFooterSize =
  sizeof(rocksdb::SstFileWriter::kInternalKeyFooter);

template<bool IsGeneratedPK>
SSTBlockBuilder<IsGeneratedPK>::SSTBlockBuilder(ObjectId table_id,
                                                catalog::Column::Id column_id,
                                                velox::memory::MemoryPool& pool)
  : _curr{pool}, _next{pool}, _table_id{table_id}, _column_id{column_id} {
  static_assert(kFlushThreshold == 64 * 1024);
  // https://jemalloc.net/jemalloc.3.html
  static constexpr size_t kCapacity = 80 * 1024;
  _curr.buffer.reserve(kCapacity);
  _next.buffer.reserve(kCapacity);
}

template<bool IsGeneratedPK>
void SSTBlockBuilder<IsGeneratedPK>::AddEntry(
  std::span<const rocksdb::Slice> value_slices, std::string_view key) {
  if (_curr.entry_cnt == 0) [[unlikely]] {
    AddEntryImpl<true>(_curr, key, value_slices);
  } else {
    AddEntryImpl<false>(_curr, key, value_slices);
  }
}

template<bool IsGeneratedPK>
template<bool IsFullKey>
void SSTBlockBuilder<IsGeneratedPK>::AddEntryImpl(
  Block& block, std::string_view key,
  std::span<const rocksdb::Slice> value_slices) {
  const size_t pk_size = IsGeneratedPK ? sizeof(uint64_t) : key.size();

  size_t total_value_size = 0;
  if (value_slices.size() == 1) [[likely]] {
    total_value_size += value_slices.front().size();
  } else {
    for (const auto& slice : value_slices) {
      total_value_size += slice.size();
    }
  }

  auto& buffer = block.buffer;

  const uint32_t shared = IsFullKey ? 0 : kPrefixSize;
  const uint32_t non_shared =
    (IsFullKey ? kPrefixSize : 0) + pk_size + kInternalKeyFooterSize;
  const uint32_t value_size = static_cast<uint32_t>(total_value_size);

  static constexpr size_t kSharedVarIntLen = 1;
  static constexpr size_t kMaxVarintSize = 5;
  static constexpr size_t kNonSharedVarIntBound =
    IsGeneratedPK ? 1 : kMaxVarintSize;

  const size_t prefix_len = IsFullKey ? kPrefixSize : 0;
  const size_t upper_bound_entry_size =
    kSharedVarIntLen + kNonSharedVarIntBound + kMaxVarintSize + prefix_len +
    pk_size + kInternalKeyFooterSize + total_value_size;

  const size_t start_pos = buffer.size();
  AppendSize(buffer, upper_bound_entry_size);
  uint8_t* ptr = reinterpret_cast<uint8_t*>(buffer.data()) + start_pos;

  // Write delta-encoding
  *(ptr++) = shared;
  if constexpr (IsGeneratedPK) {
    *(ptr++) = non_shared;
  } else {
    irs::WriteVarint(non_shared, ptr);
  }
  irs::WriteVarint(value_size, ptr);

  block.last_pk_offset = reinterpret_cast<const char*>(ptr) -
                         reinterpret_cast<const char*>(buffer.data());
  block.last_pk_size = non_shared;
  if constexpr (IsFullKey) {
    absl::big_endian::Store(ptr, _table_id);
    absl::big_endian::Store(ptr + sizeof(_table_id), _column_id);
    ptr += kPrefixSize;
  }
  std::memcpy(ptr, key.data(), pk_size);
  ptr += pk_size;

  std::memcpy(ptr, &rocksdb::SstFileWriter::kInternalKeyFooter,
              kInternalKeyFooterSize);
  ptr += kInternalKeyFooterSize;

  if (value_slices.size() == 1) [[likely]] {
    const auto& slice = value_slices[0];
    std::memcpy(ptr, slice.data(), slice.size());
    ptr += slice.size();
  } else {
    for (const auto& slice : value_slices) {
      std::memcpy(ptr, slice.data(), slice.size());
      ptr += slice.size();
    }
  }

  // We could write more bytes because we use upper bound for varint
  buffer.resize(ptr - reinterpret_cast<uint8_t*>(buffer.data()));

  block.raw_key_size += kPrefixSize + pk_size + kInternalKeyFooterSize;
  block.raw_value_size += total_value_size;
  ++block.entry_cnt;
}

template<bool IsGeneratedPK>
rocksdb::BlockFlushData SSTBlockBuilder<IsGeneratedPK>::Finish(
  std::string_view next_block_first_pk,
  std::span<const rocksdb::Slice> next_block_first_value) {
  constexpr uint32_t kRestartOffset = 0;
  size_t pos = _curr.buffer.size();
  AppendSize(_curr.buffer, sizeof(kRestartOffset));
  absl::little_endian::Store(
    reinterpret_cast<uint32_t*>(_curr.buffer.data() + pos), kRestartOffset);

  constexpr uint32_t kNumRestarts = 1;
  uint32_t block_footer = kNumRestarts;
  pos = _curr.buffer.size();
  AppendSize(_curr.buffer, sizeof(block_footer));
  absl::little_endian::Store(
    reinterpret_cast<uint32_t*>(_curr.buffer.data() + pos), block_footer);

  size_t block_data_size = _curr.buffer.size();
  rocksdb::Slice first_key_in_next_block;
  if (!next_block_first_value.empty()) {
    SDB_ASSERT(IsLastPKIsFull());
    AddEntryImpl<true>(_next, next_block_first_pk, next_block_first_value);
    first_key_in_next_block = {
      reinterpret_cast<const char*>(_next.buffer.data()) + _next.last_pk_offset,
      _next.last_pk_size};
  } else if (_curr.entry_cnt > 1 && !IsLastPKIsFull()) {
    // First key is always full.
    // This is the last block which is flushed in the Finish() of the
    // SSTSinkWriter. We couldn't know that the entry was the last so
    // it's a non-full key. We'll append it to buffer and make a view.
    const size_t full_pk_size = kPrefixSize + _curr.last_pk_size;
    const size_t append_pos = _curr.buffer.size();
    AppendSize(_curr.buffer, full_pk_size);
    auto* ptr = reinterpret_cast<char*>(_curr.buffer.data()) + append_pos;
    absl::big_endian::Store(ptr, _table_id);
    absl::big_endian::Store(ptr + sizeof(_table_id), _column_id);
    std::memcpy(ptr + kPrefixSize, _curr.buffer.data() + _curr.last_pk_offset,
                _curr.last_pk_size);
    _curr.last_pk_offset = append_pos;
    _curr.last_pk_size = full_pk_size;
  }

  return {
    .buffer = {reinterpret_cast<char*>(_curr.buffer.data()), block_data_size},
    .last_key_in_current_block = {reinterpret_cast<const char*>(
                                    _curr.buffer.data()) +
                                    _curr.last_pk_offset,
                                  _curr.last_pk_size},
    .first_key_in_next_block = first_key_in_next_block,
    .num_entries = _curr.entry_cnt,
    .raw_key_size = _curr.raw_key_size,
    .raw_value_size = _curr.raw_value_size};
}

template<bool IsGeneratedPK>
void SSTBlockBuilder<IsGeneratedPK>::NextBlock() {
  _curr.buffer.clear();
  _curr.last_pk_offset = 0;
  _curr.last_pk_size = 0;
  _curr.last_pk_is_full = false;
  _curr.entry_cnt = 0;
  _curr.raw_key_size = 0;
  _curr.raw_value_size = 0;
  std::swap(_curr, _next);
}

template class SSTBlockBuilder<true>;
template class SSTBlockBuilder<false>;

std::string GenerateSSTDirPath() {
  auto now = std::chrono::system_clock::now();
  auto timestamp =
    std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch())
      .count();
  return absl::StrCat(GetServerEngine().path(), "/", "bulk_insert_", timestamp,
                      "_", random::RandU64());
}

template<bool IsGeneratedPK>
SSTSinkWriter<IsGeneratedPK>::SSTSinkWriter(ObjectId table_id, rocksdb::DB& db,
                                            rocksdb::ColumnFamilyHandle& cf,
                                            std::span<const ColumnInfo> columns,
                                            velox::memory::MemoryPool& pool)
  : _db{&db}, _cf{&cf}, _sst_directory{GenerateSSTDirPath()} {
  _writers.resize(columns.size());
  _block_builders.resize(columns.size());

  auto options = _db->GetOptions(_cf);
  options.PrepareForBulkLoad();

  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy = nullptr;
  options.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(table_options));

  if (!options.table_factory ||
      !options.table_factory->IsInstanceOf(
        rocksdb::TableFactory::kBlockBasedTableName())) {
    SDB_THROW(ERROR_INTERNAL, "SSTSinkWriter requires BlockBasedTableFactory");
  }

  options.compression = rocksdb::kNoCompression;
  options.compression_per_level.clear();

  std::filesystem::create_directories(_sst_directory);

  rocksdb::EnvOptions env;
  env.use_direct_writes = true;

  for (size_t i = 0; i < _writers.size(); ++i) {
    if (columns[i].id == catalog::Column::kGeneratedPKId) {
      continue;
    }

    _writers[i] = std::make_unique<rocksdb::SstFileWriter>(env, options);
    _block_builders[i] = std::make_unique<SSTBlockBuilder<IsGeneratedPK>>(
      table_id, columns[i].id, pool);
    auto sst_file_path =
      absl::StrCat(_sst_directory, "/", "column_", i, "_.sst");
    auto status = _writers[i]->Open(sst_file_path);
    if (!status.ok()) {
      SDB_THROW(rocksutils::ConvertStatus(status));
    }
  }
}

template<bool IsGeneratedPK>
void SSTSinkWriter<IsGeneratedPK>::Write(
  std::span<const rocksdb::Slice> cell_slices, std::string_view key) {
  SDB_ASSERT(!cell_slices.empty());
  SDB_ASSERT(_column_idx < _block_builders.size());
  SDB_ASSERT(_block_builders[_column_idx]);

  auto& block_builder = *_block_builders[_column_idx];

  if (block_builder.ShouldFlush()) [[unlikely]] {
    if (!block_builder.IsLastPKIsFull()) {
      block_builder.AddLastEntry(cell_slices, key);
      return;
    }

    FlushBlockBuilder(_column_idx, key, cell_slices);
    return;
  }

  block_builder.AddEntry(cell_slices, key);
}

template<bool IsGeneratedPK>
void SSTSinkWriter<IsGeneratedPK>::FlushBlockBuilder(
  size_t column_idx, std::string_view next_block_first_pk,
  std::span<const rocksdb::Slice> next_block_first_value) {
  SDB_ASSERT(column_idx < _block_builders.size());
  SDB_ASSERT(_block_builders[column_idx]);
  SDB_ASSERT(_writers[column_idx]);

  auto& block_builder = *_block_builders[column_idx];
  if (block_builder.IsEmpty()) {
    return;
  }
  auto flush_data =
    block_builder.Finish(next_block_first_pk, next_block_first_value);
  auto& writer = *_writers[column_idx];
  writer.FlushFromExternalBuffer(flush_data);
  block_builder.NextBlock();
}

template<bool IsGeneratedPK>
void SSTSinkWriter<IsGeneratedPK>::Finish() {
  irs::Finally clean_dir = [&] noexcept {
    std::error_code ec;
    std::filesystem::remove_all(_sst_directory, ec);
  };

  if (_column_idx == -1) {
    // No column idx set => No data written
    return;
  }

  for (size_t i = 0; i < _block_builders.size(); ++i) {
    if (_block_builders[i]) {
      FlushBlockBuilder(i, {}, {});
    }
  }

  std::vector<std::string> sst_files;
  sst_files.reserve(_writers.size());

  for (auto& writer : _writers) {
    if (!writer) {
      continue;
    }

    rocksdb::ExternalSstFileInfo file_info;
    auto status = writer->Finish(&file_info);
    if (!status.ok()) {
      SDB_THROW(rocksutils::ConvertStatus(status));
    }
    sst_files.emplace_back(file_info.file_path);
  }

  rocksdb::IngestExternalFileOptions ingest_options;
  ingest_options.move_files = true;

  auto status = _db->IngestExternalFile(_cf, sst_files, ingest_options);
  if (!status.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(status));
  }
}

template<bool IsGeneratedPK>
void SSTSinkWriter<IsGeneratedPK>::Abort() {
  std::error_code ec;
  std::filesystem::remove_all(_sst_directory, ec);
}

template class SSTSinkWriter<true>;
template class SSTSinkWriter<false>;

}  // namespace sdb::connector
