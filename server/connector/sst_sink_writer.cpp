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
#include "catalog/table_options.h"
#include "key_utils.hpp"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

namespace {

inline constexpr size_t VarintLength(uint32_t v) {
  if (v < (1 << 7)) {
    return 1;
  } else if (v < (1 << 14)) {
    return 2;
  } else if (v < (1 << 21)) {
    return 3;
  } else if (v < (1 << 28)) {
    return 4;
  } else {
    return 5;
  }
}

inline char* EncodeVarint32(char* dst, uint32_t v) {
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  constexpr int kB = 128;
  if (v < (1 << 7)) {
    *(ptr++) = v;
  } else if (v < (1 << 14)) {
    *(ptr++) = v | kB;
    *(ptr++) = v >> 7;
  } else if (v < (1 << 21)) {
    *(ptr++) = v | kB;
    *(ptr++) = (v >> 7) | kB;
    *(ptr++) = v >> 14;
  } else if (v < (1 << 28)) {
    *(ptr++) = v | kB;
    *(ptr++) = (v >> 7) | kB;
    *(ptr++) = (v >> 14) | kB;
    *(ptr++) = v >> 21;
  } else {
    *(ptr++) = v | kB;
    *(ptr++) = (v >> 7) | kB;
    *(ptr++) = (v >> 14) | kB;
    *(ptr++) = (v >> 21) | kB;
    *(ptr++) = v >> 28;
  }
  return reinterpret_cast<char*>(ptr);
}

inline void PutVarint32x3ToBuffer(std::string& buffer, uint32_t v1, uint32_t v2,
                                  uint32_t v3) {
  const size_t len = VarintLength(v1) + VarintLength(v2) + VarintLength(v3);
  const size_t pos = buffer.size();
  basics::StrAppend(buffer, len);
  char* ptr = buffer.data() + pos;
  ptr = EncodeVarint32(ptr, v1);
  ptr = EncodeVarint32(ptr, v2);
  EncodeVarint32(ptr, v3);
}

}  // namespace

SSTBlockBuilder::SSTBlockBuilder(int64_t generated_pk_counter,
                                 ObjectId table_id,
                                 catalog::Column::Id column_id)
  : _generated_pk_counter{generated_pk_counter},
    _table_id{table_id},
    _column_id{column_id} {
  constexpr size_t kCapacity = 2 * kFlushThreshold;
  _cur.buffer.reserve(kCapacity);
  _next.buffer.reserve(kCapacity);
}

void SSTBlockBuilder::AddEntry(std::span<const rocksdb::Slice> value_slices) {
  AddEntryImpl(_cur, value_slices);
}

void SSTBlockBuilder::AddEntryImpl(
  Block& block, std::span<const rocksdb::Slice> value_slices) {
  constexpr size_t kPrimaryKeySize = sizeof(uint64_t);
  constexpr size_t kInternalKeyFooterSize =
    sizeof(rocksdb::SstFileWriter::kInternalKeyFooter);

  size_t total_value_size = 0;
  for (const auto& slice : value_slices) {
    total_value_size += slice.size();
  }

  auto& buffer = block.buffer;
  if (block.entry_cnt == 0) [[unlikely]] {
    const uint32_t non_shared =
      kPrefixSize + kPrimaryKeySize + kInternalKeyFooterSize;
    PutVarint32x3ToBuffer(buffer, 0, non_shared,
                          static_cast<uint32_t>(total_value_size));

    const size_t pos = buffer.size();
    basics::StrAppend(buffer, kPrefixSize);
    absl::big_endian::Store(buffer.data() + pos, _table_id);
    absl::big_endian::Store(buffer.data() + pos + sizeof(ObjectId), _column_id);
  } else {
    const uint32_t non_shared = kPrimaryKeySize + kInternalKeyFooterSize;
    PutVarint32x3ToBuffer(buffer, kPrefixSize, non_shared,
                          static_cast<uint32_t>(total_value_size));
  }

  block.last_pk_offset = buffer.size();
  block.last_pk_size = AppendPK(block);

  const size_t footer_pos = buffer.size();
  basics::StrAppend(buffer, kInternalKeyFooterSize);
  std::memcpy(buffer.data() + footer_pos,
              &rocksdb::SstFileWriter::kInternalKeyFooter,
              kInternalKeyFooterSize);

  for (const auto& slice : value_slices) {
    const size_t value_pos = buffer.size();
    basics::StrAppend(buffer, slice.size());
    std::memcpy(buffer.data() + value_pos, slice.data(), slice.size());
  }

  block.raw_key_size += kPrefixSize + kPrimaryKeySize + kInternalKeyFooterSize;
  block.raw_value_size += total_value_size;
  ++block.entry_cnt;
  ++_total_entry_cnt;
}

size_t SSTBlockBuilder::AppendPK(Block& block) {
  const int64_t generated_pk = _generated_pk_counter + _total_entry_cnt;
  primary_key::AppendSigned(block.buffer, generated_pk);
  return sizeof(generated_pk);
}

std::string SSTBlockBuilder::BuildLastKey() const {
  const size_t key_size = kPrefixSize + _cur.last_pk_size +
                          sizeof(rocksdb::SstFileWriter::kInternalKeyFooter);
  std::string last_key;
  basics::StrResize(last_key, key_size);

  absl::big_endian::Store(last_key.data(), _table_id);
  absl::big_endian::Store(last_key.data() + sizeof(ObjectId), _column_id);
  std::memcpy(last_key.data() + kPrefixSize,
              _cur.buffer.data() + _cur.last_pk_offset, _cur.last_pk_size);
  std::memcpy(last_key.data() + kPrefixSize + _cur.last_pk_size,
              &rocksdb::SstFileWriter::kInternalKeyFooter,
              sizeof(rocksdb::SstFileWriter::kInternalKeyFooter));

  return last_key;
}

std::string SSTBlockBuilder::BuildNextKey() const {
  constexpr size_t kPrimaryKeySize = sizeof(uint64_t);
  const size_t key_size = kPrefixSize + kPrimaryKeySize +
                          sizeof(rocksdb::SstFileWriter::kInternalKeyFooter);
  std::string next_key;
  basics::StrResize(next_key, key_size);

  absl::big_endian::Store(next_key.data(), _table_id);
  absl::big_endian::Store(next_key.data() + sizeof(ObjectId), _column_id);

  // Build the next primary key
  const int64_t next_pk = _generated_pk_counter + _total_entry_cnt;
  primary_key::AppendSigned(next_key, next_pk);

  std::memcpy(next_key.data() + kPrefixSize + kPrimaryKeySize,
              &rocksdb::SstFileWriter::kInternalKeyFooter,
              sizeof(rocksdb::SstFileWriter::kInternalKeyFooter));

  return next_key;
}

rocksdb::BlockFlushData SSTBlockBuilder::Finish(
  std::span<const rocksdb::Slice> next_block_first_value) {
  constexpr uint32_t kRestartOffset = 0;
  size_t pos = _cur.buffer.size();
  basics::StrAppend(_cur.buffer, sizeof(uint32_t));
  absl::little_endian::Store(
    reinterpret_cast<uint32_t*>(_cur.buffer.data() + pos), kRestartOffset);

  constexpr uint32_t kNumRestarts = 1;
  uint32_t block_footer = kNumRestarts;
  pos = _cur.buffer.size();
  basics::StrAppend(_cur.buffer, sizeof(uint32_t));
  absl::little_endian::Store(
    reinterpret_cast<uint32_t*>(_cur.buffer.data() + pos), block_footer);

  rocksdb::Slice first_key_in_next_block;
  if (!next_block_first_value.empty()) {
    AddEntryImpl(_next, next_block_first_value);
    const size_t key_size = kPrefixSize + _next.last_pk_size +
                            sizeof(rocksdb::SstFileWriter::kInternalKeyFooter);
    SDB_ASSERT(_next.last_pk_offset >= kPrefixSize);
    SDB_ASSERT(_next.last_pk_offset - kPrefixSize + key_size <=
               _next.buffer.size());
    const char* first_key =
      _next.buffer.data() + _next.last_pk_offset - kPrefixSize;
    first_key_in_next_block = {first_key, key_size};
  }

  // Store last key in member variable to avoid dangling pointer
  _last_key_buffer = BuildLastKey();

  return {.buffer = &_cur.buffer,
          .last_key_in_current_block = _last_key_buffer,
          .first_key_in_next_block = first_key_in_next_block,
          .num_entries = _cur.entry_cnt,
          .raw_key_size = _cur.raw_key_size,
          .raw_value_size = _cur.raw_value_size};
}

void SSTBlockBuilder::NextBlock() {
  _cur.buffer.clear();
  _cur.last_pk_offset = 0;
  _cur.last_pk_size = 0;
  _cur.entry_cnt = 0;
  _cur.raw_key_size = 0;
  _cur.raw_value_size = 0;
  std::swap(_cur, _next);
}

std::string GenerateSSTDirPath() {
  auto now = std::chrono::system_clock::now();
  auto timestamp =
    std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch())
      .count();
  return absl::StrCat(sdb::GetServerEngine().path(), "/", "bulk_insert_",
                      timestamp, "_", sdb::random::RandU64());
}

SSTSinkWriter::SSTSinkWriter(ObjectId table_id, rocksdb::DB& db,
                             rocksdb::ColumnFamilyHandle& cf,
                             std::span<const ColumnInfo> columns)
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
    SDB_THROW(
      ERROR_INTERNAL,
      "SSTSinkWriter requires BlockBasedTableFactory for PutByInternalKey");
  }

  options.compression = rocksdb::kNoCompression;
  options.compression_per_level.clear();

  std::filesystem::create_directories(_sst_directory);

  rocksdb::EnvOptions env;
  env.use_direct_writes = true;

  // TODO: if generated_pk
  const auto generated_pk_counter =
    std::bit_cast<int64_t>(RevisionId::create().id());
  for (size_t i = 0; i < _writers.size(); ++i) {
    if (columns[i].id == catalog::Column::kGeneratedPKId) {
      continue;
    }

    _writers[i] = std::make_unique<rocksdb::SstFileWriter>(env, options);
    _block_builders[i] = std::make_unique<SSTBlockBuilder>(
      generated_pk_counter, table_id, columns[i].id);
    auto sst_file_path =
      absl::StrCat(_sst_directory, "/", "column_", i, "_.sst");
    auto status = _writers[i]->Open(sst_file_path);
    if (!status.ok()) {
      SDB_THROW(rocksutils::ConvertStatus(status));
    }
  }
}

void SSTSinkWriter::Write(std::span<const rocksdb::Slice> cell_slices,
                          std::string_view full_key) {
  SDB_ASSERT(!cell_slices.empty());
  SDB_ASSERT(_column_idx < _block_builders.size());
  SDB_ASSERT(_block_builders[_column_idx]);

  auto& block_builder = *_block_builders[_column_idx];

  if (block_builder.ShouldFlush()) [[unlikely]] {
    FlushBlockBuilder(_column_idx, cell_slices);
    return;
  }

  block_builder.AddEntry(cell_slices);
}

void SSTSinkWriter::FlushBlockBuilder(
  size_t column_idx, std::span<const rocksdb::Slice> next_block_first_value) {
  SDB_ASSERT(column_idx < _block_builders.size());
  SDB_ASSERT(_block_builders[column_idx]);
  SDB_ASSERT(_writers[column_idx]);

  auto& block_builder = *_block_builders[column_idx];
  if (block_builder.IsEmpty()) {
    return;
  }
  auto flush_data = block_builder.Finish(next_block_first_value);
  auto& writer = *_writers[column_idx];
  writer.FlushFromInternalBuffer(flush_data);
  block_builder.NextBlock();
}

void SSTSinkWriter::Finish() {
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
      FlushBlockBuilder(i, {});
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

void SSTSinkWriter::Abort() {
  std::error_code ec;
  std::filesystem::remove_all(_sst_directory, ec);
}

}  // namespace sdb::connector
