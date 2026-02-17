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

template<typename Buffer>
inline void Resize(Buffer& buf, size_t additional) {
  buf.resize(buf.size() + additional);
}

template<typename Buffer>
inline void PutVarint32x3ToBuffer(Buffer& buffer, uint32_t v1, uint32_t v2,
                                  uint32_t v3) {
  const size_t len = VarintLength(v1) + VarintLength(v2) + VarintLength(v3);
  const size_t pos = buffer.size();
  Resize(buffer, len);
  char* ptr = reinterpret_cast<char*>(buffer.data()) + pos;
  ptr = EncodeVarint32(ptr, v1);
  ptr = EncodeVarint32(ptr, v2);
  EncodeVarint32(ptr, v3);
}

}  // namespace

template<bool IsGeneratedPK>
SSTBlockBuilder<IsGeneratedPK>::SSTBlockBuilder(int64_t generated_pk_counter,
                                                ObjectId table_id,
                                                catalog::Column::Id column_id)
  : _generated_pk_counter{generated_pk_counter},
    _table_id{table_id},
    _column_id{column_id} {
  constexpr size_t kCapacity = 2 * kFlushThreshold;
  _cur.buffer.reserve(kCapacity);
  _next.buffer.reserve(kCapacity);
}

template<bool IsGeneratedPK>
void SSTBlockBuilder<IsGeneratedPK>::AddEntry(
  std::span<const rocksdb::Slice> value_slices) {
  AddEntryImpl(_cur, value_slices);
}

template<bool IsGeneratedPK>
void SSTBlockBuilder<IsGeneratedPK>::AddEntryImpl(
  Block& block, std::span<const rocksdb::Slice> value_slices) {
  const size_t pk_size = GetPKSize();
  constexpr size_t kInternalKeyFooterSize =
    sizeof(rocksdb::SstFileWriter::kInternalKeyFooter);

  size_t total_value_size = 0;
  for (const auto& slice : value_slices) {
    total_value_size += slice.size();
  }

  auto& buffer = block.buffer;
  const bool is_first = block.entry_cnt == 0;

  const uint32_t shared = is_first ? 0 : kPrefixSize;
  const uint32_t non_shared =
    (is_first ? kPrefixSize : 0) + pk_size + kInternalKeyFooterSize;
  const uint32_t value_size = static_cast<uint32_t>(total_value_size);

  const size_t varint_len =
    VarintLength(shared) + VarintLength(non_shared) + VarintLength(value_size);
  const size_t prefix_len = is_first ? kPrefixSize : 0;
  const size_t total_entry_size = varint_len + prefix_len + pk_size +
                                  kInternalKeyFooterSize + total_value_size;

  const size_t start_pos = buffer.size();
  Resize(buffer, total_entry_size);
  char* ptr = reinterpret_cast<char*>(buffer.data()) + start_pos;

  // Write delta-encoding
  ptr = EncodeVarint32(ptr, shared);
  ptr = EncodeVarint32(ptr, non_shared);
  ptr = EncodeVarint32(ptr, value_size);

  if (is_first) [[unlikely]] {
    absl::big_endian::Store(ptr, _table_id);
    absl::big_endian::Store(ptr + sizeof(ObjectId), _column_id);
    ptr += kPrefixSize;
  }

  block.last_pk_offset = ptr - reinterpret_cast<char*>(buffer.data());
  if constexpr (IsGeneratedPK) {
    const int64_t generated_pk = _generated_pk_counter + _total_entry_cnt;
    absl::big_endian::Store(ptr, generated_pk);
    ptr[0] = static_cast<uint8_t>(ptr[0]) ^ 0x80;
    block.last_pk_size = sizeof(generated_pk);
  } else {
    const auto& pk = (*_keys)[_row_idx];
    std::memcpy(ptr, pk.data(), pk.size());
    block.last_pk_size = pk.size();
  }
  ptr += block.last_pk_size;

  std::memcpy(ptr, &rocksdb::SstFileWriter::kInternalKeyFooter,
              kInternalKeyFooterSize);
  ptr += kInternalKeyFooterSize;

  for (const auto& slice : value_slices) {
    std::memcpy(ptr, slice.data(), slice.size());
    ptr += slice.size();
  }

  block.raw_key_size += kPrefixSize + pk_size + kInternalKeyFooterSize;
  block.raw_value_size += total_value_size;
  ++block.entry_cnt;
  ++_total_entry_cnt;
}

template<bool IsGeneratedPK>
size_t SSTBlockBuilder<IsGeneratedPK>::GetPKSize() const {
  if constexpr (IsGeneratedPK) {
    return sizeof(int64_t);
  } else {
    return (*_keys)[_row_idx].size();
  }
}

template<bool IsGeneratedPK>
size_t SSTBlockBuilder<IsGeneratedPK>::AppendPK(Block& block) {
  if constexpr (IsGeneratedPK) {
    const int64_t generated_pk = _generated_pk_counter + _total_entry_cnt;
    const size_t pos = block.buffer.size();
    Resize(block.buffer, sizeof(generated_pk));
    char* ptr = reinterpret_cast<char*>(block.buffer.data()) + pos;
    absl::big_endian::Store(ptr, generated_pk);
    ptr[0] = static_cast<uint8_t>(ptr[0]) ^ 0x80;
    return sizeof(generated_pk);
  } else {
    // TODO: maybe it's better to build primary key directly
    // in the buffer of the first SSTBlockBuilder and the others
    // will copy from the first
    const auto& pk = (*_keys)[_row_idx];
    const size_t pos = block.buffer.size();
    Resize(block.buffer, pk.size());
    std::memcpy(reinterpret_cast<char*>(block.buffer.data()) + pos, pk.data(),
                pk.size());
    return pk.size();
  }
}

template<bool IsGeneratedPK>
std::string SSTBlockBuilder<IsGeneratedPK>::BuildLastKey() const {
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

template<bool IsGeneratedPK>
rocksdb::BlockFlushData SSTBlockBuilder<IsGeneratedPK>::Finish(
  std::span<const rocksdb::Slice> next_block_first_value) {
  constexpr uint32_t kRestartOffset = 0;
  size_t pos = _cur.buffer.size();
  Resize(_cur.buffer, sizeof(uint32_t));
  absl::little_endian::Store(
    reinterpret_cast<uint32_t*>(_cur.buffer.data() + pos), kRestartOffset);

  constexpr uint32_t kNumRestarts = 1;
  uint32_t block_footer = kNumRestarts;
  pos = _cur.buffer.size();
  Resize(_cur.buffer, sizeof(uint32_t));
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
    const char* first_key = reinterpret_cast<const char*>(_next.buffer.data()) +
                            _next.last_pk_offset - kPrefixSize;
    first_key_in_next_block = {first_key, key_size};
  }

  // TODO: build the last key with delta = 0 to pass only view to buffer for
  // _last_key_buffer
  _last_key_buffer = BuildLastKey();
  return {
    .buffer = {reinterpret_cast<char*>(_cur.buffer.data()), _cur.buffer.size()},
    .last_key_in_current_block = _last_key_buffer,
    .first_key_in_next_block = first_key_in_next_block,
    .num_entries = _cur.entry_cnt,
    .raw_key_size = _cur.raw_key_size,
    .raw_value_size = _cur.raw_value_size};
}

template<bool IsGeneratedPK>
void SSTBlockBuilder<IsGeneratedPK>::NextBlock() {
  _cur.buffer.clear();
  _cur.last_pk_offset = 0;
  _cur.last_pk_size = 0;
  _cur.entry_cnt = 0;
  _cur.raw_key_size = 0;
  _cur.raw_value_size = 0;
  std::swap(_cur, _next);
}

template class SSTBlockBuilder<true>;
template class SSTBlockBuilder<false>;

std::string GenerateSSTDirPath() {
  auto now = std::chrono::system_clock::now();
  auto timestamp =
    std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch())
      .count();
  return absl::StrCat(sdb::GetServerEngine().path(), "/", "bulk_insert_",
                      timestamp, "_", sdb::random::RandU64());
}

template<bool IsGeneratedPK>
SSTSinkWriter<IsGeneratedPK>::SSTSinkWriter(ObjectId table_id, rocksdb::DB& db,
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

  const auto generated_pk_counter =
    std::bit_cast<int64_t>(RevisionId::create().id());
  for (size_t i = 0; i < _writers.size(); ++i) {
    if (columns[i].id == catalog::Column::kGeneratedPKId) {
      continue;
    }

    _writers[i] = std::make_unique<rocksdb::SstFileWriter>(env, options);
    _block_builders[i] = std::make_unique<SSTBlockBuilder<IsGeneratedPK>>(
      generated_pk_counter, table_id, columns[i].id);
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
  std::span<const rocksdb::Slice> cell_slices, std::string_view full_key) {
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

template<bool IsGeneratedPK>
void SSTSinkWriter<IsGeneratedPK>::FlushBlockBuilder(
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

template<bool IsGeneratedPK>
void SSTSinkWriter<IsGeneratedPK>::Abort() {
  std::error_code ec;
  std::filesystem::remove_all(_sst_directory, ec);
}

template class SSTSinkWriter<true>;
template class SSTSinkWriter<false>;

}  // namespace sdb::connector
