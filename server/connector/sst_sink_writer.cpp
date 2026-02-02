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
#include <format>
#include <random>
#include <string>

#include "basics/assert.h"
#include "basics/random/random_generator.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb::connector {

std::string GenerateSSTDirPath(std::string_view rocksdb_directory) {
  auto now = std::chrono::system_clock::now();
  auto timestamp =
    std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch())
      .count();

  return absl::StrCat(rocksdb_directory, "/", "bulk_insert_", timestamp, "_",
                      random::RandU64());
}

SSTSinkWriter::SSTSinkWriter(rocksdb::DB& db, rocksdb::ColumnFamilyHandle& cf,
                             std::span<const ColumnInfo> columns,
                             std::string_view rocksdb_directory)
  : _db{&db}, _cf{&cf}, _sst_directory{GenerateSSTDirPath(rocksdb_directory)} {
  _writers.resize(columns.size());

  auto options = _db->GetOptions(_cf);
  options.PrepareForBulkLoad();

  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy = nullptr;
  options.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(table_options));
  options.compression = rocksdb::kNoCompression;
  options.compression_per_level.clear();

  std::filesystem::create_directories(_sst_directory);
  for (size_t i = 0; i < _writers.size(); ++i) {
    if (columns[i].id == catalog::Column::kGeneratedPKId) {
      continue;
    }

    _writers[i] =
      std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions{}, options);
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
  SDB_ASSERT(_column_idx < _writers.size());
  SDB_ASSERT(_writers[_column_idx]);

  rocksdb::Slice key_slice{full_key};
  rocksdb::Status status;
  auto& writer = *_writers[_column_idx];
  if (cell_slices.size() == 1) {
    status = writer.Put(key_slice, cell_slices.front());
  } else {
    std::string merged_value;
    size_t total_size = 0;
    for (const auto& slice : cell_slices) {
      total_size += slice.size();
    }
    merged_value.reserve(total_size);
    for (const auto& slice : cell_slices) {
      merged_value.append(slice.data(), slice.size());
    }
    status = writer.Put(key_slice, merged_value);
  }

  if (!status.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(status));
  }
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
