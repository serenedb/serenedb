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

#include <filesystem>
#include <format>
#include <iostream>
#include <random>
#include <string>

#include "basics/assert.h"
#include "rocksdb/options.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb::connector {

//  TODO before PR: do smthing with that
std::string GenerateSSTFilePath() {
  auto now = std::chrono::system_clock::now();
  auto timestamp =
    std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch())
      .count();

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis(0, UINT32_MAX);
  uint32_t random_suffix = dis(gen);

  auto temp_dir = std::filesystem::temp_directory_path();
  return (temp_dir /
          std::format("serenedb_sst_{}_{}.sst", timestamp, random_suffix))
    .string();
}

SSTSinkWriter::SSTSinkWriter(rocksdb::DB& db, rocksdb::ColumnFamilyHandle& cf,
                             size_t column_count)
  : _db{&db}, _cf{&cf} {
  _writers.reserve(column_count);
  // TODO before PR: fix this column_count - 1 (generated)
  for (size_t i = 0; i < column_count - 1; ++i) {
    _writers.push_back(std::make_unique<rocksdb::SstFileWriter>(
      rocksdb::EnvOptions{}, _db->GetOptions(_cf)));
    auto status = _writers[i]->Open(GenerateSSTFilePath());
    if (!status.ok()) {
      SDB_THROW(rocksutils::ConvertStatus(status));
    }
  }
}

void SSTSinkWriter::Write(size_t column_idx,
                          std::span<const rocksdb::Slice> cell_slices,
                          std::string_view full_key) {
  SDB_ASSERT(!cell_slices.empty());
  SDB_ASSERT(column_idx < _writers.size());

  rocksdb::Slice key_slice{full_key};
  rocksdb::Status status;

  if (cell_slices.size() == 1) {
    status = _writers[column_idx]->Put(key_slice, cell_slices.front());
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
    status = _writers[column_idx]->Put(key_slice, merged_value);
  }

  if (!status.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(status));
  }
}

void SSTSinkWriter::Finish() {
  std::vector<std::string> sst_files;
  sst_files.reserve(_writers.size());
  for (auto& writer : _writers) {
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

void SSTSinkWriter::Abort() {}

}  // namespace sdb::connector
