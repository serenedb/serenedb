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

#pragma once

#include <memory>
#include <span>

#include "common.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"

namespace sdb::connector {

class SSTSinkWriter {
 public:
  SSTSinkWriter(rocksdb::DB& db, rocksdb::ColumnFamilyHandle& cf,
                std::span<const ColumnInfo> columns,
                std::string_view rocksdb_directory);

  void SetColumnIndex(size_t column_idx) { _column_idx = column_idx; }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key);

  void Finish();

  void Abort();

 private:
  rocksdb::DB* _db;
  rocksdb::ColumnFamilyHandle* _cf;
  std::vector<std::unique_ptr<rocksdb::SstFileWriter>> _writers;
  std::string _sst_directory;
  int64_t _column_idx = -1;
};

}  // namespace sdb::connector
