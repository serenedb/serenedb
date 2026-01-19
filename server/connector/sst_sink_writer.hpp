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

#include "catalog/table_options.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"

namespace sdb::connector {

class SSTSinkWriter {
 public:
  SSTSinkWriter(rocksdb::DB& db, rocksdb::ColumnFamilyHandle& cf,
                std::span<catalog::Column::Id> column_oids);

  rocksdb::Status Lock(std::string_view full_key) {
    return rocksdb::Status::OK();
  }

  void Write(size_t column_idx, std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key);

  void Finish();

  void Abort();

 private:
  rocksdb::DB* _db;
  rocksdb::ColumnFamilyHandle* _cf;
  std::vector<std::unique_ptr<rocksdb::SstFileWriter>> _writers;
};

}  // namespace sdb::connector
