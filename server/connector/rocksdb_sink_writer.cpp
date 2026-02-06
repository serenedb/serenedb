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

#include "rocksdb_sink_writer.hpp"

#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb::connector {

void RocksDBSinkWriter::Write(std::span<const rocksdb::Slice> cell_slices,
                              std::string_view full_key) {
  rocksdb::Slice key_slice(full_key);
  rocksdb::Status status;
  SDB_ASSERT(!cell_slices.empty());
  if (cell_slices.size() == 1) {
    // Optimizing single slice case - rocksdb does not do additional copying
    // while gathering slice parts
    status = _transaction.Put(&_cf, key_slice, cell_slices.front());
  } else {
    // TODO(Dronplane): Currently RocksDB does intermediate merging
    // all parts to a single string before actual inserting where it copies it
    // all again to the transaction buffer. Let's  propose a PR for them that
    // keeps SliceParts until they are copied to the transaction buffer.
    status = _transaction.Put(
      &_cf, rocksdb::SliceParts(&key_slice, 1),
      rocksdb::SliceParts(cell_slices.data(), cell_slices.size()));
  }
  if (!status.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(status));
  }
}

std::unique_ptr<rocksdb::Iterator> RocksDBSinkWriter::CreateIterator() {
  rocksdb::ReadOptions read_options;
  read_options.async_io = IsIOUringEnabled();
  read_options.snapshot = _transaction.GetSnapshot();
  return std::unique_ptr<rocksdb::Iterator>{
    _transaction.GetIterator(read_options, &_cf)};
}

void RocksDBSinkWriter::DeleteCell(std::string_view full_key) {
  rocksdb::Slice key_slice(full_key);
  rocksdb::Status status = _transaction.Delete(&_cf, key_slice);
  if (!status.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(status));
  }
}

}  // namespace sdb::connector
