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

#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb::connector {

void RocksDBSinkWriter::ConfigureReadOptions() {
  _read_options.async_io = true;
  _read_options.snapshot = _transaction.GetSnapshot();
}

size_t RocksDBSinkWriter::HandleConflicts(primary_key::Keys& keys,
                                          std::span<const std::string> s) {
  SDB_ASSERT(keys.size() == s.size() || s.empty());
  if (_conflict_policy == WriteConflictPolicy::Replace) {
    // Optimize out reading
    return 0;
  }

  auto* db = _transaction.GetDB();
  SDB_ASSERT(db);

  ConfigureReadOptions();
  size_t skipped_cnt = 0;
  for (size_t i = 0; i < keys.size(); ++i) {
    auto& key = keys[i];
    if (key.empty()) {
      // marked as duplicated in locks
      continue;
    }
    // Old key equals to new key
    if (!s.empty() && s[i] == key) {
      continue;
    }

    auto status = db->Get(_read_options, &_cf, key, &_lookup_value);
    // We do not need value, so it may be forgotten immediately.
    // See issue #184
    _lookup_value.Reset();

    if (!status.ok() && !status.IsNotFound()) {
      SDB_THROW(rocksutils::ConvertStatus(status));
    }

    const bool conflict = status.ok();

    if (conflict) {
      switch (_conflict_policy) {
        case WriteConflictPolicy::Replace:
          SDB_ASSERT(false,
                     "WriteConflictPolicy::Update should be handled earlier "
                     "for optimiztion reason");
          break;
        case WriteConflictPolicy::DoNothing:
          // Mark key: it should be skipped
          key.clear();
          skipped_cnt++;
          break;
        case WriteConflictPolicy::EmitError:
          SDB_THROW(ERROR_SERVER_UNIQUE_CONSTRAINT_VIOLATED,
                    "Primary key already exists");
          break;
        default:
          SDB_UNREACHABLE();
      }
    }
  }
  return skipped_cnt;
}

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
  ConfigureReadOptions();
  return std::unique_ptr<rocksdb::Iterator>{
    _transaction.GetIterator(_read_options, &_cf)};
}

void RocksDBSinkWriter::DeleteCell(std::string_view full_key) {
  rocksdb::Slice key_slice(full_key);
  rocksdb::Status status = _transaction.Delete(&_cf, key_slice);
  if (!status.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(status));
  }
}

}  // namespace sdb::connector
