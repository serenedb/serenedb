////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "rocksdb_utils.h"

#include <rocksdb/convenience.h>
#include <vpack/iterator.h>

#include <string>
#include <string_view>

#include "basics/errors.h"
#include "basics/static_strings.h"

using namespace sdb;

namespace {

bool HasObjectIds(vpack::Slice input_slice) {
  bool rv = false;
  if (input_slice.isObject()) {
    for (auto object_pair : vpack::ObjectIterator(input_slice)) {
      if (object_pair.key.stringView() == StaticStrings::kObjectId) {
        return true;
      }
      rv = HasObjectIds(object_pair.value());
      if (rv) {
        break;
      }
    }
  } else if (input_slice.isArray()) {
    for (auto slice : vpack::ArrayIterator(input_slice)) {
      rv = HasObjectIds(slice);
      if (rv) {
        break;
      }
    }
  }
  return rv;
}

vpack::Builder& StripObjectIdsImpl(vpack::Builder& builder,
                                   vpack::Slice input_slice) {
  if (input_slice.isObject()) {
    builder.openObject();
    for (auto object_pair : vpack::ObjectIterator(input_slice)) {
      if (object_pair.key.stringView() == StaticStrings::kObjectId) {
        continue;
      }
      builder.add(object_pair.key);
      StripObjectIdsImpl(builder, object_pair.value());
    }
    builder.close();
  } else if (input_slice.isArray()) {
    builder.openArray();
    for (auto slice : vpack::ArrayIterator(input_slice)) {
      StripObjectIdsImpl(builder, slice);
    }
    builder.close();
  } else {
    builder.add(input_slice);
  }
  return builder;
}

}  // namespace

namespace sdb::rocksutils {

sdb::Result ConvertStatus(const rocksdb::Status& status, StatusHint hint) {
  switch (status.code()) {
    case rocksdb::Status::Code::kOk:
      return {};
    case rocksdb::Status::Code::kNotFound:
      switch (hint) {
        case StatusHint::kCollection:
          return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
        case StatusHint::kDatabase:
          return {ERROR_SERVER_DATABASE_NOT_FOUND};
        case StatusHint::kDocument:
          return {ERROR_SERVER_DOCUMENT_NOT_FOUND};
        case StatusHint::kIndex:
          return {ERROR_SERVER_INDEX_NOT_FOUND};
        case StatusHint::kView:
          return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
        case StatusHint::kWal:
          // suppress this error if the WAL is queried for changes that are not
          // available
          return {};
        default:
          return {ERROR_SERVER_DOCUMENT_NOT_FOUND, status.ToString()};
      }
    case rocksdb::Status::Code::kCorruption:
      return {ERROR_SERVER_CORRUPTED_DATAFILE, status.ToString()};
    case rocksdb::Status::Code::kNotSupported:
      return {ERROR_NOT_IMPLEMENTED, status.ToString()};
    case rocksdb::Status::Code::kInvalidArgument:
      return {ERROR_BAD_PARAMETER, status.ToString()};
    case rocksdb::Status::Code::kIOError:
      if (status.subcode() == rocksdb::Status::SubCode::kNoSpace) {
        return {ERROR_SERVER_FILESYSTEM_FULL, status.ToString()};
      }
      return {ERROR_SERVER_IO_ERROR, status.ToString()};
    case rocksdb::Status::Code::kMergeInProgress:
      return {ERROR_SERVER_MERGE_IN_PROGRESS, status.ToString()};
    case rocksdb::Status::Code::kIncomplete:
      return {ERROR_SERVER_INCOMPLETE_READ,
              "'incomplete' error in storage engine: " + status.ToString()};
    case rocksdb::Status::Code::kShutdownInProgress:
      return {ERROR_SHUTTING_DOWN, status.ToString()};
    case rocksdb::Status::Code::kTimedOut:
      if (status.subcode() == rocksdb::Status::SubCode::kMutexTimeout) {
        return {ERROR_LOCK_TIMEOUT, status.ToString()};
      }
      if (status.subcode() == rocksdb::Status::SubCode::kLockTimeout) {
        return {ERROR_SERVER_CONFLICT,
                "timeout waiting to lock key " + status.ToString()};
      }
      return {ERROR_LOCK_TIMEOUT, status.ToString()};
    case rocksdb::Status::Code::kAborted:
      return {ERROR_TRANSACTION_ABORTED, status.ToString()};
    case rocksdb::Status::Code::kBusy:
      if (status.subcode() == rocksdb::Status::SubCode::kDeadlock) {
        return {ERROR_DEADLOCK, status.ToString()};
      }
      if (status.subcode() == rocksdb::Status::SubCode::kLockLimit) {
        // should actually not occur with our RocksDB configuration
        return {ERROR_RESOURCE_LIMIT,
                "failed to acquire lock due to lock number limit " +
                  status.ToString()};
      }
      return {ERROR_SERVER_CONFLICT, "write-write conflict"};
    case rocksdb::Status::Code::kExpired:
      return {ERROR_INTERNAL,
              "key expired; TTL was set in error " + status.ToString()};
    case rocksdb::Status::Code::kTryAgain:
      return {ERROR_SERVER_TRY_AGAIN, status.ToString()};
    default:
      return {ERROR_INTERNAL,
              "unknown RocksDB status code " + status.ToString()};
  }
}

std::pair<vpack::Slice, std::unique_ptr<vpack::BufferUInt8>> StripObjectIds(
  vpack::Slice input_slice, bool check_before_copy) {
  std::unique_ptr<vpack::BufferUInt8> buffer;
  if (check_before_copy) {
    if (!HasObjectIds(input_slice)) {
      return {input_slice, std::move(buffer)};
    }
  }
  buffer = std::make_unique<vpack::BufferUInt8>();
  vpack::Builder builder(*buffer);
  StripObjectIdsImpl(builder, input_slice);
  return {vpack::Slice(buffer->data()), std::move(buffer)};
}

}  // namespace sdb::rocksutils
