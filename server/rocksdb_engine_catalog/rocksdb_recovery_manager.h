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

#pragma once

#include <rocksdb/types.h>

#include <atomic>

#include "basics/common.h"
#include "basics/result.h"
#include "rest_server/serened.h"

namespace rocksdb {

class TransactionDB;
}  // namespace rocksdb

namespace sdb {

enum class RecoveryState : uint32_t {
  /// recovery is not yet started
  Before = 0,

  /// recovery is in progress
  InProgress,

  /// recovery is done
  Done,
};

class RocksDBRecoveryManager final : public SerenedFeature {
 public:
  static constexpr std::string_view name() { return "RocksDBRecoveryManager"; }

  explicit RocksDBRecoveryManager(Server& server);

  void prepare() final;
  void unprepare() final;

  void start() final;

  RecoveryState recoveryState() const noexcept;

  // current recovery sequence number
  rocksdb::SequenceNumber recoverySequenceNumber() const noexcept;

  Result registerPostRecoveryCallback(std::function<Result()>&& callback);

 private:
  Result parseRocksWAL();
  void runRecovery();
  void recoveryDone();

  std::vector<std::function<Result()>> _pending_recovery_callbacks;
  std::atomic<rocksdb::SequenceNumber> _current_sequence_number;
  std::atomic<RecoveryState> _recovery_state;
};

}  // namespace sdb
