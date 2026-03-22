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

#include <absl/functional/any_invocable.h>
#include <vpack/builder.h>
#include <vpack/iterator.h>

#include <atomic>
#include <memory>
#include <string>
#include <string_view>

#include "aql/execution_state.h"
#include "basics/assert.h"
#include "basics/buffer.h"
#include "basics/common.h"
#include "basics/result.h"
#include "basics/system-functions.h"
#include "catalog/types.h"
#include "function2.hpp"

namespace vpack {

class Builder;
class Slice;

}  // namespace vpack
namespace sdb {
namespace transaction {

class Context;
}

typedef Tick CursorId;

class Cursor {
 public:
  Cursor(const Cursor&) = delete;
  Cursor& operator=(const Cursor&) = delete;

  Cursor(CursorId id, size_t batch_size, double ttl, bool has_count,
         bool is_retriable)
    : _id(id),
      _batch_size(batch_size == 0 ? 1 : batch_size),
      _current_batch_id(0),
      _last_available_batch_id(1),
      _ttl(ttl),
      _expires(utilities::GetMicrotime() + _ttl),
      _has_count(has_count),
      _is_retriable(is_retriable),
      _is_deleted(false),
      _is_used(false) {}

  virtual ~Cursor() = default;

 public:
  CursorId id() const noexcept { return _id; }

  size_t batchSize() const noexcept { return _batch_size; }

  bool hasCount() const noexcept { return _has_count; }

  bool isRetriable() const noexcept { return _is_retriable; }

  double ttl() const noexcept { return _ttl; }

  double expires() const noexcept {
    return _expires.load(std::memory_order_relaxed);
  }

  bool isUsed() const noexcept {
    // (1) - this release-store synchronizes-with the acquire-load (2)
    return _is_used.load(std::memory_order_acquire);
  }

  bool isDeleted() const noexcept { return _is_deleted; }

  void setDeleted() noexcept { _is_deleted = true; }

  bool isCurrentBatchId(uint64_t id) const noexcept {
    return id == _current_batch_result.first;
  }

  bool isNextBatchId(uint64_t id) const {
    return id == _current_batch_result.first + 1 &&
           id == _last_available_batch_id;
  }

  void setLastQueryBatchObject(
    std::shared_ptr<vpack::BufferUInt8> buffer) noexcept {
    _current_batch_result.second = std::move(buffer);
  }

  std::shared_ptr<vpack::BufferUInt8> getLastBatch() const {
    return _current_batch_result.second;
  }

  uint64_t storedBatchId() const { return _current_batch_result.first; }

  void handleNextBatchIdValue(vpack::Builder& builder, bool has_more) {
    _current_batch_result.first = ++_current_batch_id;
    if (has_more) {
      builder.add("nextBatchId", std::to_string(_current_batch_id + 1));
      _last_available_batch_id = _current_batch_id + 1;
    }
  }

  void use() noexcept {
    SDB_ASSERT(!_is_deleted);
    SDB_ASSERT(!_is_used);

    _is_used.store(true, std::memory_order_relaxed);
  }

  void release() noexcept {
    SDB_ASSERT(_is_used);
    _expires.store(utilities::GetMicrotime() + _ttl, std::memory_order_relaxed);
    // (2) - this release-store synchronizes-with the acquire-load (1)
    _is_used.store(false, std::memory_order_release);
  }

  virtual void kill() {}

  // Debug method to kill a query at a specific position
  // during execution. It internally asserts that the query
  // is actually visible through other APIS (e.g. current queries)
  // so user actually has a chance to kill it here.
  virtual void debugKillQuery() {}

  virtual uint64_t memoryUsage() const noexcept = 0;

  virtual size_t count() const = 0;

  virtual std::shared_ptr<transaction::Context> context() const = 0;

  /**
   * Dump the cursor result, async version. The caller needs to be
   * contiueable
   *
   * result The Builder to write the result to
   * continueHandler The function that is posted on scheduler to contiue
   * this execution.
   *
   * Return First: ExecutionState either DONE or WAITING. On Waiting we need to
   * free this thread on DONE we have a result. Second: Result If State==DONE
   * this contains Error information or NO_ERROR. On NO_ERROR result is filled.
   */
  virtual std::pair<aql::ExecutionState, Result> dump(
    vpack::Builder& result) = 0;

  /**
   * Dump the cursor result. This is guaranteed to return the result in
   * this thread.
   *
   * result the Builder to write the result to
   *
   * Return ErrorResult, if something goes wrong
   */
  virtual Result dumpSync(vpack::Builder& result) = 0;

  /// Set wakeup handler on streaming cursor
  virtual void setWakeupHandler(absl::AnyInvocable<bool()>&& cb) {}
  virtual void resetWakeupHandler() {}

  virtual bool allowDirtyReads() const noexcept { return false; }

 protected:
  const CursorId _id;
  const size_t _batch_size;
  size_t _current_batch_id;
  size_t _last_available_batch_id;
  const double _ttl;
  std::atomic<double> _expires;
  const bool _has_count;
  const bool _is_retriable;
  bool _is_deleted;
  std::atomic<bool> _is_used;
  std::pair<uint64_t, std::shared_ptr<vpack::BufferUInt8>>
    _current_batch_result;
};

}  // namespace sdb
