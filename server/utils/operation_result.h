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

#include <vpack/options.h>
#include <vpack/slice.h>

#include "basics/buffer.h"
#include "basics/common.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/result.h"
#include "utils/coro_helper.h"
#include "utils/operation_options.h"

namespace sdb {

// TODO(mbkkt) is uint32_t enough?
using ErrorsCount = containers::FlatHashMap<ErrorCode, uint32_t>;

struct [[nodiscard]] OperationResult final {
  explicit OperationResult(Result result, OperationOptions options)
    : result(std::move(result)), options(std::move(options)) {}

  OperationResult(const OperationResult& other) = delete;
  OperationResult& operator=(const OperationResult& other) = delete;

  OperationResult(OperationResult&& other) noexcept = default;
  OperationResult& operator=(OperationResult&& other) noexcept = default;

  // create result with details
  OperationResult(Result result, std::shared_ptr<vpack::BufferUInt8> buffer,
                  OperationOptions options, ErrorsCount count_error_codes = {})
    : result(std::move(result)),
      buffer(std::move(buffer)),
      options(std::move(options)),
      count_error_codes(std::move(count_error_codes)) {
    if (this->result.ok()) {
      SDB_ASSERT(this->buffer != nullptr);
      SDB_ASSERT(this->buffer->data() != nullptr);
    }
  }

  // Result-like interface
  bool ok() const noexcept { return result.ok(); }
  bool fail() const noexcept { return result.fail(); }
  ErrorCode errorNumber() const noexcept { return result.errorNumber(); }
  bool is(ErrorCode error_number) const noexcept {
    return result.is(error_number);
  }
  bool isNot(ErrorCode error_number) const noexcept {
    return result.isNot(error_number);
  }
  std::string_view errorMessage() const { return result.errorMessage(); }

  bool hasSlice() const noexcept { return buffer != nullptr; }
  vpack::Slice slice() const noexcept {
    SDB_ASSERT(buffer != nullptr);
    return vpack::Slice(buffer->data());
  }

  void reset() noexcept {
    result = {};
    buffer = {};
    options = {};
    count_error_codes = {};
  }

  Result result;
  std::shared_ptr<vpack::BufferUInt8> buffer;
  OperationOptions options;

  // Executive summary for baby operations: reports all errors that did occur
  // during these operations. Details are stored in the respective positions of
  // the failed documents.
  ErrorsCount count_error_codes;
};

}  // namespace sdb

// For more safety
template<>
constexpr const std::__ignore_type& std::__ignore_type::operator=
  <yaclib::Future<sdb::OperationResult>>(
    const yaclib::Future<sdb::OperationResult>&) const noexcept = delete;
