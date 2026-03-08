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

#include <velox/vector/ComplexVector.h>

#include <yaclib/async/future.hpp>

#include "basics/fwd.h"

namespace sdb::query {

class Query;

class BatchExecutor {
 public:
  virtual void SetQuery(Query& query) = 0;
  virtual yaclib::Future<> Execute(velox::RowVectorPtr& batch) = 0;
  virtual void RequestCancel() = 0;
  virtual ~BatchExecutor() = default;

  decltype(auto) IgnoreOutput(this auto&& self) noexcept {
    return (self._ignore_output);
  }

 protected:
  [[maybe_unused]] bool _ignore_output = false;
};

}  // namespace sdb::query
