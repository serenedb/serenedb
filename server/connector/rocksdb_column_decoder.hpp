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

#include <absl/functional/any_invocable.h>
#include <velox/common/memory/MemoryPool.h>
#include <velox/type/Type.h>
#include <velox/vector/BaseVector.h>

#include <memory>
#include <string_view>

#include "basics/fwd.h"

namespace sdb::connector {

class RocksDBColumnDecoder {
 public:
  using Writer =
    absl::AnyInvocable<void(velox::vector_size_t, std::string_view) const>;

  explicit RocksDBColumnDecoder(Writer&& writer) : _writer(std::move(writer)) {}
  virtual ~RocksDBColumnDecoder() = default;

  void Add(velox::vector_size_t idx, std::string_view value) {
    _writer(idx, value);
  }

  virtual velox::VectorPtr Finish(velox::vector_size_t actual_rows) = 0;

 private:
  Writer _writer;
};

std::unique_ptr<RocksDBColumnDecoder> MakeRocksDBColumnDecoder(
  const velox::TypePtr& type, velox::vector_size_t max_rows,
  velox::memory::MemoryPool& pool);

}  // namespace sdb::connector
