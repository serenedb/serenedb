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

#include <velox/common/memory/MemoryPool.h>
#include <velox/vector/ComplexVector.h>

#include <string>

#include "basics/fwd.h"
#include "rocksdb_engine_catalog/concat.h"

namespace sdb::connector::primary_key {

// TODO(Dronplane)
// PK can be quite large, do we need std::string?
// If not, maybe we can use string_view/const char* that allocated in arena
using Keys = std::vector<std::string, velox::memory::StlAllocator<std::string>>;

void Create(const velox::RowVector& data,
            std::span<const facebook::velox::column_index_t> key_childs,
            velox::vector_size_t idx, std::string& key);

void Create(const velox::RowVector& data,
            std::span<const velox::column_index_t> key_childs, Keys& buffer);

void Create(const velox::RowVector& data, velox::vector_size_t idx,
            std::string& key);

}  // namespace sdb::connector::primary_key
