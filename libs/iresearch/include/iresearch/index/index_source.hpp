////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>

namespace duckdb {

class ClientContext;
class Vector;

}  // namespace duckdb
namespace sdb::connector {

class IndexSource {
 public:
  virtual ~IndexSource() = default;

  // Materializes the source columns for the `count` primary keys held in `pk`
  // -- the stored, self-describing PK column read straight from the index: a
  // single scalar column, or a struct of key fields. Each source reads the
  // shape it wrote (integer fields are read regardless of signedness/width).
  // Returns the number of output rows produced -- equal to `count` unless a
  // pushed lookup-column filter compacted the batch to survivors.
  virtual duckdb::idx_t Materialize(duckdb::ClientContext& context,
                                    duckdb::Vector& pk, duckdb::idx_t count,
                                    duckdb::DataChunk& output) = 0;
};

}  // namespace sdb::connector
