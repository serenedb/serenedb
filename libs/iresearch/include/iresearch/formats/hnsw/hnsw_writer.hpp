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

#include <faiss/impl/HNSW.h>

#include <memory>
#include <utility>

#include "iresearch/formats/hnsw/hnsw_reader.hpp"  // ChunkedVectorCache, ReadHNSW
#include "iresearch/index/column_info.hpp"

namespace irs {

class DataOutput;
class ReadContext;

void WriteHNSW(DataOutput& out, const faiss::HNSW& hnsw);

class HnswWriter final {
 public:
  explicit HnswWriter(HNSWInfo info);
  ~HnswWriter();

  HnswWriter(const HnswWriter&) = delete;
  HnswWriter& operator=(const HnswWriter&) = delete;

  void Build(const ColumnReader& vector_column, ReadContext& ctx);

  void Serialize(DataOutput& out);

  const HNSWInfo& Info() const noexcept { return _info; }

  const std::shared_ptr<faiss::HNSW>& Graph() const noexcept { return _hnsw; }

 private:
  HNSWInfo _info;
  std::shared_ptr<faiss::HNSW> _hnsw;
};

}  // namespace irs
