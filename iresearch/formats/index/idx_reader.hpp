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

#include <cstdint>
#include <duckdb/common/serializer/serialization_traits.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_input.hpp"  // IndexInput
#include "iresearch/types.hpp"
#include "iresearch/utils/bytes_utils.hpp"  // bstring
#include "iresearch/utils/type_limits.hpp"  // field_limits

namespace irs {

struct Directory;
struct AnnIndex;
struct IResourceManager;

struct IvfCentroidMeta {
  uint64_t tree_offset = 0;
  uint64_t tree_byte_size = 0;
  uint64_t stats_offset = 0;
  uint64_t stats_byte_size = 0;
  uint64_t root_byte_size = 0;
};

struct HnswHeader {
  uint32_t d = 0;
  VectorMetric metric = VectorMetric::L2Sqr;
  VectorQuantization quant = VectorQuantization::None;
  uint32_t ef_construction = 0;
  uint32_t record_size = 0;
  uint64_t rows = 0;
  bool rotated = false;
};

struct HnswMeta {
  uint64_t offset = 0;
  uint64_t byte_size = 0;
  HnswHeader header;
};

inline constexpr std::string_view kIdxFormatExt = "idx";

inline constexpr duckdb::field_id_t kIdxFieldIvf = 1;
inline constexpr duckdb::field_id_t kIdxFieldHnsw = 2;
inline constexpr duckdb::field_id_t kIdxFieldTerms = 3;

class IdxReader final {
 public:
  IdxReader(const Directory& dir, std::string_view segment_name,
            HnswBases* hnsw_bases, bool keep_terms = true);
  ~IdxReader();

  IdxReader(const IdxReader&) = delete;
  IdxReader& operator=(const IdxReader&) = delete;

  bool HasAnn(field_id id) const noexcept;
  const AnnIndex* Ann(field_id id) const noexcept;

  term_dict::TermsMeta TakeTerms() noexcept;

  std::span<const uint64_t> AnnEnds() const noexcept;

  IndexInput::ptr ReopenIn() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> _impl;
};

}  // namespace irs
