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
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/serialization_traits.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <memory>
#include <string_view>
#include <utility>

#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/bytes_utils.hpp"  // bstring
#include "iresearch/utils/compression.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/type_limits.hpp"  // field_limits

namespace irs {

struct AnnIndex;

struct IvfCentroidMeta {
  uint64_t tree_offset = 0;
  uint64_t stats_offset = 0;
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

struct HnswGraphFrames {
  compression::Frames levels;
  compression::Frames base;
  compression::Frames upper;

  bool Packed() const noexcept { return !levels.sizes.empty(); }
};

struct HnswMeta {
  uint64_t offset = 0;
  HnswHeader header;
  HnswGraphFrames graph_compression;
};

inline constexpr std::string_view kIdxFormatExt = "idx";

inline constexpr duckdb::field_id_t kIdxFieldIvf = 1;
inline constexpr duckdb::field_id_t kIdxFieldHnsw = 2;
inline constexpr duckdb::field_id_t kIdxFieldTerms = 3;

inline constexpr duckdb::field_id_t kHnswMetaGraphCompression = 10;
inline constexpr duckdb::field_id_t kHnswGraphLevels = 0;
inline constexpr duckdb::field_id_t kHnswGraphBase = 1;
inline constexpr duckdb::field_id_t kHnswGraphUpper = 2;

struct IvfCentroidEntry {
  field_id id;
  IvfCentroidMeta meta;
};

struct HnswEntry {
  field_id id;
  HnswMeta meta;
};

struct IdxFooter {
  std::vector<IvfCentroidEntry> ivf;
  std::vector<HnswEntry> hnsw;
  term_dict::TermsMeta terms;
};

class IdxReader final {
 public:
  IdxReader(duckdb::FileSystem& fs, std::string_view root,
            std::string_view segment_name, FileSizes sizes,
            HnswBases* hnsw_bases);
  IdxReader(duckdb::shared_ptr<duckdb::InputFile> file, IdxFooter footer,
            HnswBases* hnsw_bases);
  ~IdxReader();

  IdxReader(const IdxReader&) = delete;
  IdxReader& operator=(const IdxReader&) = delete;

  const AnnIndex* Ann(field_id id) const noexcept;

  term_dict::TermsMeta TakeTerms() noexcept {
    return std::exchange(_terms, {});
  }

  const duckdb::InputFile* File() const noexcept { return _file.get(); }

 private:
  void Adopt(IdxFooter footer, HnswBases* hnsw_bases);

  duckdb::shared_ptr<duckdb::InputFile> _file;
  containers::FlatHashMap<field_id, std::unique_ptr<AnnIndex>> _anns;
  term_dict::TermsMeta _terms;
};

}  // namespace irs
