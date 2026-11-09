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

#include "iresearch/formats/index/idx_reader.hpp"

#include <cstring>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/types.hpp>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/search/detail/ann_index.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/storage.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

#ifndef IRESEARCH_NO_VECTOR
#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#endif

namespace irs {

IdxReader::IdxReader(duckdb::FileSystem& fs, std::string_view root,
                     std::string_view segment_name, bool present,
                     HnswBases* hnsw_bases) {
  if (!present) {
    return;
  }
  const auto filename = FileName(segment_name, kIdxFormatExt);
  _file =
    OpenInput(fs, PathOf(root, filename), duckdb::MappedFileAccess::RANDOM);

  format_utils::ReadFooter(
    *_file, filename,
    [&](duckdb::BinaryDeserializer& footer, uint64_t data_size) {
#ifndef IRESEARCH_NO_VECTOR
      footer.ReadOptionalList(
        kIdxFieldIvf, "ivf",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            const auto id = obj.ReadProperty<uint64_t>(0, "id");
            const auto tree_offset =
              obj.ReadProperty<uint64_t>(1, "tree_offset");
            const auto stats_offset =
              obj.ReadProperty<uint64_t>(3, "stats_offset");
            const auto root_byte_size =
              obj.ReadProperty<uint64_t>(5, "root_byte_size");

            IndexInput body{*_file};
            body.Seek(tree_offset);
            auto entry = CentroidsTree::Deserialize(body, root_byte_size);
            entry.SetQuantStatsOffset(stats_offset);

            _anns.emplace(id,
                          std::make_unique<IvfIndex>(std::move(entry), *_file));
          });
        });
      footer.ReadOptionalList(
        kIdxFieldHnsw, "hnsw",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            const auto id = obj.ReadProperty<uint64_t>(0, "id");
            const HnswMeta meta{
              .offset = obj.ReadProperty<uint64_t>(1, "offset"),
              .byte_size = obj.ReadProperty<uint64_t>(2, "byte_size"),
              .header =
                {
                  .d = obj.ReadProperty<uint32_t>(3, "d"),
                  .metric = static_cast<VectorMetric>(
                    obj.ReadProperty<uint8_t>(4, "metric")),
                  .quant = static_cast<VectorQuantization>(
                    obj.ReadProperty<uint8_t>(5, "quant")),
                  .ef_construction =
                    obj.ReadProperty<uint32_t>(6, "ef_construction"),
                  .record_size = obj.ReadProperty<uint32_t>(7, "record_size"),
                  .rows = obj.ReadProperty<uint64_t>(8, "rows"),
                  .rotated = obj.ReadProperty<bool>(9, "rotated"),
                },
            };

            _anns.emplace(
              id, std::make_unique<HnswIndex>(meta, id, hnsw_bases, *_file));
          });
        });
#endif
      footer.ReadOptionalObject(
        kIdxFieldTerms, "terms", [&](duckdb::BinaryDeserializer& terms) {
          _terms = term_dict::ReadTerms(terms, data_size);
        });
    });
}

IdxReader::~IdxReader() = default;

const AnnIndex* IdxReader::Ann(field_id id) const noexcept {
  const auto it = _anns.find(id);
  return it == _anns.end() ? nullptr : it->second.get();
}

}  // namespace irs
