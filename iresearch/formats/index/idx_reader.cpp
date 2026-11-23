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
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/search/detail/ann_index.hpp"

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
  _file = fs.OpenInputFile(fs.JoinPath(root, filename),
                           duckdb::MappedFileAccess::RANDOM);

  IdxFooter footer;
  format_utils::ReadFooter(
    *_file, filename, [&](duckdb::BinaryDeserializer& in, uint64_t data_size) {
#ifndef IRESEARCH_NO_VECTOR
      in.ReadOptionalList(
        kIdxFieldIvf, "ivf",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            footer.ivf.push_back({
              .id = obj.ReadProperty<uint64_t>(0, "id"),
              .meta =
                {
                  .tree_offset = obj.ReadProperty<uint64_t>(1, "tree_offset"),
                  .stats_offset = obj.ReadProperty<uint64_t>(3, "stats_offset"),
                  .root_byte_size =
                    obj.ReadProperty<uint64_t>(5, "root_byte_size"),
                },
            });
          });
        });
      in.ReadOptionalList(
        kIdxFieldHnsw, "hnsw",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            footer.hnsw.push_back({
              .id = obj.ReadProperty<uint64_t>(0, "id"),
              .meta =
                {
                  .offset = obj.ReadProperty<uint64_t>(1, "offset"),
                  .header =
                    {
                      .d = obj.ReadProperty<uint32_t>(3, "d"),
                      .metric = static_cast<VectorMetric>(
                        obj.ReadProperty<uint8_t>(4, "metric")),
                      .quant = static_cast<VectorQuantization>(
                        obj.ReadProperty<uint8_t>(5, "quant")),
                      .ef_construction =
                        obj.ReadProperty<uint32_t>(6, "ef_construction"),
                      .record_size =
                        obj.ReadProperty<uint32_t>(7, "record_size"),
                      .rows = obj.ReadProperty<uint64_t>(8, "rows"),
                      .rotated = obj.ReadProperty<bool>(9, "rotated"),
                    },
                },
            });
          });
        });
#endif
      in.ReadOptionalObject(
        kIdxFieldTerms, "terms", [&](duckdb::BinaryDeserializer& terms) {
          footer.terms = term_dict::ReadTerms(terms, data_size);
        });
      footer.norms = ReadNormColumns(in, kIdxFieldNorms, data_size);
    });
  Adopt(std::move(footer), hnsw_bases);
}

IdxReader::IdxReader(duckdb::shared_ptr<duckdb::InputFile> file,
                     IdxFooter footer, HnswBases* hnsw_bases)
  : _file{std::move(file)} {
  Adopt(std::move(footer), hnsw_bases);
}

void IdxReader::Adopt(IdxFooter footer, HnswBases* hnsw_bases) {
#ifndef IRESEARCH_NO_VECTOR
  for (const auto& [id, meta] : footer.ivf) {
    duckdb::InputFileReader body{*_file};
    body.Seek(meta.tree_offset);
    auto entry = CentroidsTree::Deserialize(body, meta.root_byte_size);
    entry.SetQuantStatsOffset(meta.stats_offset);
    _anns.emplace(id, std::make_unique<IvfIndex>(std::move(entry), *_file));
  }
  for (const auto& [id, meta] : footer.hnsw) {
    _anns.emplace(id,
                  std::make_unique<HnswIndex>(meta, id, hnsw_bases, *_file));
  }
#else
  SDB_ASSERT(footer.ivf.empty() && footer.hnsw.empty());
#endif
  _terms = std::move(footer.terms);
  _norms = NormColumns{_file, footer.norms};
}

IdxReader::~IdxReader() = default;

const AnnIndex* IdxReader::Ann(field_id id) const noexcept {
  const auto it = _anns.find(id);
  return it == _anns.end() ? nullptr : it->second.get();
}

}  // namespace irs
