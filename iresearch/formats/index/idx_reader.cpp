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

#include <absl/strings/str_cat.h>

#include <cstring>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/types.hpp>
#include <utility>
#include <vector>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constexpr duckdb::field_id_t kFooterSlotTermDict = 100;
constexpr duckdb::field_id_t kFooterSlotIvf = 101;
constexpr duckdb::field_id_t kFooterSlotHnsw = 102;

}  // namespace

struct IdxReader::Impl {
  IndexInput::ptr in;
  std::vector<std::pair<field_id, std::unique_ptr<AnnIndex>>> ann_entries;
  irs::containers::FlatHashMap<field_id, size_t> ann_by_id;
  std::vector<std::pair<field_id, TermDictMeta>> term_dicts;
};

IdxReader::IdxReader(const Directory& dir, std::string_view segment_name)
  : _impl{std::make_unique<Impl>()} {
  const auto filename = absl::StrCat(segment_name, ".", kIdxFormatExt);
  bool exists = false;
  if (!dir.exists(exists, filename)) {
    throw IoError{
      absl::StrCat("Failed to check existence of file, path: ", filename)};
  }
  if (!exists) {
    return;
  }

  _impl->in = dir.open(filename, IOAdvice::SEQUENTIAL);
  if (!_impl->in) {
    throw IoError{absl::StrCat("Failed to open index file, path: ", filename)};
  }
  _impl->in->EnableReadahead();

  format_utils::ReadFooter(
    *_impl->in, filename, [&](duckdb::Deserializer& footer, uint64_t) {
      footer.ReadList(
        kFooterSlotTermDict, "term_dict",
        [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
          list.ReadObject([&](duckdb::Deserializer& obj) {
            TermDictMeta meta;
            const auto id = obj.ReadProperty<uint64_t>(0, "id");
            meta.features = static_cast<IndexFeatures>(
              obj.ReadProperty<uint32_t>(1, "features"));
            meta.term_count = obj.ReadProperty<uint64_t>(2, "term_count");
            meta.doc_count = obj.ReadProperty<uint64_t>(3, "doc_count");
            meta.total_doc_freq =
              obj.ReadProperty<uint64_t>(4, "total_doc_freq");
            meta.total_term_freq =
              obj.ReadProperty<uint64_t>(5, "total_term_freq");
            meta.has_score_bounds =
              obj.ReadProperty<bool>(6, "has_score_bounds");
            meta.body_offset = obj.ReadProperty<uint64_t>(7, "body_offset");
            meta.norm = obj.ReadPropertyWithExplicitDefault<uint64_t>(
              8, "norm", field_limits::invalid());
            _impl->term_dicts.emplace_back(id, std::move(meta));
          });
        });
      footer.ReadOptionalList(
        kFooterSlotIvf, "ivf",
        [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
          list.ReadObject([&](duckdb::Deserializer& obj) {
            const auto id = obj.ReadProperty<uint64_t>(0, "id");
            const auto tree_offset =
              obj.ReadProperty<uint64_t>(1, "tree_offset");
            const auto tree_byte_size =
              obj.ReadProperty<uint64_t>(2, "tree_byte_size");
            const auto stats_offset =
              obj.ReadProperty<uint64_t>(3, "stats_offset");
            const auto stats_byte_size =
              obj.ReadProperty<uint64_t>(4, "stats_byte_size");

            auto body = _impl->in->Dup();
            body->Seek(tree_offset);
            auto entry = CentroidsTree::Deserialize(*body, tree_byte_size);
            entry.SetQuantStatsLocation(stats_offset, stats_byte_size);

            const size_t idx = _impl->ann_entries.size();
            _impl->ann_entries.emplace_back(
              id, std::make_unique<IvfIndex>(std::move(entry)));
            _impl->ann_by_id.emplace(id, idx);
          });
        });
      footer.ReadOptionalList(
        kFooterSlotHnsw, "hnsw",
        [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
          list.ReadObject([&](duckdb::Deserializer& obj) {
            const auto id = obj.ReadProperty<uint64_t>(0, "id");
            const auto offset = obj.ReadProperty<uint64_t>(1, "offset");
            const auto byte_size = obj.ReadProperty<uint64_t>(2, "byte_size");

            auto body = _impl->in->Dup();
            body->Seek(offset);
            auto header = HnswIndex::ReadHeader(*body);

            const size_t idx = _impl->ann_entries.size();
            _impl->ann_entries.emplace_back(
              id,
              std::make_unique<HnswIndex>(
                header, HnswMeta{.offset = offset, .byte_size = byte_size}));
            _impl->ann_by_id.emplace(id, idx);
          });
        });
    });
}

IdxReader::~IdxReader() = default;

bool IdxReader::HasAnn(field_id id) const noexcept {
  return _impl->ann_by_id.contains(id);
}

const AnnIndex* IdxReader::Ann(field_id id) const noexcept {
  auto it = _impl->ann_by_id.find(id);
  return it == _impl->ann_by_id.end()
           ? nullptr
           : _impl->ann_entries[it->second].second.get();
}

std::span<const std::pair<field_id, TermDictMeta>> IdxReader::TermDicts()
  const noexcept {
  return _impl->term_dicts;
}

IndexInput::ptr IdxReader::ReopenIn() const {
  return _impl->in ? _impl->in->Reopen() : IndexInput::ptr{};
}

}  // namespace irs
