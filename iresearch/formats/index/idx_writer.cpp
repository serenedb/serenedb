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

#include "iresearch/formats/index/idx_writer.hpp"

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types.hpp>
#include <utility>
#include <vector>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/file_output.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

IdxWriter::IdxWriter(duckdb::FileSystem& fs, std::string_view root,
                     const FileObservers& observers,
                     std::string_view segment_name)
  : _fs{fs},
    _root{root},
    _observers{observers},
    _filename{FileName(segment_name, kIdxFormatExt)} {}

void IdxWriter::EnsureOut() {
  if (_out) {
    return;
  }
  _out = FileOutput::Open(_fs, _root, _filename, _observers, true);
}

DataOutput& IdxWriter::BlocksOut() {
  EnsureOut();
  return *_out;
}

void IdxWriter::AddIvf(field_id id, IvfCentroidMeta meta) {
  _ivf_entries.push_back(IvfCentroidEntry{.id = id, .meta = std::move(meta)});
}

void IdxWriter::AddHnsw(field_id id, HnswMeta meta) {
  _hnsw_entries.push_back(HnswEntry{.id = id, .meta = std::move(meta)});
}

void IdxWriter::SetTerms(term_dict::TermsMeta terms) noexcept {
  _terms = std::move(terms);
}

bool IdxWriter::Empty() const noexcept {
  return _ivf_entries.empty() && _hnsw_entries.empty() && _terms.fields.empty();
}

void IdxWriter::Commit() {
  if (Empty() && !_out) {
    return;
  }

  EnsureOut();

  format_utils::WriteFooter(*_out, [&](duckdb::BinarySerializer& footer) {
    if (!_ivf_entries.empty()) {
      footer.WriteList(
        kIdxFieldIvf, "ivf", _ivf_entries.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          const auto& e = _ivf_entries[i];
          list.WriteObject([&](duckdb::BinarySerializer& obj) {
            obj.WriteProperty<uint64_t>(0, "id", e.id);
            obj.WriteProperty<uint64_t>(1, "tree_offset", e.meta.tree_offset);
            obj.WriteProperty<uint64_t>(2, "tree_byte_size",
                                        e.meta.tree_byte_size);
            obj.WriteProperty<uint64_t>(3, "stats_offset", e.meta.stats_offset);
            obj.WriteProperty<uint64_t>(4, "stats_byte_size",
                                        e.meta.stats_byte_size);
            obj.WriteProperty<uint64_t>(5, "root_byte_size",
                                        e.meta.root_byte_size);
          });
        });
    }
    if (!_hnsw_entries.empty()) {
      footer.WriteList(
        kIdxFieldHnsw, "hnsw", _hnsw_entries.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          const auto& e = _hnsw_entries[i];
          list.WriteObject([&](duckdb::BinarySerializer& obj) {
            obj.WriteProperty<uint64_t>(0, "id", e.id);
            obj.WriteProperty<uint64_t>(1, "offset", e.meta.offset);
            obj.WriteProperty<uint64_t>(2, "byte_size", e.meta.byte_size);
            const auto& h = e.meta.header;
            obj.WriteProperty<uint32_t>(3, "d", h.d);
            obj.WriteProperty<uint8_t>(4, "metric",
                                       static_cast<uint8_t>(h.metric));
            obj.WriteProperty<uint8_t>(5, "quant",
                                       static_cast<uint8_t>(h.quant));
            obj.WriteProperty<uint32_t>(6, "ef_construction",
                                        h.ef_construction);
            obj.WriteProperty<uint32_t>(7, "record_size", h.record_size);
            obj.WriteProperty<uint64_t>(8, "rows", h.rows);
            obj.WriteProperty<bool>(9, "rotated", h.rotated);
          });
        });
    }
    if (!_terms.fields.empty()) {
      footer.WriteObject(kIdxFieldTerms, "terms",
                         [&](duckdb::BinarySerializer& terms) {
                           term_dict::WriteTerms(terms, _terms);
                         });
    }
  });
  _out->Close();
  _out.reset();
}

}  // namespace irs
