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

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types.hpp>
#include <utility>
#include <vector>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

struct HnswEntry {
  field_id id;
  HnswMeta meta;
};

struct IvfCentroidEntry {
  field_id id;
  IvfCentroidMeta meta;
};

struct IdxWriter::Impl {
  Directory* dir;
  std::string filename;
  IndexOutput::ptr out;
  std::vector<IvfCentroidEntry> ivf_entries;
  std::vector<HnswEntry> hnsw_entries;
  TermsHead terms;
};

IdxWriter::IdxWriter(Directory& dir, std::string_view segment_name)
  : _impl{std::make_unique<Impl>()} {
  _impl->dir = &dir;
  _impl->filename = absl::StrCat(segment_name, ".", kIdxFormatExt);
}

IdxWriter::~IdxWriter() {
  if (_impl && _impl->out) {
    Rollback();
  }
}

void IdxWriter::EnsureOut() {
  if (_impl->out) {
    return;
  }
  _impl->out = _impl->dir->create(_impl->filename);
}

IndexOutput& IdxWriter::BlocksOut() {
  EnsureOut();
  return *_impl->out;
}

void IdxWriter::AddIvf(field_id id, IvfCentroidMeta meta) {
  _impl->ivf_entries.push_back(
    IvfCentroidEntry{.id = id, .meta = std::move(meta)});
}

void IdxWriter::AddHnsw(field_id id, HnswMeta meta) {
  _impl->hnsw_entries.push_back(HnswEntry{.id = id, .meta = std::move(meta)});
}

void IdxWriter::SetTerms(const TermsHead& head) noexcept {
  _impl->terms = head;
}

bool IdxWriter::Empty() const noexcept {
  return _impl->ivf_entries.empty() && _impl->hnsw_entries.empty() &&
         _impl->terms.size == 0;
}

void IdxWriter::Commit() {
  if (Empty() && !_impl->out) {
    return;
  }

  EnsureOut();

  format_utils::WriteFooter(*_impl->out, [&](duckdb::BinarySerializer& footer) {
    if (!_impl->ivf_entries.empty()) {
      footer.WriteList(
        kIdxFieldIvf, "ivf", _impl->ivf_entries.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          const auto& e = _impl->ivf_entries[i];
          list.WriteObject([&](duckdb::BinarySerializer& obj) {
            obj.WriteProperty<uint64_t>(0, "id", e.id);
            obj.WriteProperty<uint64_t>(1, "tree_offset", e.meta.tree_offset);
            obj.WriteProperty<uint64_t>(2, "tree_byte_size",
                                        e.meta.tree_byte_size);
            obj.WriteProperty<uint64_t>(3, "stats_offset", e.meta.stats_offset);
            obj.WriteProperty<uint64_t>(4, "stats_byte_size",
                                        e.meta.stats_byte_size);
          });
        });
    }
    if (!_impl->hnsw_entries.empty()) {
      footer.WriteList(
        kIdxFieldHnsw, "hnsw", _impl->hnsw_entries.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          const auto& e = _impl->hnsw_entries[i];
          list.WriteObject([&](duckdb::BinarySerializer& obj) {
            obj.WriteProperty<uint64_t>(0, "id", e.id);
            obj.WriteProperty<uint64_t>(1, "offset", e.meta.offset);
            obj.WriteProperty<uint64_t>(2, "byte_size", e.meta.byte_size);
          });
        });
    }
    if (const auto& terms = _impl->terms; terms.size != 0) {
      footer.WriteProperty<uint64_t>(kIdxFieldTermsHeadOffset,
                                     "terms_head_offset", terms.offset);
      footer.WriteProperty<uint64_t>(kIdxFieldTermsHeadSize, "terms_head_size",
                                     terms.size);
      footer.WriteProperty<uint32_t>(kIdxFieldTermsHeadCrc32c,
                                     "terms_head_crc32c", terms.crc32c);
    }
  });
  _impl->out.reset();
}

void IdxWriter::Rollback() noexcept {
  _impl->out.reset();
  _impl->ivf_entries.clear();
  _impl->hnsw_entries.clear();
  _impl->terms = {};
}

}  // namespace irs
