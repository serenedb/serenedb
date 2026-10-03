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

#include "iresearch/formats/column/nrm_writer.hpp"

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <utility>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/column/nrm_reader.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {
namespace {

void SerializeNormColumn(duckdb::BinarySerializer& s,
                         const NormColumnWriter& nw, uint64_t base) {
  s.WriteProperty(0, "id", static_cast<uint64_t>(nw.Id()));
  s.WriteProperty(1, "row_group_size", nw.RowGroupSize());
  s.WriteProperty(2, "row_count", nw.RowCount());
  const auto& ptrs = nw.Pointers();
  s.WriteList(3, "row_groups", ptrs.size(),
              [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                const auto& p = ptrs[i];
                list.WriteObject([&](duckdb::BinarySerializer& obj) {
                  obj.WriteProperty(0, "byte_size", p.byte_size);
                  obj.WriteProperty(1, "max", p.max);
                  obj.WriteProperty(2, "sum", p.sum);
                  obj.WriteProperty(3, "non_zero_count", p.non_zero_count);
                  obj.WriteProperty(4, "file_offset", base + p.file_offset);
                  obj.WritePropertyWithDefault<uint32_t>(5, "exceptions",
                                                         p.exceptions, 0);
                });
              });
}

}  // namespace

std::string NrmFileName(std::string_view segment_name) {
  return absl::StrCat(segment_name, ".", kNrmExt);
}

NrmWriter::Column::Column(field_id id, uint32_t row_group_size)
  : file{IResourceManager::gNoop},
    out{file},
    writer{id, row_group_size, out} {}

NrmWriter::NrmWriter(Directory& dir, std::string_view segment_name)
  : _dir{&dir}, _filename{NrmFileName(segment_name)} {}

NormColumnWriter& NrmWriter::OpenNormColumn(field_id id,
                                            uint32_t row_group_size) {
  if (auto it = _by_id.find(id); it != _by_id.end()) {
    return *it->second;
  }
  auto& column =
    *_columns.emplace_back(std::make_unique<Column>(id, row_group_size));
  _by_id.emplace(id, &column.writer);
  return column.writer;
}

void NrmWriter::Commit(uint64_t target_row) {
  std::vector<std::pair<const NormColumnWriter*, uint64_t>> written;
  IndexOutput::ptr out;
  for (auto& column : _columns) {
    column->writer.PadTo(target_row);
    column->writer.Finalize();
    column->out.Flush();
    if (column->writer.Pointers().empty()) {
      continue;
    }
    if (!out) {
      out = _dir->create(_filename);
      if (!out) {
        throw IoError{
          absl::StrCat("nrm writer: cannot create .nrm file: ", _filename)};
      }
    }
    written.emplace_back(&column->writer, out->Position());
    column->file >> *out;
  }
  if (!out) {
    return;
  }
  format_utils::WriteFooter(*out, [&](duckdb::BinarySerializer& footer) {
    footer.WriteList(
      kNrmFieldColumns, "norm_columns", written.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          SerializeNormColumn(obj, *written[i].first, written[i].second);
        });
      });
  });
}

}  // namespace irs
