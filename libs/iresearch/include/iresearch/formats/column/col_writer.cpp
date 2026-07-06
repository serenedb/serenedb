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

#include "iresearch/formats/column/col_writer.hpp"

#include <absl/strings/str_cat.h>

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/database.hpp>
#include <optional>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/serialization.h"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/internal/persistent_column_data.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"

namespace irs {
namespace {

// Footer slot ids; stable, never reuse. Must match col_reader.cpp.
constexpr duckdb::field_id_t kFooterSlotColumns = 100;
constexpr duckdb::field_id_t kFooterSlotNormColumns = 101;

void SerializeColumnData(duckdb::Serializer& obj,
                         const PersistentColumnData& node) {
  obj.WriteProperty(0, "type", node.type);
  obj.WriteList(1, "data", node.pointers.size(),
                [&](duckdb::Serializer::List& plist, duckdb::idx_t j) {
                  plist.WriteObject([&](duckdb::Serializer& p) {
                    node.pointers[j].Serialize(p);
                  });
                });
  obj.WriteList(2, "validity", node.validity_pointers.size(),
                [&](duckdb::Serializer::List& plist, duckdb::idx_t j) {
                  plist.WriteObject([&](duckdb::Serializer& p) {
                    node.validity_pointers[j].Serialize(p);
                  });
                });
  obj.WriteList(3, "child_columns", node.child_columns.size(),
                [&](duckdb::Serializer::List& clist, duckdb::idx_t j) {
                  clist.WriteObject([&](duckdb::Serializer& child) {
                    SerializeColumnData(child, node.child_columns[j]);
                  });
                });
  obj.WriteList(4, "variant_layouts", node.variant_layouts.size(),
                [&](duckdb::Serializer::List& vlist, duckdb::idx_t j) {
                  const auto& l = node.variant_layouts[j];
                  vlist.WriteObject([&](duckdb::Serializer& vo) {
                    vo.WriteProperty<uint64_t>(0, "row_start", l.row_start);
                    vo.WriteProperty<uint64_t>(1, "row_count", l.row_count);
                    vo.WriteProperty<uint8_t>(
                      2, "shred_state", static_cast<uint8_t>(l.shred_state));
                    vo.WriteObject(3, "unshredded", [&](duckdb::Serializer& u) {
                      SerializeColumnData(u, *l.unshredded);
                    });
                    if (l.shred_state != VariantShredState::Unshredded) {
                      vo.WriteObject(4, "shredded_node",
                                     [&](duckdb::Serializer& s) {
                                       SerializeColumnData(s, *l.shredded_node);
                                     });
                    }
                  });
                });
  obj.WritePropertyWithDefault<bool>(5, "fully_shredded", node.fully_shredded,
                                     true);
  obj.WritePropertyWithDefault<duckdb::shared_ptr<duckdb::HyperLogLog>>(
    6, "hyperloglog", node.hyperloglog);
}

}  // namespace

struct ColWriter::Impl {
  Directory* dir;
  std::string filename;
  duckdb::DatabaseInstance* db;
  const IndexFieldOptions* field_options = nullptr;
  IndexOutput::ptr out;
  std::unique_ptr<WriteContext> write_ctx;
  std::vector<std::unique_ptr<ColumnWriter>> column_writers;
  sdb::containers::FlatHashMap<field_id, ColumnWriter*> column_by_id;
  std::vector<std::unique_ptr<FooterColumnEntry>> column_entries;
  sdb::containers::FlatHashMap<field_id, FooterColumnEntry*>
    column_entries_by_id;
  std::vector<std::unique_ptr<NormColumnWriter>> norm_writers;
  sdb::containers::FlatHashMap<field_id, NormColumnWriter*> norm_by_id;
  std::unique_ptr<IvfWriter> ivf;
  bool has_ivf_column = false;
};

ColWriter::ColWriter(Directory& dir, std::string_view segment_name,
                     duckdb::DatabaseInstance& db)
  : _impl{std::make_unique<Impl>()} {
  _impl->dir = &dir;
  _impl->db = &db;
  _impl->filename = absl::StrCat(segment_name, ".", kColFormatExt);
}

void ColWriter::SetFieldOptions(
  const IndexFieldOptions* field_options) noexcept {
  // Set once on open, then only re-pointed to an equal view on resume; opened
  // columns already encode with the current options.
  SDB_ASSERT(_impl->field_options == nullptr ||
               CompatibleFieldOptions(_impl->field_options, field_options),
             "ColWriter::SetFieldOptions: encodings differ mid-segment");
  _impl->field_options = field_options;
}

void ColWriter::EnsureOut() {
  if (_impl->out) {
    return;
  }
  _impl->out = _impl->dir->create(_impl->filename);
  if (!_impl->out) {
    throw IoError{
      absl::StrCat("failed to create .col writer file: ", _impl->filename)};
  }
  format_utils::WriteHeader(*_impl->out, kColFormatName, kColFormatVersion);
  _impl->write_ctx = std::make_unique<WriteContext>(*_impl->db, *_impl->out);
}

bool ColWriter::Empty() const noexcept {
  return _impl->column_writers.empty() && _impl->norm_writers.empty();
}

ColWriter::~ColWriter() {
  if (_impl && _impl->out) {
    Rollback();
  }
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type) {
  ColumnOptions opts{};
  if (_impl->field_options) {
    opts = _impl->field_options->GetColumnOptions(id);
  }
  if (opts.ivf_info) {
    _impl->has_ivf_column = true;
  }
  auto& cw =
    OpenColumn(id, std::move(type), opts.skip_validity, opts.row_group_size,
               opts.compression, opts.hyperloglog);
  return cw;
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type,
                                    bool skip_validity, uint32_t row_group_size,
                                    duckdb::CompressionType compression,
                                    bool hyperloglog) {
  SDB_ASSERT(row_group_size != 0);
  // Per-batch SearchSink may re-open the same id; return the existing
  // writer so batches accumulate into one footer entry.
  if (auto it = _impl->column_by_id.find(id); it != _impl->column_by_id.end()) {
    auto& existing = *it->second;
    SDB_ASSERT(
      existing.Type() == type && existing.RowGroupSize() == row_group_size &&
        existing.SkipValidity() == skip_validity &&
        existing.Compression() == compression &&
        existing.HasHyperLogLog() == hyperloglog,
      "ColWriter::OpenColumn: re-opened id ", id,
      " with mismatched settings (type ", type.ToString(), " vs ",
      existing.Type().ToString(), ", row_group_size ", row_group_size, " vs ",
      existing.RowGroupSize(), ", skip_validity ", skip_validity, " vs ",
      existing.SkipValidity(), ", compression ",
      duckdb::CompressionTypeToString(compression), " vs ",
      duckdb::CompressionTypeToString(existing.Compression()), ", hyperloglog ",
      hyperloglog, " vs ", existing.HasHyperLogLog(), ")");
    return existing;
  }
  EnsureOut();
  auto entry = std::make_unique<FooterColumnEntry>();
  entry->id = id;
  entry->root.type = type;
  auto* entry_ptr = entry.get();
  auto cw =
    std::make_unique<ColumnWriter>(id, type, row_group_size, *_impl->write_ctx,
                                   *entry, skip_validity, hyperloglog);
  cw->SetCompression(compression);
  _impl->column_entries.push_back(std::move(entry));
  _impl->column_entries_by_id.emplace(id, entry_ptr);
  auto& back = *_impl->column_writers.emplace_back(std::move(cw));
  _impl->column_by_id.emplace(id, &back);
  return back;
}

void ColWriter::NoteIvfColumn() noexcept { _impl->has_ivf_column = true; }

std::unique_ptr<IvfWriter> ColWriter::TakeIvf() noexcept {
  return std::move(_impl->ivf);
}

std::unique_ptr<ColumnReader> ColWriter::ReopenColumn(field_id id) const {
  auto it = _impl->column_entries_by_id.find(id);
  if (it == _impl->column_entries_by_id.end()) {
    return nullptr;
  }
  return MakeColumnReader(it->second->id, Clone(it->second->root));
}

std::span<const std::unique_ptr<NormColumnWriter>> ColWriter::NormWriters()
  const noexcept {
  return _impl->norm_writers;
}

NormColumnWriter* ColWriter::OpenNormColumn(field_id id) {
  if (!_impl->field_options) {
    return nullptr;
  }
  const auto opts = _impl->field_options->GetNormColumnOptions(id);
  if (!field_limits::valid(opts.id)) {
    return nullptr;
  }
  return &OpenNormColumn(opts.id, opts.row_group_size);
}

NormColumnWriter& ColWriter::OpenNormColumn(field_id id,
                                            uint32_t row_group_size) {
  if (auto it = _impl->norm_by_id.find(id); it != _impl->norm_by_id.end()) {
    return *it->second;
  }
  EnsureOut();
  auto cw = std::make_unique<NormColumnWriter>(id, row_group_size, *_impl->out);
  auto& back = *_impl->norm_writers.emplace_back(std::move(cw));
  _impl->norm_by_id.emplace(id, &back);
  return back;
}

void ColWriter::Commit(uint64_t target_row, IdxWriter* idx) {
  if (Empty() && !_impl->out) {
    return;
  }
  for (auto& cw : _impl->column_writers) {
    cw->Finalize();
  }
  for (auto& nw : _impl->norm_writers) {
    nw->PadTo(target_row);
    nw->Finalize();
  }
  if (_impl->has_ivf_column) {
    SDB_ASSERT(idx,
               "ColWriter::Commit requires an IdxWriter when an IVF "
               "column is present");
    _impl->out->Flush();
    auto in = _impl->dir->open(_impl->filename, IOAdvice::RANDOM);
    if (!in) {
      throw IoError{
        absl::StrCat("failed to open .col writer for derived index build: ",
                     _impl->filename)};
    }
    ReadContext ctx{*_impl->db, std::move(in)};
    _impl->ivf = std::make_unique<IvfWriter>();
    for (const auto& e : _impl->column_entries) {
      const auto opts = _impl->field_options->GetColumnOptions(e->id);
      if (!opts.ivf_info) {
        continue;
      }
      auto col = ReopenColumn(e->id);
      if (!col) {
        continue;
      }
      _impl->ivf->BuildColumn(std::move(col), ctx, *idx, *opts.ivf_info);
    }
  }

  const uint64_t footer_offset = _impl->out->Position();

  duckdb::BinarySerializer serializer{*_impl->out,
                                      duckdb::VersionStorageOptions()};
  serializer.Begin();
  serializer.WriteList(
    kFooterSlotColumns, "columns", _impl->column_entries.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& e = *_impl->column_entries[i];
      list.WriteObject([&](duckdb::Serializer& obj) {
        obj.WriteProperty<uint64_t>(0, "id", e.id);
        obj.WriteObject(1, "root", [&](duckdb::Serializer& root_obj) {
          SerializeColumnData(root_obj, e.root);
        });
      });
    });
  serializer.WriteList(
    kFooterSlotNormColumns, "norm_columns", _impl->norm_writers.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& nw = *_impl->norm_writers[i];
      SDB_ASSERT(!nw.Pointers().empty());
      list.WriteObject([&](duckdb::Serializer& obj) {
        obj.WriteProperty<uint64_t>(0, "id", nw.Id());
        obj.WriteList(
          1, "row_groups", nw.Pointers().size(),
          [&](duckdb::Serializer::List& plist, duckdb::idx_t j) {
            const auto& p = nw.Pointers()[j];
            plist.WriteObject([&](duckdb::Serializer& pe) {
              pe.WriteProperty<uint8_t>(0, "byte_size", p.byte_size);
              pe.WriteProperty<uint32_t>(1, "row_count", p.row_count);
              pe.WriteProperty<uint32_t>(2, "max", p.max);
              pe.WriteProperty<uint64_t>(3, "sum", p.sum);
              pe.WriteProperty<uint64_t>(4, "non_zero_count", p.non_zero_count);
              pe.WriteProperty<uint64_t>(5, "file_offset", p.file_offset);
            });
          });
      });
    });
  serializer.End();

  _impl->out->WriteU64(footer_offset);
  format_utils::WriteFooter(*_impl->out);
  _impl->out.reset();
}

void ColWriter::Rollback() noexcept { _impl->out.reset(); }

}  // namespace irs
