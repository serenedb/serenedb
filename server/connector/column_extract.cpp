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

#include "connector/column_extract.h"

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>

#include "basics/assert.h"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/formats/column/variant_column_reader.hpp"

namespace sdb::connector {
namespace {

const duckdb::LogicalType& VariantType() {
  static const auto kType = duckdb::LogicalType::VARIANT();
  return kType;
}

size_t FindStructFieldIndex(const duckdb::LogicalType& struct_type,
                            std::string_view field) {
  const auto& fields = duckdb::StructType::GetChildTypes(struct_type);
  for (size_t i = 0; i < fields.size(); ++i) {
    if (duckdb::StringUtil::CIEquals(field,
                                     fields[i].first.GetIdentifierName())) {
      return i;
    }
  }
  return fields.size();
}

duckdb::Vector& Scratch(
  const duckdb::LogicalType& type,
  std::unique_ptr<irs::ColumnReader::VectorScratch>& slot) {
  if (!slot) {
    slot = std::make_unique<irs::ColumnReader::VectorScratch>(type);
  }
  return slot->Reset();
}

duckdb::Vector& GatherScratch(
  const duckdb::LogicalType& type,
  std::unique_ptr<irs::ColumnReader::VectorScratch>& slot) {
  auto& scratch = Scratch(type, slot);
  duckdb::FlatVector::ValidityMutable(scratch).Reset();
  return scratch;
}

void SetAllNull(duckdb::Vector& out_vec, duckdb::idx_t at, duckdb::idx_t n) {
  auto& validity = duckdb::FlatVector::ValidityMutable(out_vec);
  if (at == 0) {
    validity.SetAllInvalid(n);
    return;
  }
  validity.EnsureWritable();
  for (duckdb::idx_t i = 0; i < n; ++i) {
    validity.SetInvalidUnsafe(at + i);
  }
}

}  // namespace

void ExtractBinding::Bind(const irs::ColumnReader& column,
                          irs::ReadContext& ctx,
                          std::span<const std::string_view> path,
                          const duckdb::LogicalType& scan_type,
                          duckdb::ClientContext* context) {
  _reader = &column;
  _state = std::make_unique<irs::ColumnReader::ScanState>(column.InitScan(ctx));
  _extract_path = path;
  _extract_scan_type = scan_type;
  _context = context;
  Resolve();
}

void ExtractBinding::Resolve() {
  const auto* leaf = _reader;
  auto* leaf_st = _state.get();
  for (size_t pi = 0; pi < _extract_path.size(); ++pi) {
    if (leaf->Type().id() == duckdb::LogicalTypeId::VARIANT) {
      _extract_kind = ExtractKind::Variant;
      _variant_skip = pi;
      _leaf_reader = leaf;
      _leaf_state = leaf_st;
      return;
    }
    if (leaf->Type().id() != duckdb::LogicalTypeId::STRUCT) {
      _extract_kind = ExtractKind::AllNull;
      return;
    }
    const size_t idx = FindStructFieldIndex(leaf->Type(), _extract_path[pi]);
    if (idx >= leaf->StructFieldCount()) {
      _extract_kind = ExtractKind::AllNull;
      return;
    }
    leaf = &leaf->StructField(idx);
    leaf_st = &leaf_st->child_states[idx + 1];
  }
  _leaf_reader = leaf;
  _leaf_state = leaf_st;
  if (leaf->Type().id() == duckdb::LogicalTypeId::VARIANT) {
    _extract_kind = ExtractKind::Variant;
    _variant_skip = _extract_path.size();
  } else {
    _extract_kind = ExtractKind::ScalarLeaf;
    _leaf_scalar = !leaf->Type().IsNested();
  }
}

const duckdb::vector<duckdb::VariantPathComponent>& ExtractBinding::Components()
  const {
  if (!_components_built) {
    _components.reserve(_extract_path.size());
    for (const auto& field : _extract_path) {
      _components.emplace_back(std::string{field});
    }
    _components_built = true;
  }
  return _components;
}

const duckdb::vector<duckdb::VariantPathComponent>&
ExtractBinding::ComponentsSuffix(size_t skip) const {
  const auto& all = Components();
  if (skip == 0) {
    return all;
  }
  if (skip >= all.size()) {
    static const duckdb::vector<duckdb::VariantPathComponent> kEmpty;
    return kEmpty;
  }
  if (!_components_suffix_built || _components_suffix_skip != skip) {
    _components_suffix.assign(all.begin() + static_cast<std::ptrdiff_t>(skip),
                              all.end());
    _components_suffix_skip = skip;
    _components_suffix_built = true;
  }
  return _components_suffix;
}

void ExtractBinding::MaterializeRows(DocRows rows, duckdb::Vector& out_vec,
                                     duckdb::idx_t at, bool entire) const {
  SDB_ASSERT(_context);
  ExtractPath(rows, out_vec, at, /*contiguous=*/false,
              /*entire=*/entire && at == 0);
}

void ExtractBinding::MaterializeContiguous(uint64_t start_row,
                                           duckdb::idx_t count,
                                           duckdb::Vector& out_vec) const {
  SDB_ASSERT(_context);
  ExtractPath(irs::IotaRange{start_row, count}, out_vec, 0,
              /*contiguous=*/true, /*entire=*/true);
}

template<typename Rows>
void ExtractBinding::ExtractPath(const Rows& rows, duckdb::Vector& out_vec,
                                 duckdb::idx_t at, bool contiguous,
                                 bool entire) const {
  const auto n = static_cast<duckdb::idx_t>(rows.size());
  SDB_ASSERT(n <= STANDARD_VECTOR_SIZE);
  if (n == 0) {
    return;
  }
  switch (_extract_kind) {
    case ExtractKind::AllNull:
      SetAllNull(out_vec, at, n);
      return;
    case ExtractKind::Variant:
      VariantExtract(rows, out_vec, at, contiguous, entire);
      return;
    case ExtractKind::ScalarLeaf:
      ExtractLeaf(rows, out_vec, at, contiguous, entire);
      return;
  }
}

template<typename Rows>
void ExtractBinding::ExtractLeaf(const Rows& rows, duckdb::Vector& out_vec,
                                 duckdb::idx_t at, bool contiguous,
                                 bool entire) const {
  const auto n = static_cast<duckdb::idx_t>(rows.size());
  const auto& scan_type = _extract_scan_type;
  auto& leaf = *_leaf_reader;
  auto& leaf_state = *_leaf_state;
  const bool fastpath =
    contiguous && _leaf_scalar &&
    leaf.GatherCursor(leaf_state) == static_cast<uint64_t>(rows[0]);
  if (leaf.Type() == scan_type) {
    SDB_ASSERT(duckdb::FlatVector::Validity(out_vec).CheckAllValid(at + n, at),
               "columnstore extract leaf requires an all-valid target; "
               "validity codec AND-combines into out_vec");
    if (fastpath) {
      leaf.Scan(leaf_state, out_vec, n);
    } else {
      irs::column_internal::GatherRows(leaf, leaf_state, rows, out_vec, at,
                                       entire);
    }
    return;
  }
  auto& scratch = GatherScratch(leaf.Type(), _scratch);
  if (fastpath) {
    leaf.Scan(leaf_state, scratch, n);
  } else {
    irs::column_internal::GatherRows(leaf, leaf_state, rows, scratch, 0,
                                     /*whole_output=*/true);
  }
  PlaceCast(scratch, scan_type, out_vec, at, n, _casted, entire);
}

namespace {

DocRows Sub(DocRows rows, size_t i, size_t n) {
  return DocRows{rows.docs.subspan(i, n)};
}
irs::IotaRange Sub(irs::IotaRange rows, size_t i, size_t n) {
  return irs::IotaRange{rows.start + i, n};
}

template<typename Rows>
struct Shifted {
  Rows rows;
  uint64_t shift;
  size_t size() const noexcept { return rows.size(); }
  uint64_t operator[](size_t k) const noexcept {
    return static_cast<uint64_t>(rows[k]) - shift;
  }
};

}  // namespace

template<typename Rows>
void ExtractBinding::VariantExtract(const Rows& rows, duckdb::Vector& out_vec,
                                    duckdb::idx_t at, bool contiguous,
                                    bool entire) const {
  const auto n = static_cast<duckdb::idx_t>(rows.size());
  const auto& scan_type = _extract_scan_type;
  const bool variant_out = scan_type.id() == duckdb::LogicalTypeId::VARIANT;
  SDB_ASSERT(_leaf_reader->Type().id() == duckdb::LogicalTypeId::VARIANT);
  const auto& variant_col =
    static_cast<const irs::VariantColumnReader&>(*_leaf_reader);
  auto& variant_state = *_leaf_state;
  const size_t skip = _variant_skip;

  const auto remaining =
    _extract_path.subspan(std::min<size_t>(skip, _extract_path.size()));

  duckdb::LogicalType leaf_type;
  if (!variant_out && variant_col.CachedUniformShreddedLeaf(
                        variant_state, remaining, leaf_type)) {
    const bool direct = leaf_type == scan_type;
    duckdb::idx_t i = 0;
    while (i < n) {
      uint64_t rg_begin = 0;
      uint64_t rg_end = 0;
      const auto rg = variant_col.VariantRgSpan(static_cast<uint64_t>(rows[i]),
                                                rg_begin, rg_end);
      duckdb::idx_t j = i + 1;
      while (j < n && static_cast<uint64_t>(rows[j]) < rg_end) {
        ++j;
      }
      const auto glen = j - i;
      const auto out_off = at + i;
      const bool entire_group =
        (contiguous || entire) && out_off == 0 && glen == n;
      if (variant_col.RgLeafOk(variant_state, rg)) {
        const auto [leaf, leaf_scan] =
          variant_col.ShreddedLeafScan(variant_state, rg, remaining);
        const Shifted<Rows> local{Sub(rows, i, glen), rg_begin};
        if (direct) {
          irs::column_internal::GatherRows(*leaf, *leaf_scan, local, out_vec,
                                           out_off, entire_group);
        } else {
          auto& scratch = GatherScratch(leaf_type, _scratch);
          irs::column_internal::GatherRows(*leaf, *leaf_scan, local, scratch, 0,
                                           /*whole_output=*/true);
          PlaceCast(scratch, scan_type, out_vec, out_off, glen, _casted,
                    entire_group);
        }
      } else {
        auto& variant = GatherScratch(VariantType(), _variant);
        irs::column_internal::GatherRows(variant_col, variant_state,
                                         Sub(rows, i, glen), variant, 0,
                                         /*whole_output=*/true);
        auto& extracted = Scratch(VariantType(), _extracted);
        duckdb::VariantUtils::VariantExtract(variant, ComponentsSuffix(skip),
                                             extracted, glen);
        PlaceCast(extracted, scan_type, out_vec, out_off, glen, _casted,
                  entire_group);
      }
      i = j;
    }
    return;
  }

  auto& variant = GatherScratch(VariantType(), _variant);
  irs::column_internal::GatherRows(variant_col, variant_state, rows, variant,
                                   0);

  const bool direct_out = variant_out && entire && at == 0;
  auto& extracted = direct_out ? out_vec : Scratch(VariantType(), _extracted);
  duckdb::VariantUtils::VariantExtract(variant, ComponentsSuffix(skip),
                                       extracted, n);

  if (variant_out) {
    if (!direct_out) {
      duckdb::VectorOperations::Copy(extracted, out_vec, n, 0, at);
    }
    return;
  }
  PlaceCast(extracted, scan_type, out_vec, at, n, _casted, entire && at == 0);
}

void ExtractBinding::PlaceCast(
  duckdb::Vector& src, const duckdb::LogicalType& scan_type,
  duckdb::Vector& out_vec, duckdb::idx_t at, duckdb::idx_t n,
  std::unique_ptr<irs::ColumnReader::VectorScratch>& slot, bool entire) const {
  if (entire && at == 0) {
    duckdb::VectorOperations::Cast(*_context, src, out_vec, n);
    return;
  }
  auto& casted = Scratch(scan_type, slot);
  duckdb::VectorOperations::Cast(*_context, src, casted, n);
  duckdb::VectorOperations::Copy(casted, out_vec, n, 0, at);
}

}  // namespace sdb::connector
