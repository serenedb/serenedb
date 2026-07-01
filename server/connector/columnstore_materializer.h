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
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/scalar/variant_utils.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/exceptions.h"
#include "catalog/table_options.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/types.hpp"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {
namespace materialize {

inline const duckdb::LogicalType& VariantType() {
  static const duckdb::LogicalType type = duckdb::LogicalType::VARIANT();
  return type;
}

inline size_t FindStructFieldIndex(const duckdb::LogicalType& struct_type,
                                   std::string_view field) {
  const auto& fields = duckdb::StructType::GetChildTypes(struct_type);
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].first == field) {
      return i;
    }
  }
  return fields.size();
}

}  // namespace materialize

struct ColumnstoreProjection {
  duckdb::idx_t output_slot;
  catalog::Column::Id column_id;
  std::vector<std::string_view> extract_path;
  duckdb::LogicalType extract_scan_type = duckdb::LogicalType::INVALID;

  bool IsExtract() const noexcept { return !extract_path.empty(); }
};

class ColumnstoreMaterializer {
 public:
  ColumnstoreMaterializer(const irs::ColReader& reader,
                          std::span<const ColumnstoreProjection> projections,
                          duckdb::ClientContext* context);

  ColumnstoreMaterializer(const ColumnstoreMaterializer&) = delete;
  ColumnstoreMaterializer& operator=(const ColumnstoreMaterializer&) = delete;

  bool HasAny() const noexcept { return !_bound.empty(); }

  template<typename DocIds>
  void SelectByDocIds(const DocIds& doc_ids, duckdb::DataChunk& output,
                      duckdb::idx_t output_start) const {
    if (_bound.empty() || doc_ids.size() == 0) {
      return;
    }
    MaterializeInto(doc_ids, output, output_start,
                    /*allow_scan_fastpath=*/false,
                    /*start_doc=*/0);
  }

  void Scan(uint64_t start_doc, duckdb::idx_t count, duckdb::DataChunk& output,
            duckdb::idx_t output_start) const {
    if (_bound.empty() || count == 0) {
      return;
    }
    MaterializeInto(irs::IotaRange{start_doc, count}, output, output_start,
                    /*allow_scan_fastpath=*/true, start_doc);
  }

 private:
  enum class ExtractKind : uint8_t {
    kScalarLeaf,
    kVariant,
    kAllNull,
  };

  struct Binding {
    const irs::ColumnReader* reader;
    duckdb::idx_t output_slot;
    std::unique_ptr<irs::ColumnReader::ScanState> state;
    std::span<const std::string_view> extract_path;
    duckdb::LogicalType extract_scan_type = duckdb::LogicalType::INVALID;

    mutable duckdb::vector<duckdb::VariantPathComponent> components;
    mutable bool components_built = false;
    mutable duckdb::vector<duckdb::VariantPathComponent> components_suffix;
    mutable size_t components_suffix_skip = 0;
    mutable bool components_suffix_built = false;
    mutable std::unique_ptr<irs::ColumnReader::VectorScratch> scratch;
    mutable std::unique_ptr<irs::ColumnReader::VectorScratch> casted;
    mutable std::unique_ptr<irs::ColumnReader::VectorScratch> variant;
    mutable std::unique_ptr<irs::ColumnReader::VectorScratch> extracted;

    mutable bool resolved = false;
    mutable bool is_extract = false;
    mutable bool is_list_like = false;
    mutable duckdb::LogicalTypeId type_id = duckdb::LogicalTypeId::INVALID;
    mutable ExtractKind extract_kind = ExtractKind::kScalarLeaf;
    mutable bool leaf_scalar = false;
    mutable size_t variant_skip = 0;
    mutable const irs::ColumnReader* leaf_reader = nullptr;
    mutable irs::ColumnReader::ScanState* leaf_state = nullptr;

    bool IsExtract() const noexcept { return !extract_path.empty(); }

    void Resolve() const {
      if (resolved) {
        return;
      }
      type_id = reader->Type().id();
      is_extract = IsExtract();
      is_list_like = type_id == duckdb::LogicalTypeId::LIST ||
                     type_id == duckdb::LogicalTypeId::MAP;
      if (is_extract) {
        const auto* leaf = reader;
        auto* leaf_st = state.get();
        for (size_t pi = 0; pi < extract_path.size(); ++pi) {
          if (leaf->Type().id() == duckdb::LogicalTypeId::VARIANT) {
            extract_kind = ExtractKind::kVariant;
            variant_skip = pi;
            leaf_reader = leaf;
            leaf_state = leaf_st;
            resolved = true;
            return;
          }
          if (leaf->Type().id() != duckdb::LogicalTypeId::STRUCT) {
            extract_kind = ExtractKind::kAllNull;
            resolved = true;
            return;
          }
          const size_t idx =
            materialize::FindStructFieldIndex(leaf->Type(), extract_path[pi]);
          if (idx >= leaf->StructFieldCount()) {
            extract_kind = ExtractKind::kAllNull;
            resolved = true;
            return;
          }
          leaf = &leaf->StructField(idx);
          leaf_st = &leaf_st->child_states[idx + 1];
        }
        leaf_reader = leaf;
        leaf_state = leaf_st;
        if (leaf->Type().id() == duckdb::LogicalTypeId::VARIANT) {
          extract_kind = ExtractKind::kVariant;
          variant_skip = extract_path.size();
        } else {
          extract_kind = ExtractKind::kScalarLeaf;
          leaf_scalar = !leaf->Type().IsNested();
        }
      }
      resolved = true;
    }

    const duckdb::vector<duckdb::VariantPathComponent>& Components() const {
      if (!components_built) {
        components.reserve(extract_path.size());
        for (const auto& field : extract_path) {
          components.emplace_back(std::string{field});
        }
        components_built = true;
      }
      return components;
    }

    const duckdb::vector<duckdb::VariantPathComponent>& ComponentsSuffix(
      size_t skip) const {
      const auto& all = Components();
      if (skip == 0) {
        return all;
      }
      if (skip >= all.size()) {
        static const duckdb::vector<duckdb::VariantPathComponent> kEmpty;
        return kEmpty;
      }
      if (!components_suffix_built || components_suffix_skip != skip) {
        components_suffix.assign(
          all.begin() + static_cast<std::ptrdiff_t>(skip), all.end());
        components_suffix_skip = skip;
        components_suffix_built = true;
      }
      return components_suffix;
    }
  };

  template<typename DocIds>
  void MaterializeInto(const DocIds& doc_ids, duckdb::DataChunk& output,
                       duckdb::idx_t output_start, bool allow_scan_fastpath,
                       uint64_t start_doc) const {
    SDB_IF_FAILURE("SearchIncludeFetchFault") { SDB_THROW(ERROR_DEBUG); }
    const auto count = static_cast<duckdb::idx_t>(doc_ids.size());
    for (const auto& b : _bound) {
      b.Resolve();
      auto& out_vec = output.data[b.output_slot];
      if (b.is_extract) {
        SDB_ASSERT(_context);
        ExtractPath(b, doc_ids, out_vec, output_start, allow_scan_fastpath);
        continue;
      }
      if (output_start == 0 && b.is_list_like) {
        duckdb::ListVector::SetListSize(out_vec, 0);
      }
      if (allow_scan_fastpath && output_start == 0 &&
          b.reader->CursorRow(*b.state) == start_doc) {
        SDB_ASSERT(
          duckdb::FlatVector::Validity(out_vec).CheckAllValid(count, 0),
          "columnstore Scan requires an all-valid target; validity codec "
          "AND-combines into out_vec");
        b.reader->Scan(*b.state, out_vec, count);
      } else {
        SDB_ASSERT(duckdb::FlatVector::Validity(out_vec).CheckAllValid(
                     output_start + count, output_start),
                   "columnstore Gather requires an all-valid target; validity "
                   "codec AND-combines into out_vec");
        b.reader->Gather(*b.state, doc_ids, out_vec, output_start);
      }
    }
  }

  static duckdb::Vector& Scratch(
    const duckdb::LogicalType& type,
    std::unique_ptr<irs::ColumnReader::VectorScratch>& slot) {
    if (!slot) {
      slot = std::make_unique<irs::ColumnReader::VectorScratch>(type);
    }
    return slot->Reset();
  }

  static duckdb::Vector& GatherScratch(
    const duckdb::LogicalType& type,
    std::unique_ptr<irs::ColumnReader::VectorScratch>& slot) {
    auto& scratch = Scratch(type, slot);
    duckdb::FlatVector::ValidityMutable(scratch).Reset();
    return scratch;
  }

  void PlaceCast(
    duckdb::Vector& src, const duckdb::LogicalType& scan_type,
    duckdb::Vector& out_vec, duckdb::idx_t output_start, duckdb::idx_t n,
    std::unique_ptr<irs::ColumnReader::VectorScratch>& casted_slot) const {
    if (output_start == 0) {
      duckdb::VectorOperations::Cast(*_context, src, out_vec, n);
      return;
    }
    auto& casted = Scratch(scan_type, casted_slot);
    duckdb::VectorOperations::Cast(*_context, src, casted, n);
    duckdb::VectorOperations::Copy(casted, out_vec, n, 0, output_start);
  }

  template<typename DocIds>
  void ExtractPath(const Binding& b, const DocIds& doc_ids,
                   duckdb::Vector& out_vec, duckdb::idx_t output_start,
                   bool allow_scan_fastpath) const {
    const auto n = static_cast<duckdb::idx_t>(doc_ids.size());
    SDB_ASSERT(n <= STANDARD_VECTOR_SIZE);
    if (n == 0) {
      return;
    }
    switch (b.extract_kind) {
      case ExtractKind::kAllNull:
        SetAllNull(out_vec, output_start, n);
        return;
      case ExtractKind::kVariant:
        VariantExtract(b, *b.leaf_reader, *b.leaf_state, doc_ids,
                       b.variant_skip, out_vec, output_start);
        return;
      case ExtractKind::kScalarLeaf:
        ExtractLeaf(b, *b.leaf_reader, *b.leaf_state, doc_ids, out_vec,
                    output_start, allow_scan_fastpath);
        return;
    }
  }

  template<typename DocIds>
  void ExtractLeaf(const Binding& b, const irs::ColumnReader& leaf,
                   irs::ColumnReader::ScanState& leaf_state,
                   const DocIds& doc_ids, duckdb::Vector& out_vec,
                   duckdb::idx_t output_start, bool allow_scan_fastpath) const {
    const auto n = static_cast<duckdb::idx_t>(doc_ids.size());
    const auto& scan_type = b.extract_scan_type;
    const bool fastpath = allow_scan_fastpath && output_start == 0 &&
                          b.leaf_scalar &&
                          ContiguousFromCursor(leaf, leaf_state, doc_ids);
    if (leaf.Type() == scan_type) {
      SDB_ASSERT(duckdb::FlatVector::Validity(out_vec).CheckAllValid(
                   output_start + n, output_start),
                 "columnstore extract leaf requires an all-valid target; "
                 "validity codec AND-combines into out_vec");
      if (fastpath) {
        leaf.Scan(leaf_state, out_vec, n);
      } else {
        leaf.Gather(leaf_state, doc_ids, out_vec, output_start);
      }
      return;
    }
    auto& scratch = GatherScratch(leaf.Type(), b.scratch);
    if (fastpath) {
      leaf.Scan(leaf_state, scratch, n);
    } else {
      leaf.Gather(leaf_state, doc_ids, scratch, 0);
    }
    PlaceCast(scratch, scan_type, out_vec, output_start, n, b.casted);
  }

  template<typename DocIds>
  static bool ContiguousFromCursor(
    const irs::ColumnReader& leaf,
    const irs::ColumnReader::ScanState& leaf_state, const DocIds& doc_ids) {
    if constexpr (requires { typename DocIds::contiguous_range_tag; }) {
      return leaf.CursorRow(leaf_state) == static_cast<uint64_t>(doc_ids[0]);
    } else {
      return false;
    }
  }

  template<typename DocIds>
  void VariantExtract(const Binding& b, const irs::ColumnReader& variant_col,
                      irs::ColumnReader::ScanState& variant_state,
                      const DocIds& doc_ids, size_t skip,
                      duckdb::Vector& out_vec,
                      duckdb::idx_t output_start) const {
    const auto n = static_cast<duckdb::idx_t>(doc_ids.size());
    const auto& scan_type = b.extract_scan_type;
    const bool variant_out = scan_type.id() == duckdb::LogicalTypeId::VARIANT;

    const auto remaining =
      b.extract_path.subspan(std::min<size_t>(skip, b.extract_path.size()));

    if (!variant_out) {
      if (variant_state.variant_leaf_resolved < 0) {
        variant_state.variant_leaf_resolved =
          variant_col.ResolveUniformShreddedLeaf(
            remaining, variant_state.variant_leaf_type)
            ? 1
            : 0;
      }
      if (variant_state.variant_leaf_resolved == 1) {
        if (variant_state.variant_leaf_type == scan_type) {
          const bool ok = variant_col.TryGatherVariantExtract(
            variant_state, doc_ids, remaining, out_vec, output_start);
          SDB_ASSERT(ok);
          return;
        }
        auto& scratch =
          GatherScratch(variant_state.variant_leaf_type, b.scratch);
        const bool ok = variant_col.TryGatherVariantExtract(
          variant_state, doc_ids, remaining, scratch, 0);
        SDB_ASSERT(ok);
        PlaceCast(scratch, scan_type, out_vec, output_start, n, b.casted);
        return;
      }
    }

    auto& variant = GatherScratch(materialize::VariantType(), b.variant);
    variant_col.Gather(variant_state, doc_ids, variant, 0);

    auto& extracted = (variant_out && output_start == 0)
                        ? out_vec
                        : Scratch(materialize::VariantType(), b.extracted);
    duckdb::VariantUtils::VariantExtract(variant, b.ComponentsSuffix(skip),
                                         extracted, n);

    if (variant_out) {
      if (output_start != 0) {
        duckdb::VectorOperations::Copy(extracted, out_vec, n, 0, output_start);
      }
      return;
    }
    PlaceCast(extracted, scan_type, out_vec, output_start, n, b.casted);
  }

  static void SetAllNull(duckdb::Vector& out_vec, duckdb::idx_t output_start,
                         duckdb::idx_t n) {
    auto& validity = duckdb::FlatVector::ValidityMutable(out_vec);
    if (output_start == 0) {
      validity.SetAllInvalid(n);
      return;
    }
    validity.EnsureWritable();
    for (duckdb::idx_t i = 0; i < n; ++i) {
      validity.SetInvalidUnsafe(output_start + i);
    }
  }

  irs::ReadContext _ctx;
  std::vector<Binding> _bound;
  duckdb::ClientContext* _context = nullptr;
};

}  // namespace sdb::connector
