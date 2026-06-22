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

#include "iresearch/formats/column/scan.hpp"

#include <absl/strings/match.h>

#include <duckdb/common/types/variant.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/scalar/variant_utils.hpp>
#include <memory>

#include "basics/assert.h"

namespace irs {

std::unique_ptr<MaterializeState> MakeMaterializeState(
  const ColumnReader& reader, ReadContext& ctx) {
  auto state = std::make_unique<MaterializeState>(reader, ctx);
  switch (reader.Type().id()) {
    case duckdb::LogicalTypeId::ARRAY:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      const auto* child = reader.Child();
      SDB_ASSERT(child);
      state->children.emplace_back(MakeMaterializeState(*child, ctx));
    } break;
    case duckdb::LogicalTypeId::STRUCT: {
      state->children.reserve(reader.StructFieldCount());
      for (size_t fi = 0; fi < reader.StructFieldCount(); ++fi) {
        state->children.emplace_back(
          MakeMaterializeState(reader.StructField(fi), ctx));
      }
    } break;
    case duckdb::LogicalTypeId::VARIANT:
      break;
    default:
      break;
  }
  return state;
}

MaterializeState::VariantRgState& EnsureVariantRgState(
  const ColumnReader& reader, MaterializeState& state, size_t rg) {
  if (state.variant_rg_states.empty()) {
    state.variant_rg_states.resize(reader.VariantRgCount());
  }
  auto& slot = state.variant_rg_states[rg];
  if (!slot.unshredded) {
    const auto& row_group_reader = reader.VariantRg(rg);
    slot.unshredded =
      MakeMaterializeState(*row_group_reader.unshredded, *state.ctx);
    if (row_group_reader.shred_state != VariantShredState::Unshredded) {
      slot.shredded =
        MakeMaterializeState(*row_group_reader.shredded, *state.ctx);
    }
  }
  return slot;
}

void ReconstructVariantRun(
  MaterializeState::VariantRgState& rgstate,
  const ColumnReader::VariantRgReader& row_group_reader, uint64_t local_start,
  size_t run, duckdb::Vector& out_variant, duckdb::idx_t out_off,
  VectorPool& scratch) {
  if (row_group_reader.shred_state == VariantShredState::Unshredded) {
    MaterializeNode(*row_group_reader.unshredded, *rgstate.unshredded,
                    IotaRange{local_start, run}, out_variant, out_off);
    return;
  }
  duckdb::child_list_t<duckdb::LogicalType> children;
  children.emplace_back("unshredded", row_group_reader.unshredded->Type());
  children.emplace_back("shredded", row_group_reader.shredded->Type());
  auto intermediate_lease =
    scratch.Acquire(duckdb::LogicalType::STRUCT(children));
  auto& intermediate = *intermediate_lease;
  auto& entries = duckdb::StructVector::GetEntries(intermediate);
  MaterializeNode(*row_group_reader.unshredded, *rgstate.unshredded,
                  IotaRange{local_start, run}, entries[0], /*output_start=*/0);
  MaterializeNode(*row_group_reader.shredded, *rgstate.shredded,
                  IotaRange{local_start, run}, entries[1], /*output_start=*/0);

  auto unshredded = scratch.Acquire(out_variant.GetType());
  duckdb::VariantUtils::UnshredVariantData(intermediate, *unshredded, run);
  duckdb::VectorOperations::Copy(*unshredded, out_variant, run, 0, out_off);
}

size_t FindStructFieldIndex(const duckdb::LogicalType& struct_type,
                            std::string_view field) {
  const auto& children = duckdb::StructType::GetChildTypes(struct_type);
  for (size_t i = 0; i < children.size(); ++i) {
    if (absl::EqualsIgnoreCase(field, children[i].first)) {
      return i;
    }
  }
  return children.size();
}

const ColumnReader* ResolveShreddedLeaf(
  const ColumnReader& shredded_node, std::span<const std::string_view> path) {
  const auto* node = &shredded_node;
  if (!node->FullyShredded()) {
    return nullptr;
  }
  for (const auto& field : path) {
    if (node->Type().id() != duckdb::LogicalTypeId::STRUCT) {
      return nullptr;
    }
    const auto& typed = node->StructField(kShreddedTypedValueIndex);
    if (typed.Type().id() != duckdb::LogicalTypeId::STRUCT) {
      return nullptr;
    }
    const size_t idx = FindStructFieldIndex(typed.Type(), field);
    if (idx == duckdb::StructType::GetChildCount(typed.Type())) {
      return nullptr;
    }
    node = &typed.StructField(idx);
    if (!node->FullyShredded()) {
      return nullptr;
    }
  }
  const auto* leaf = node->Type().id() == duckdb::LogicalTypeId::STRUCT
                       ? &node->StructField(kShreddedTypedValueIndex)
                       : node;
  return leaf->Type().IsNested() ? nullptr : leaf;
}

MaterializeState::ExtractLeafSlot& EnsureExtractLeaf(
  MaterializeState& state, size_t rg,
  const ColumnReader::VariantRgReader& rg_reader,
  std::span<const std::string_view> path, size_t rg_count) {
  if (state.extract_leaf_slots.empty()) {
    state.extract_leaf_slots.resize(rg_count);
  }
  auto& slot = state.extract_leaf_slots[rg];
  if (!slot.resolved) {
    slot.resolved = true;
    if (rg_reader.shred_state != VariantShredState::Unshredded) {
      slot.leaf = ResolveShreddedLeaf(*rg_reader.shredded, path);
    }
    if (slot.leaf != nullptr) {
      slot.state = MakeMaterializeState(*slot.leaf, *state.ctx);
    }
  }
  return slot;
}

void CastExtractInto(duckdb::ClientContext& context, duckdb::Vector& src,
                     duckdb::Vector& dst, size_t run,
                     duckdb::idx_t dst_offset) {
  const auto count = static_cast<duckdb::idx_t>(run);
  if (src.GetType() == dst.GetType()) {
    duckdb::VectorOperations::Copy(src, dst, count, 0, dst_offset);
    return;
  }
  duckdb::Vector casted{dst.GetType(), count};
  duckdb::VectorOperations::Cast(context, src, casted, count);
  duckdb::VectorOperations::Copy(casted, dst, count, 0, dst_offset);
}

}  // namespace irs
