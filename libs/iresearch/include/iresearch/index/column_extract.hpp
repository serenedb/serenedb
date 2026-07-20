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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/scalar/variant_utils.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/types.hpp"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

struct DocRows {
  std::span<const irs::doc_id_t> docs;

  size_t size() const noexcept { return docs.size(); }
  uint64_t operator[](size_t i) const noexcept {
    return docs[i] - irs::doc_limits::min();
  }
};

struct ColumnstoreProjection {
  duckdb::idx_t output_slot;
  irs::field_id column_id;
  std::vector<std::string_view> extract_path;
  duckdb::LogicalType extract_scan_type = duckdb::LogicalType::INVALID;

  bool IsExtract() const noexcept { return !extract_path.empty(); }
};

class ExtractBinding {
 public:
  ExtractBinding() = default;

  ExtractBinding(const ExtractBinding&) = delete;
  ExtractBinding& operator=(const ExtractBinding&) = delete;

  void Bind(const irs::ColumnReader& column, irs::ReadContext& ctx,
            std::span<const std::string_view> path,
            const duckdb::LogicalType& scan_type,
            duckdb::ClientContext* context);

  void MaterializeRows(DocRows rows, duckdb::Vector& out_vec, duckdb::idx_t at,
                       bool entire) const;

  void MaterializeContiguous(uint64_t start_row, duckdb::idx_t count,
                             duckdb::Vector& out_vec) const;

  // Survivor rows start_row + sel[0..survivors) into out_vec[0, survivors):
  // the sel-backed row view goes through the same density-banded gathers as
  // MaterializeRows (Scan / decode+Slice / scatter runs), no staging copy.
  void MaterializeSelected(uint64_t start_row,
                           const duckdb::SelectionVector& sel,
                           duckdb::idx_t survivors,
                           duckdb::Vector& out_vec) const;

 private:
  enum class ExtractKind : uint8_t {
    ScalarLeaf,
    Variant,
    AllNull,
  };

  void Resolve();
  const duckdb::vector<duckdb::VariantPathComponent>& Components() const;
  const duckdb::vector<duckdb::VariantPathComponent>& ComponentsSuffix(
    size_t skip) const;

  template<typename Rows>
  void ExtractPath(const Rows& rows, duckdb::Vector& out_vec, duckdb::idx_t at,
                   bool contiguous, bool entire) const;
  template<typename Rows>
  void ExtractLeaf(const Rows& rows, duckdb::Vector& out_vec, duckdb::idx_t at,
                   bool contiguous, bool entire) const;
  template<typename Rows>
  void VariantExtract(const Rows& rows, duckdb::Vector& out_vec,
                      duckdb::idx_t at, bool contiguous, bool entire) const;

  void PlaceCast(duckdb::Vector& src, const duckdb::LogicalType& scan_type,
                 duckdb::Vector& out_vec, duckdb::idx_t at, duckdb::idx_t n,
                 std::unique_ptr<irs::ColumnReader::VectorScratch>& slot,
                 bool entire) const;

  const irs::ColumnReader* _reader = nullptr;
  std::unique_ptr<irs::ColumnReader::ScanState> _state;
  std::span<const std::string_view> _extract_path;
  duckdb::LogicalType _extract_scan_type = duckdb::LogicalType::INVALID;
  duckdb::ClientContext* _context = nullptr;

  mutable duckdb::vector<duckdb::VariantPathComponent> _components;
  mutable bool _components_built = false;
  mutable duckdb::vector<duckdb::VariantPathComponent> _components_suffix;
  mutable size_t _components_suffix_skip = 0;
  mutable bool _components_suffix_built = false;
  mutable std::unique_ptr<irs::ColumnReader::VectorScratch> _scratch;
  mutable std::unique_ptr<irs::ColumnReader::VectorScratch> _casted;
  mutable std::unique_ptr<irs::ColumnReader::VectorScratch> _variant;
  mutable std::unique_ptr<irs::ColumnReader::VectorScratch> _extracted;

  ExtractKind _extract_kind = ExtractKind::ScalarLeaf;
  bool _leaf_scalar = false;
  size_t _variant_skip = 0;
  const irs::ColumnReader* _leaf_reader = nullptr;
  irs::ColumnReader::ScanState* _leaf_state = nullptr;
};

}  // namespace sdb::connector
