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

#include "connector/ann_filter.hpp"

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>

#include "connector/search_remove_filter.hpp"

namespace sdb::connector {

ANNFilter::ANNFilter(duckdb::ClientContext& context,
                     const irs::IndexReader& reader,
                     std::unique_ptr<RowMaterializer> materializer,
                     std::vector<duckdb::unique_ptr<duckdb::Expression>> exprs,
                     std::vector<duckdb::LogicalType> filter_types)
  : reader_{reader},
    materializer_{std::move(materializer)},
    exprs_{std::move(exprs)},
    executor_{context} {
  for (const auto& e : exprs_) {
    executor_.AddExpression(*e);
  }
  duckdb::vector<duckdb::LogicalType> scratch_types(filter_types.begin(),
                                                    filter_types.end());
  scratch_.Initialize(duckdb::Allocator::DefaultAllocator(), scratch_types);

  duckdb::vector<duckdb::LogicalType> bool_types(exprs_.size(),
                                                 duckdb::LogicalType::BOOLEAN);
  bool_out_.Initialize(duckdb::Allocator::DefaultAllocator(), bool_types);
}

bool ANNFilter::is_member(faiss::idx_t id) const {
  auto [seg_id, doc_id] = irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id));
  if (seg_id >= reader_.size()) {
    return false;
  }
  const auto& segment = reader_[seg_id];
  const auto* pk_col = segment.column(kPkFieldName);
  if (!pk_col) {
    return false;
  }
  auto pk_iter = pk_col->iterator(irs::ColumnHint::Normal);
  if (!pk_iter) {
    return false;
  }
  const auto* pk_val = irs::get<irs::PayAttr>(*pk_iter);
  if (!pk_val) {
    return false;
  }
  if (pk_iter->seek(doc_id) != doc_id) {
    return false;
  }
  auto val = pk_val->value;
  std::string_view pk{reinterpret_cast<const char*>(val.data()), val.size()};

  scratch_.Reset();
  std::array<std::string_view, 1> pks{pk};
  materializer_->Materialize(pks, scratch_);
  scratch_.SetCardinality(1);

  bool_out_.Reset();
  bool_out_.SetCardinality(1);
  executor_.Execute(scratch_, bool_out_);

  for (duckdb::idx_t i = 0; i < bool_out_.ColumnCount(); ++i) {
    auto& vec = bool_out_.data[i];
    auto value = vec.GetValue(0);
    if (value.IsNull()) {
      return false;
    }
    if (!duckdb::BooleanValue::Get(value)) {
      return false;
    }
  }
  return true;
}

}  // namespace sdb::connector
