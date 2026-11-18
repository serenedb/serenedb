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

#include <duckdb/common/types/vector.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/detail/vector_slots.hpp>
#include <iresearch/search/explain/explanation.hpp>
#include <optional>
#include <string>
#include <string_view>

#include "connector/score_emit.h"

namespace sdb::connector {

irs::explain::Node ExplainDistance(const VectorScorerOptions& vs,
                                   std::string_view column,
                                   std::optional<irs::score_t> returned,
                                   std::optional<irs::score_t> stored);

class DistanceExplainer final : public irs::explain::Explainer {
 public:
  DistanceExplainer(const irs::SubReader& segment,
                    const VectorScorerOptions& vs, std::string column);

  irs::explain::Node Explain(irs::doc_id_t doc,
                             std::optional<irs::score_t> returned) final;

 private:
  std::optional<irs::score_t> Stored(irs::doc_id_t doc);

  const VectorScorerOptions& _vs;
  std::string _column;
  std::optional<irs::detail::RawVectorReader> _reader;
  std::optional<irs::ColumnReader::PointReader> _validity;
  duckdb::Vector _scratch{duckdb::LogicalType::BOOLEAN, 1};
};

}  // namespace sdb::connector
