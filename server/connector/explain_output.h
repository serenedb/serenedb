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
#include <iresearch/search/explain/explanation.hpp>
#include <span>
#include <string>

#include "connector/scan/scan_bind.h"

namespace sdb::connector {

duckdb::LogicalType ExplainType();

class ExplainLabels {
 public:
  explicit ExplainLabels(const ScanBindData& bind) noexcept : _bind{bind} {}

  std::string Of(const irs::explain::Subject& subject) const;

 private:
  std::string Term(irs::field_id field, irs::bytes_view term) const;

  const ScanBindData& _bind;
};

void WriteExplanations(std::span<const irs::explain::Node> rows,
                       const ExplainLabels& labels, duckdb::Vector& out);

}  // namespace sdb::connector
