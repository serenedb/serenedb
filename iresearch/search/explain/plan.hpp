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
#include <memory>
#include <optional>

#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/explain/explanation.hpp"
#include "iresearch/search/explain/stats.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/search/scorers/scorer.hpp"

namespace irs::explain {

class Part {
 public:
  enum class Outcome : uint8_t {
    None,
    Matched,
    Scored,
  };

  virtual ~Part() = default;

  virtual Outcome Explain(doc_id_t doc, Node& out) = 0;
};

class Plan {
 public:
  Plan(const QueryBuilder& query, const Scorer& scorer,
       const StatsIndex& stats);

  Plan(const Plan&) = delete;
  Plan& operator=(const Plan&) = delete;

  Node Explain(doc_id_t doc, std::optional<score_t> executed);

 private:
  ColumnArgsFetcher _fetcher;
  std::unique_ptr<Part> _root;
};

}  // namespace irs::explain
