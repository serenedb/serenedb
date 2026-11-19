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

#include <cstddef>

#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/explain/stats.hpp"

namespace irs::explain {

SlotStats FieldStats(const FieldPrepareCollector& collector, field_id field);

TermStats TermOf(bytes_view term, const TermCollector& counts);

void DescribeTerm(const ByTermsCollector& collector, size_t i, field_id field,
                  bytes_view term, StatsIndex& out);

void DescribeSlot(const PrepareCollector* collector, field_id field,
                  StatsIndex& out);

void DescribeMultiTerm(const PrepareCollector* collector, field_id field,
                       StatsIndex& out);

void DescribeBlended(const PrepareCollector* collector, field_id field,
                     StatsIndex& out);

}  // namespace irs::explain
