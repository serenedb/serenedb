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

#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

// Tracks the collection-wide frequency of a single term (TermMeta::freq),
// which is what the LM's term weight needs -- not just the doc count.
struct LMTermCollector final : TermCollector {
  uint64_t total_term_freq = 0;  // sum of tf across all docs containing term

  void collect(const SubReader& /*segment*/, const TermReader& /*field*/,
               const AttributeProvider& term_attrs) final;

  void reset() noexcept final { total_term_freq = 0; }

  void collect(bytes_view in) final;

  void write(DataOutput& out) const final;
};

// Stats persisted per (field,term) and consumed by score().
// `collection_prob` is DefaultCollectionModel:
//   (ttf_term + 1) / (ttf_field + 1)
struct LMStats {
  score_t collection_prob;
};

}  // namespace irs
