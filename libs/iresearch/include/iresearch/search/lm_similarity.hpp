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

// Shared collectors for language-model similarities (LMJelinekMercer,
// LMDirichlet). Tracks:
//   - docs_with_field: number of documents indexed in the field,
//   - total_term_freq: sum of term frequencies across all docs in
//     the field (i.e. the field's total token count).
//
// Used to derive the collection model probability
//   P(t | C) = (ttf_t + 1) / (sum_ttf + 1)
struct LMFieldCollector final : FieldCollector {
  uint64_t docs_with_field = 0;
  uint64_t total_term_freq = 0;

  void collect(const SubReader& /*segment*/,
               const TermReader& field) noexcept final;

  void reset() noexcept final {
    docs_with_field = 0;
    total_term_freq = 0;
  }

  void collect(bytes_view in) final;

  void collect(FieldCollector&& other) noexcept final;

  void write(DataOutput& out) const final;
};

// Tracks the collection-wide frequency of a single term (TermMeta::freq),
// which is what the LM's term weight needs -- not just the doc count.
struct LMTermCollector final : TermCollector {
  uint64_t total_term_freq = 0;  // sum of tf across all docs containing term

  void collect(const SubReader& /*segment*/, const TermReader& /*field*/,
               const AttributeProvider& term_attrs) final;

  void reset() noexcept final { total_term_freq = 0; }

  void collect(bytes_view in) final;

  void collect(TermCollector&& other) noexcept final;

  void write(DataOutput& out) const final;
};

// Stats persisted per (field,term) and consumed by score().
// `collection_prob` is DefaultCollectionModel:
//   (ttf_term + 1) / (ttf_field + 1)
struct LMStats {
  score_t collection_prob;
};

}  // namespace irs
