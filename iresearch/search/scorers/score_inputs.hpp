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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/scorer.hpp"

namespace irs {

inline const score_t* ScaleOf(const ScoreContext& ctx) noexcept {
  const auto* attr = irs::get<ScaleBlockAttr>(ctx.doc_attrs);
  return attr ? attr->value : nullptr;
}

inline const uint32_t* FieldNormsOf(const ScoreContext& ctx) {
  if (const auto* attr = irs::get<Norm>(ctx.doc_attrs)) {
    return &attr->value;
  }
  return ctx.fetcher.AddNorms(ctx.field.norm,
                              ctx.segment.norms(ctx.field.norm));
}

inline const uint32_t* NormsOf(const ScoreContext& ctx) {
  const auto* norm = FieldNormsOf(ctx);
  return norm ? norm : kNorms.data();
}

}  // namespace irs
