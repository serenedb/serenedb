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
#include <span>

#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

template<typename TermIterator>
bool SeekClusterTerm(TermIterator& terms, uint32_t cluster_id,
                     std::span<byte_type, kCentroidTermWidth> term_buf) {
  EncodeCentroidTerm(cluster_id, term_buf.data());
  if (!terms.seek(bytes_view{term_buf.data(), term_buf.size()})) {
    return false;
  }
  terms.read();
  return true;
}

inline bool PrepareInnerFilter(const std::shared_ptr<const Filter>& inner,
                               const SubReader& segment,
                               const PrepareContext& ctx,
                               QueryBuilder::ptr& out) {
  if (!inner) {
    return true;
  }
  auto inner_ctx = ctx;
  inner_ctx.collector = nullptr;
  out = inner->PrepareSegment(segment, inner_ctx);
  return out != nullptr;
}

}  // namespace irs
