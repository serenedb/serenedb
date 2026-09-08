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

#include <duckdb/common/types/string_type.hpp>

#include "basics/shared.hpp"
#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/words/masks.hpp"

namespace irs::analysis::words {

template<typename EmitFn>
IRS_FORCE_INLINE void SplitByNonAlpha(duckdb::string_t data, EmitFn&& emit) {
  classify::ForEachRun(
    reinterpret_cast<const byte_type*>(data.GetData()), data.GetSize(),
    [](const byte_type* block)
      IRS_FORCE_INLINE { return ClassifyAlnumBlock(block); },
    emit);
}

}  // namespace irs::analysis::words
