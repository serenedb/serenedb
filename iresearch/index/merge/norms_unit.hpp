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
#include <span>

#include "iresearch/formats/column/merge.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColWriter;
class MergeOrder;
class NormColumnReader;

const NormColumnReader* SourceNormColumn(const MergeSource& source,
                                         field_id id);

struct LiveNormStats {
  bool present = false;
  uint64_t sum = 0;
  uint64_t non_zero = 0;
};

LiveNormStats ComputeLiveNormStats(field_id id,
                                   std::span<const MergeSource> sources);

void MergeNormColumn(ColWriter& col_writer, field_id id, field_id out_id,
                     std::span<const MergeSource> sources,
                     const MergeOrder* order = nullptr);

}  // namespace irs
