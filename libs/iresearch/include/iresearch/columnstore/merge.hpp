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

#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_meta.hpp"

namespace irs {

class SubReader;

}  // namespace irs
namespace irs::columnstore {

class Reader;
class Writer;

// Per-source handle the merge passes to both `columnstore::MergeInto` and
// the iresearch-side norm/posting walk. `reader` is the iresearch view used
// to look up a field's norm id by name; columnstore::MergeInto itself does
// not read it. `cs_reader` is the cached columnstore view owned by the same
// SegmentReaderImpl, so no extra footer parse per source.
struct MergeSource {
  const SubReader* reader;
  const Reader* cs_reader;
  DocumentMaskView mask;
  uint64_t alive_count;
};

void MergeInto(std::span<const MergeSource> sources, Writer& output,
               const ColumnOptionsProvider* column_options);

}  // namespace irs::columnstore
