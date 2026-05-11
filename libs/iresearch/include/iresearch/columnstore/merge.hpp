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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <span>

#include "iresearch/index/index_meta.hpp"

namespace irs::columnstore {

class Reader;
class Writer;

// Merges columns from `sources` into `output`. Processes sources in order;
// for each column id present in any source, the output writer opens a
// column with the source's LogicalType. Per source row group, values are
// pulled via ColumnSegment::Scan, filtered against the source's docs_mask,
// and forwarded to ColumnWriter::Append.
//
// `source_masks` is parallel to `sources`; nullptr means no deletes.
void MergeInto(std::span<const Reader* const> sources,
               std::span<const DocumentMask* const> source_masks,
               Writer& output);

}  // namespace irs::columnstore
