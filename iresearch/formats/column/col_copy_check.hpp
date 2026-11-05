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
#include <cstdint>
#include <duckdb/storage/object_cache.hpp>
#include <memory>

#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/reencode_cache.hpp"
#include "iresearch/index/column_info.hpp"

namespace irs {

inline constexpr size_t kReencodeSampleRowGroups = 2;
inline constexpr size_t kCopyMinRowGroups = 4 * kReencodeSampleRowGroups;
inline constexpr uint64_t kReencodeMinSavingsPercent = 10;

bool ReencodePays(const ReencodeEstimate& estimate) noexcept;

ReencodeEstimate EstimateReencode(
  const ColReader& reader, const ColumnReader& column,
  const IndexFieldOptions& options, duckdb::ObjectCache* cache,
  std::shared_ptr<const codecs::TrainedDictionary> dictionary = nullptr);

}  // namespace irs
