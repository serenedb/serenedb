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
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_input.hpp"  // IndexInput
#include "iresearch/types.hpp"
#include "iresearch/utils/bytes_utils.hpp"  // bstring
#include "iresearch/utils/type_limits.hpp"  // field_limits

namespace irs {

class Directory;
class TwoLayerCentroids;
struct IResourceManager;

struct TermDictMeta {
  IndexFeatures features{IndexFeatures::None};
  uint64_t term_count{};
  uint64_t doc_count{};
  uint64_t total_doc_freq{};
  uint64_t total_term_freq{};
  bool has_wand{false};
  uint64_t body_offset{};
  field_id norm{field_limits::invalid()};
};

inline constexpr std::string_view kIdxFormatExt = "idx";
inline constexpr std::string_view kIdxFormatName = "iresearch_index";
inline constexpr int32_t kIdxFormatVersion = 1;

enum class IdxSlotKind : uint8_t {
  TermDict = 0,
  Ivf = 1,
};

class IdxReader final {
 public:
  IdxReader(const Directory& dir, std::string_view segment_name);
  ~IdxReader();

  IdxReader(const IdxReader&) = delete;
  IdxReader& operator=(const IdxReader&) = delete;

  bool HasIvf(field_id id) const noexcept;
  const TwoLayerCentroids* Ivf(field_id id) const noexcept;

  const TermDictMeta* TermDict(field_id id) const noexcept;

  std::span<const std::pair<field_id, TermDictMeta>> TermDicts() const noexcept;

  IndexInput::ptr ReopenIn() const;

  uint64_t BodyStart() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> _impl;
};

}  // namespace irs
