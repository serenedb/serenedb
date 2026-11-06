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

#include <absl/base/call_once.h>
#include <zstd.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/store/data_input.hpp"

namespace irs::codecs {

inline constexpr size_t kTrainedFrameBytes = 16 * 1024;

struct ZstdFree {
  void operator()(ZSTD_CCtx* ctx) const noexcept { ZSTD_freeCCtx(ctx); }
  void operator()(ZSTD_DCtx* ctx) const noexcept { ZSTD_freeDCtx(ctx); }
  void operator()(ZSTD_CDict* dict) const noexcept { ZSTD_freeCDict(dict); }
  void operator()(ZSTD_DDict* dict) const noexcept { ZSTD_freeDDict(dict); }
};

class TrainedDictionary {
 public:
  explicit TrainedDictionary(std::string bytes);
  TrainedDictionary(std::string_view mapped, IndexInput::ptr mapping);

  TrainedDictionary(const TrainedDictionary&) = delete;
  TrainedDictionary& operator=(const TrainedDictionary&) = delete;

  std::string_view Bytes() const noexcept { return _bytes; }
  const ZSTD_DDict* ZstdDictionary() const;
  uint64_t Id() const noexcept { return _id; }

 private:
  std::string _owned;
  IndexInput::ptr _mapping;
  std::string_view _bytes;
  mutable absl::once_flag _ddict_once;
  mutable std::unique_ptr<ZSTD_DDict, ZstdFree> _ddict;
  uint64_t _id;
};

using TrainedDictionaries =
  std::vector<std::shared_ptr<const TrainedDictionary>>;

}  // namespace irs::codecs
