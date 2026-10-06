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
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

struct ZSTD_DDict_s;

namespace irs::codecs {

inline constexpr size_t kTrainedDictionaryBytes = 64 * 1024;
inline constexpr size_t kTrainedFrameBytes = 16 * 1024;
inline constexpr size_t kSamplePieceBytes = 4 * 1024;
inline constexpr size_t kRowGroupSampleBytes = 1024 * 1024;
inline constexpr size_t kTrainSampleBytes = 4 * 1024 * 1024;

class TrainedDictionary {
 public:
  explicit TrainedDictionary(std::string bytes);
  ~TrainedDictionary();

  TrainedDictionary(const TrainedDictionary&) = delete;
  TrainedDictionary& operator=(const TrainedDictionary&) = delete;

  std::string_view Bytes() const noexcept { return _bytes; }
  const ZSTD_DDict_s* ZstdDictionary() const noexcept { return _ddict; }
  uint64_t Id() const noexcept { return _id; }

 private:
  std::string _bytes;
  ZSTD_DDict_s* _ddict;
  uint64_t _id;
};

using TrainedDictionaries =
  std::vector<std::shared_ptr<const TrainedDictionary>>;

class DictionarySampler {
 public:
  void Add(std::span<const std::string_view> entries);
  bool Ready() const noexcept { return _samples.size() >= kTrainSampleBytes; }
  std::optional<std::string> Train();

 private:
  std::string _samples;
};

}  // namespace irs::codecs
