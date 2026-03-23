////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <vpack/builder.h>
#include <vpack/slice.h>

#include <memory>
#include <utility>

#include "basics/buffer.h"
#include "basics/common.h"
#include "basics/result.h"

namespace sdb::rocksutils {

enum StatusHint {
  kNone,
  kDocument,
  kCollection,
  kView,
  kIndex,
  kDatabase,
  kWal
};

inline rocksdb::Slice StrToSlice(std::string_view str) noexcept {
  return {str.data(), str.size()};
}

inline std::string_view SliceToStr(rocksdb::Slice slice) noexcept {
  return {slice.data(), slice.size()};
}

sdb::Result ConvertStatus(const rocksdb::Status&,
                          StatusHint hint = StatusHint::kNone);

std::pair<vpack::Slice, std::unique_ptr<vpack::BufferUInt8>> StripObjectIds(
  vpack::Slice input_slice, bool check_before_copy = true);

}  // namespace sdb::rocksutils
