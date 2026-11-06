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
#include <string_view>
#include <utility>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {

class ColFileWriter final {
 public:
  explicit ColFileWriter(IndexOutput::ptr out) noexcept
    : _out{std::move(out)} {}

  uint64_t Position() const noexcept { return _out->Position(); }

  uint64_t Append(std::string_view bytes);

  uint64_t AppendAligned(std::span<const std::string_view> parts);

  uint64_t AppendString(std::string_view string);

  void Finish(format_utils::FooterWriter footer);

 private:
  uint64_t Write(uint64_t alignment, std::span<const std::string_view> parts);

  IndexOutput::ptr _out;
};

}  // namespace irs
