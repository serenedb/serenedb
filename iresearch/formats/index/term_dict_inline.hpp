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
#include <vector>

#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

class InlinePages {
 public:
  struct Span {
    uint64_t from = 0;
    uint64_t to = 0;
  };

  InlinePages() = default;
  InlinePages(bytes_view starts, uint32_t blocks, uint64_t offset,
              uint64_t size);

  Span Of(uint32_t block) const noexcept;
  size_t MemoryBytes() const noexcept {
    return (_ones.capacity() + _empty.capacity() + _samples.capacity()) *
           sizeof(uint64_t);
  }

 private:
  uint64_t Select(uint64_t rank) const noexcept;
  uint64_t Next(uint64_t at) const noexcept;

  std::vector<uint64_t> _ones;
  std::vector<uint64_t> _empty;
  std::vector<uint64_t> _samples;
  uint64_t _begin = 0;
  uint64_t _end = 0;
  uint32_t _blocks = 0;
};

}  // namespace irs::term_dict
