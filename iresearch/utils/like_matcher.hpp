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
#include <vector>

#include "iresearch/utils/string.hpp"

namespace irs {

class LikeMatcher {
 public:
  explicit LikeMatcher(bytes_view pattern);

  bool ok() const noexcept { return _ok; }

  bool Match(bytes_view text) const noexcept;

  bool operator==(const LikeMatcher&) const noexcept = default;

 private:
  struct Piece {
    uint32_t skip;
    uint32_t offset;
    uint32_t size;

    bool operator==(const Piece&) const noexcept = default;
  };

  struct Segment {
    uint32_t begin;
    uint32_t end;

    bool Empty() const noexcept { return begin == end; }

    bool operator==(const Segment&) const noexcept = default;
  };

  size_t MatchAt(const Piece* piece, const Piece* end, bytes_view text,
                 size_t pos, size_t limit) const noexcept;
  size_t MatchBefore(Segment segment, bytes_view text,
                     size_t lower) const noexcept;
  size_t FindFrom(Segment segment, bytes_view text, size_t pos,
                  size_t limit) const noexcept;

  bstring _bytes;
  std::vector<Piece> _pieces;
  std::vector<Segment> _segments;
  bool _any_string{false};
  bool _ok{false};
};

}  // namespace irs
