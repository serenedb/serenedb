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

#include "iresearch/utils/like_matcher.hpp"

#include <absl/algorithm/container.h>
#include <simdutf.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {
namespace {

constexpr size_t kNoMatch = std::numeric_limits<size_t>::max();

constexpr std::array<uint8_t, 256> kUnitSize = [] {
  std::array<uint8_t, 256> sizes{};
  std::fill(sizes.begin(), sizes.begin() + 0x80, 1);
  std::fill(sizes.begin() + 0xC2, sizes.begin() + 0xE0, 2);
  std::fill(sizes.begin() + 0xE0, sizes.begin() + 0xF0, 3);
  std::fill(sizes.begin() + 0xF0, sizes.begin() + 0xF5, 4);
  return sizes;
}();

bool IsContinuation(byte_type b) noexcept { return (b & 0xC0) == 0x80; }

size_t UnitAt(const byte_type* p, const byte_type* end) noexcept {
  const size_t n = kUnitSize[*p];
  if (n > static_cast<size_t>(end - p)) {
    return 0;
  }
  for (size_t i = 1; i < n; ++i) {
    if (!IsContinuation(p[i])) {
      return 0;
    }
  }
  return n;
}

size_t UnitBefore(const byte_type* begin, const byte_type* p) noexcept {
  const auto available = static_cast<size_t>(p - begin);
  for (size_t n = 1; n <= 4 && n <= available; ++n) {
    const auto b = *(p - n);
    if (!IsContinuation(b)) {
      return kUnitSize[b] == n ? n : 0;
    }
  }
  return 0;
}

bool IsRe2Utf8(bytes_view bytes) noexcept {
  const auto* end = bytes.data() + bytes.size();
  for (const auto* p = bytes.data(); p != end;) {
    const auto n = UnitAt(p, end);
    if (n == 0 || (p[0] == 0xE0 && p[1] < 0xA0) ||
        (p[0] == 0xF0 && p[1] < 0x90) || (p[0] == 0xF4 && p[1] > 0x8F)) {
      return false;
    }
    p += n;
  }
  return true;
}

bool IsUnits(bytes_view text) noexcept {
  if (simdutf::validate_utf8(reinterpret_cast<const char*>(text.data()),
                             text.size())) {
    return true;
  }
  const auto* end = text.data() + text.size();
  for (const auto* p = text.data(); p != end;) {
    const auto n = UnitAt(p, end);
    if (n == 0) {
      return false;
    }
    p += n;
  }
  return true;
}

}  // namespace

LikeMatcher::LikeMatcher(bytes_view pattern) {
  _bytes.reserve(pattern.size());
  Piece piece{};
  uint32_t begin = 0;
  const auto close_piece = [&] {
    if (piece.skip != 0 || piece.size != 0) {
      _pieces.push_back(piece);
    }
    piece = {0, static_cast<uint32_t>(_bytes.size()), 0};
  };
  const auto close_segment = [&] {
    close_piece();
    const auto end = static_cast<uint32_t>(_pieces.size());
    _segments.push_back({begin, end});
    begin = end;
  };
  bool escaped = false;
  for (const auto c : pattern) {
    if (escaped) {
      escaped = false;
    } else if (c == WildcardMatch::kEscape) {
      escaped = true;
      continue;
    } else if (c == WildcardMatch::kAnyStr) {
      close_segment();
      _any_string = true;
      continue;
    } else if (c == WildcardMatch::kAnyChr) {
      if (piece.size != 0) {
        close_piece();
      }
      ++piece.skip;
      continue;
    }
    _bytes.push_back(c);
    ++piece.size;
  }
  close_segment();
  if (_segments.size() > 2) {
    const auto last = _segments.end() - 1;
    _segments.erase(std::remove_if(_segments.begin() + 1, last,
                                   [](const Segment& s) { return s.Empty(); }),
                    last);
  }
  _ok = absl::c_all_of(_pieces, [&](const Piece& p) {
    return IsRe2Utf8({_bytes.data() + p.offset, p.size});
  });
}

size_t LikeMatcher::MatchAt(const Piece* piece, const Piece* end,
                            bytes_view text, size_t pos,
                            size_t limit) const noexcept {
  const auto* data = text.data();
  for (; piece != end; ++piece) {
    for (auto skip = piece->skip; skip != 0; --skip) {
      const auto n = pos == limit ? 0 : UnitAt(data + pos, data + limit);
      if (n == 0) {
        return kNoMatch;
      }
      pos += n;
    }
    const size_t size = piece->size;
    if (size > limit - pos ||
        (size != 0 &&
         std::memcmp(data + pos, _bytes.data() + piece->offset, size) != 0)) {
      return kNoMatch;
    }
    pos += size;
  }
  return pos;
}

size_t LikeMatcher::MatchBefore(Segment segment, bytes_view text,
                                size_t lower) const noexcept {
  const auto* data = text.data();
  const auto* first = _pieces.data() + segment.begin;
  size_t pos = text.size();
  for (const auto* piece = _pieces.data() + segment.end; piece != first;) {
    --piece;
    const size_t size = piece->size;
    if (size > pos - lower) {
      return kNoMatch;
    }
    pos -= size;
    if (size != 0 &&
        std::memcmp(data + pos, _bytes.data() + piece->offset, size) != 0) {
      return kNoMatch;
    }
    for (auto skip = piece->skip; skip != 0; --skip) {
      const auto n = UnitBefore(data + lower, data + pos);
      if (n == 0) {
        return kNoMatch;
      }
      pos -= n;
    }
  }
  return pos;
}

size_t LikeMatcher::FindFrom(Segment segment, bytes_view text, size_t pos,
                             size_t limit) const noexcept {
  const auto* first = _pieces.data() + segment.begin;
  const auto* end = _pieces.data() + segment.end;
  if (first->size == 0) {
    return MatchAt(first, end, text, pos, limit);
  }
  const auto* data = text.data();
  const bytes_view window{data, limit};
  const bytes_view literal{_bytes.data() + first->offset, first->size};
  for (size_t from = pos + first->skip;; ++from) {
    from = window.find(literal, from);
    if (from == bytes_view::npos) {
      return kNoMatch;
    }
    auto start = from;
    auto skip = first->skip;
    for (; skip != 0; --skip) {
      const auto n = UnitBefore(data + pos, data + start);
      if (n == 0) {
        break;
      }
      start -= n;
    }
    if (skip == 0) {
      const auto matched =
        MatchAt(first + 1, end, text, from + first->size, limit);
      if (matched != kNoMatch) {
        return matched;
      }
    }
  }
}

bool LikeMatcher::Match(bytes_view text) const noexcept {
  SDB_ASSERT(_ok);
  const auto* pieces = _pieces.data();
  const auto head = _segments.front();
  auto pos =
    MatchAt(pieces + head.begin, pieces + head.end, text, 0, text.size());
  if (!_any_string) {
    return pos == text.size();
  }
  if (pos == kNoMatch) {
    return false;
  }
  const auto limit = MatchBefore(_segments.back(), text, pos);
  if (limit == kNoMatch) {
    return false;
  }
  for (auto it = _segments.begin() + 1, last = _segments.end() - 1; it < last;
       ++it) {
    pos = FindFrom(*it, text, pos, limit);
    if (pos == kNoMatch) {
      return false;
    }
  }
  return IsUnits(text);
}

}  // namespace irs
