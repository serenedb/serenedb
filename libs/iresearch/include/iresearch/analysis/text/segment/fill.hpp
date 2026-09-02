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

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>

#include <limits>
#include <string_view>

#include "iresearch/analysis/text/segment/options.hpp"
#include "iresearch/analysis/text/sz/stringzilla.hpp"
#include "iresearch/analysis/text/words/ascii.hpp"
#include "iresearch/analysis/text/words/unicode.hpp"
#include "iresearch/analysis/token_sink.hpp"
#include "iresearch/utils/utf8_character_utils.hpp"

namespace irs::analysis::segment {

template<typename Pred>
bool AnyOfChar32(std::string_view bytes, Pred pred) noexcept {
  const auto* it = reinterpret_cast<const byte_type*>(bytes.data());
  const auto* end = it + bytes.size();
  while (it != end) {
    if (pred(utf8_utils::ToChar32(it, end))) {
      return true;
    }
  }
  return false;
}

template<Accept A, bool Ascii>
bool AcceptBytes(std::string_view bytes) noexcept {
  static_assert(A != Accept::Any);
  if constexpr (A == Accept::Graphic) {
    if constexpr (Ascii) {
      return absl::c_any_of(bytes, absl::ascii_isgraph);
    } else {
      return AnyOfChar32(
        bytes, [](uint32_t c) { return !utf8_utils::CharIsWhiteSpace(c); });
    }
  } else if constexpr (A == Accept::AlphaNumeric) {
    if constexpr (Ascii) {
      return absl::c_any_of(bytes, absl::ascii_isalnum);
    } else {
      return AnyOfChar32(bytes, [](uint32_t c) {
        const auto g = utf8_utils::CharPrimaryCategory(c);
        return g == 'L' || g == 'N';
      });
    }
  } else {
    if constexpr (Ascii) {
      return absl::c_any_of(bytes, absl::ascii_isalpha);
    } else {
      return AnyOfChar32(bytes, [](uint32_t c) {
        return utf8_utils::CharPrimaryCategory(c) == 'L';
      });
    }
  }
}

template<Accept A>
bool AcceptSegment(const char* data,
                   const words::UnicodeSegment& seg) noexcept {
  if constexpr (A == Accept::Any) {
    return true;
  } else {
    if constexpr (A != Accept::Graphic) {
      if (seg.has_ascii_alpha) {
        return true;
      }
      if constexpr (A == Accept::AlphaNumeric) {
        if (seg.has_ascii_digit) {
          return true;
        }
      }
      if (seg.ascii_only) {
        return false;
      }
    }
    const std::string_view bytes{data + seg.begin, seg.end - seg.begin};
    return seg.ascii_only ? AcceptBytes<A, true>(bytes)
                          : AcceptBytes<A, false>(bytes);
  }
}

template<typename Finder, typename OnMatch>
void ForEachSzMatch(Finder finder, const char* data, size_t n,
                    OnMatch&& on_match) {
  constexpr size_t kBatch = 64;
  size_t starts[kBatch];
  size_t lengths[kBatch];
  size_t offset = 0;
  while (offset < n) {
    size_t consumed = 0;
    const size_t count =
      finder(data + offset, n - offset, starts, lengths, kBatch, &consumed);
    for (size_t k = 0; k < count; ++k) {
      const size_t begin = offset + starts[k];
      on_match(begin, begin + lengths[k]);
    }
    if (consumed == 0) {
      break;
    }
    offset += consumed;
  }
}

template<TokenLayout Layout, Convert C, bool Ascii>
IRS_FORCE_INLINE void EmitConverted(TokenSink& sink, const char* data,
                                    uint32_t value_size, uint32_t begin,
                                    uint32_t end) {
  if constexpr (C == Convert::None) {
    sink.EmitSlice<Layout>(data, data + value_size, Offs{begin, end});
  } else if constexpr (Ascii) {
    sink.EmitSliceCaseConverted<Layout, C == Convert::Lower>(
      data, data + value_size, Offs{begin, end});
  } else {
    sink.EmitCaseConvertedUtf8<Layout, C == Convert::Lower>(
      std::string_view{data + begin, end - begin}, Offs{begin, end});
  }
}

template<TokenLayout Layout, Convert C, Accept A, bool Ascii>
IRS_FORCE_INLINE void EmitAccepted(TokenSink& sink, const char* data,
                                   uint32_t value_size, uint32_t begin,
                                   uint32_t end) {
  if constexpr (A != Accept::Any) {
    if (!AcceptBytes<A, Ascii>(std::string_view{data + begin, end - begin})) {
      return;
    }
  }
  EmitConverted<Layout, C, Ascii>(sink, data, value_size, begin, end);
}

template<TokenLayout Layout, Convert C, Accept A, bool Ascii>
IRS_FORCE_INLINE void EmitTrimmedSegment(TokenSink& sink, const char* data,
                                         uint32_t value_size, uint32_t begin,
                                         uint32_t end) {
  while (begin < end && static_cast<uint8_t>(data[begin]) <= ' ') {
    ++begin;
  }
  while (end > begin && static_cast<uint8_t>(data[end - 1]) <= ' ') {
    --end;
  }
  if (begin == end) {
    return;
  }
  EmitAccepted<Layout, C, A, Ascii>(sink, data, value_size, begin, end);
}

template<TokenLayout Layout, Convert C, Accept A, bool Ascii>
IRS_NO_INLINE void WordFillValue(TokenSink& sink, duckdb::string_t value) {
  const char* data = value.GetData();
  const uint32_t n = value.GetSize();
  if constexpr (!Ascii) {
    words::ScanUnicode(value, [&](const words::UnicodeSegment& seg) {
      if (!AcceptSegment<A>(data, seg)) {
        return;
      }
      if constexpr (C == Convert::None) {
        EmitConverted<Layout, C, true>(sink, data, n, seg.begin, seg.end);
      } else if (seg.ascii_only) [[likely]] {
        EmitConverted<Layout, C, true>(sink, data, n, seg.begin, seg.end);
      } else {
        EmitConverted<Layout, C, false>(sink, data, n, seg.begin, seg.end);
      }
    });
  } else if constexpr (A == Accept::AlphaNumeric || A == Accept::Alpha) {
    const auto accept = [](const words::AsciiSegment& seg) IRS_FORCE_INLINE {
      if constexpr (A == Accept::AlphaNumeric) {
        return seg.has_alpha || seg.has_digit;
      } else {
        return seg.has_alpha;
      }
    };
    words::ScanAsciiRuns(value, [&](const words::AsciiSegment& seg) {
      if (!accept(seg)) {
        return;
      }
      EmitConverted<Layout, C, true>(sink, data, n, seg.begin, seg.end);
    });
  } else {
    words::ScanAscii(value, [&](const words::AsciiSegment& seg) {
      EmitAccepted<Layout, C, A, true>(sink, data, n, seg.begin, seg.end);
    });
  }
}

template<TokenLayout Layout, Convert C, Accept A, bool Ascii>
IRS_NO_INLINE void SentenceFillValue(TokenSink& sink, duckdb::string_t value) {
  const char* data = value.GetData();
  const uint32_t n = value.GetSize();
  ForEachSzMatch(sz::Sentences, data, n, [&](size_t begin, size_t end) {
    EmitTrimmedSegment<Layout, C, A, Ascii>(
      sink, data, n, static_cast<uint32_t>(begin), static_cast<uint32_t>(end));
  });
}

template<TokenLayout Layout, Convert C, Accept A, bool Paragraph, bool Ascii>
IRS_NO_INLINE void LineFillValue(TokenSink& sink, duckdb::string_t value) {
  const char* data = value.GetData();
  const uint32_t n = value.GetSize();
  const auto emit = [&](size_t begin, size_t end) {
    EmitTrimmedSegment<Layout, C, A, Ascii>(
      sink, data, n, static_cast<uint32_t>(begin), static_cast<uint32_t>(end));
  };
  size_t seg_start = 0;
  size_t prev_end = std::numeric_limits<size_t>::max();
  size_t run_start = 0;
  size_t run_len = 0;
  ForEachSzMatch(sz::Newlines, data, n, [&](size_t m_start, size_t m_end) {
    if constexpr (!Paragraph) {
      emit(seg_start, m_start);
      seg_start = m_end;
      return;
    }
    run_len = m_start == prev_end ? run_len + 1 : 1;
    if (run_len == 1) {
      run_start = m_start;
    }
    prev_end = m_end;
    const bool ps =
      m_end - m_start == 3 && static_cast<uint8_t>(data[m_start + 2]) == 0xA9;
    if (ps) {
      emit(seg_start, m_start);
      seg_start = m_end;
      run_len = 0;
      return;
    }
    if (run_len == 2) {
      emit(seg_start, run_start);
    }
    if (run_len >= 2) {
      seg_start = m_end;
    }
  });
  emit(seg_start, n);
}

template<TokenLayout Layout, Convert C, Accept A, bool Ascii>
IRS_NO_INLINE void WholeFillValue(TokenSink& sink, duckdb::string_t value) {
  const uint32_t size = value.GetSize();
  if (size == 0) {
    return;
  }
  EmitAccepted<Layout, C, A, Ascii>(sink, value.GetData(), size, 0, size);
}

}  // namespace irs::analysis::segment
