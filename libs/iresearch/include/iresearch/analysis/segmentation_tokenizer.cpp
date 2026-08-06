////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "segmentation_tokenizer.hpp"

#include <absl/strings/ascii.h>
#include <simdutf.h>

#include <array>
#include <boost/text/case_mapping.hpp>
#include <boost/text/word_break.hpp>
#include <string_view>

#include "basics/misc.hpp"
#include "basics/string_utils.h"
#include "iresearch/analysis/ascii_words.hpp"
#include "iresearch/analysis/term_view.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/utf8_character_utils.hpp"

namespace irs::analysis {
namespace {

using Options = SegmentationTokenizer::Options;

using namespace boost::text;

using Data =
  decltype(as_utf32(std::string_view{}.begin(), std::string_view{}.end()));
using DataIt = decltype(Data{}.begin());

enum class DataEncoding {
  Unknown = 0,
  Ascii,
  UTF32,
};

struct SeparateNone {
  static constexpr bool kWord = false;
  DataIt operator()(const DataIt&, const DataIt& end) const { return end; }
};

struct SeparateWord {
  static constexpr bool kWord = true;
  DataIt operator()(const DataIt& it, const DataIt& end) const {
    return next_word_break(it, end);
  }
};

struct DataState {
  const DataIt& begin_it;
  const DataIt& end_it;
  mutable DataEncoding data_encoding = DataEncoding::Unknown;

  const auto& begin() const { return begin_it; }
  const auto& end() const { return end_it; }

  std::string_view Bytes() const { return bytes; }

  size_t ByteSize() const { return bytes.size(); }

  bool IsAscii() const {
    if (data_encoding != DataEncoding::Unknown) [[likely]] {
      return data_encoding == DataEncoding::Ascii;
    }
    const bool is_ascii = simdutf::validate_ascii(bytes);
    data_encoding = is_ascii ? DataEncoding::Ascii : DataEncoding::UTF32;
    return is_ascii;
  }

  const std::string_view bytes{begin_it.base(), end_it.base()};
};

template<typename Separate, typename Accept>
class UnicodeAnalyzerImpl final
  : public TypedTokenizer<UnicodeAnalyzerImpl<Separate, Accept>>,
    public SegmentationTokenizer {
 public:
  UnicodeAnalyzerImpl(const Options& opts, Separate&& separate,
                      Accept&& accept) noexcept
    : _separate{std::move(separate)}, _accept{std::move(accept)}, _opts{opts} {}

  auto PrepareBatch() const { return std::tuple{_opts.convert, _opts.accept}; }

  TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  template<TokenLayout Layout, Options::Convert C, Options::Accept A>
  bool DoFill(duckdb::string_t raw, TokenSink& sink) {
    const std::string_view value{raw.GetData(), raw.GetSize()};
    if constexpr (Separate::kWord) {
      if (simdutf::validate_ascii(value.data(), value.size())) {
        AsciiFillValue<Layout, C, A>(sink, raw);
        return true;
      }
    }
    auto utf32 = as_utf32(value.begin(), value.end());
    FillValue<Layout, C>(sink, utf32.begin(), utf32.end());
    return true;
  }

 private:
  template<TokenLayout Layout, Options::Convert C, Options::Accept A>
  void AsciiFillValue(TokenSink& sink, duckdb::string_t value) {
    const std::string_view bytes_all{value.GetData(), value.GetSize()};
    ScanAsciiWords(value, [&](const AsciiSegment& seg) {
      if constexpr (A == Options::Accept::Graphic) {
        const auto bytes = bytes_all.substr(seg.begin, seg.end - seg.begin);
        if (!absl::c_any_of(bytes, absl::ascii_isgraph)) {
          return;
        }
      } else if constexpr (A == Options::Accept::AlphaNumeric) {
        if (!seg.has_alpha && !seg.has_digit) {
          return;
        }
      } else if constexpr (A == Options::Accept::Alpha) {
        if (!seg.has_alpha) {
          return;
        }
      }
      const uint32_t size = seg.end - seg.begin;
      if constexpr (C == Options::Convert::None) {
        sink.Emit<Layout>(MakeTermView(bytes_all.data() + seg.begin, size,
                                       bytes_all.data() + bytes_all.size()),
                          Offs{seg.begin, seg.end});
      } else if (size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
        sink.Emit<Layout>(
          FoldTermViewAscii<C == Options::Convert::Lower>(
            MakeTermView(bytes_all.data() + seg.begin, size,
                         bytes_all.data() + bytes_all.size())),
          Offs{seg.begin, seg.end});
      } else {
        sink.Emit<Layout>(
          size,
          [&](byte_type* out) IRS_FORCE_INLINE {
            if constexpr (C == Options::Convert::Lower) {
              absl::ascii_internal::AsciiStrToLower(
                reinterpret_cast<char*>(out), bytes_all.data() + seg.begin, size);
            } else {
              absl::ascii_internal::AsciiStrToUpper(
                reinterpret_cast<char*>(out), bytes_all.data() + seg.begin, size);
            }
            return size;
          },
          Offs{seg.begin, seg.end});
      }
    });
  }

  template<TokenLayout Layout, Options::Convert C>
  void FillValue(TokenSink& sink, DataIt begin, DataIt end) {
    uint32_t off_end = 0;
    while (true) {
      const auto tok_begin = begin;
      begin = _separate(begin, end);

      const auto length = static_cast<size_t>(begin.base() - tok_begin.base());
      if (length == 0) {
        return;
      }
      SDB_ASSERT(length <= std::numeric_limits<uint32_t>::max());
      const auto off_start = off_end;
      off_end += static_cast<uint32_t>(length);

      const bool optimize_accept = length <= kMaxStringSizeToOptimizeAccept;
      DataState state{
        tok_begin,
        begin,
        optimize_accept ? DataEncoding::Unknown : DataEncoding::UTF32,
      };
      if (!_accept(state)) {
        continue;
      }
      if (!optimize_accept) {
        state.data_encoding = DataEncoding::Unknown;
      }

      const auto bytes = state.Bytes();
      if constexpr (C == Options::Convert::None) {
        sink.Emit<Layout>(
          MakeTermView(bytes.data(), static_cast<uint32_t>(bytes.size()),
                       std::to_address(end.base())),
          Offs{off_start, off_end});
      } else if (state.IsAscii()) {
        // ascii fold straight into the sink, no intermediate buffer
        const auto size = static_cast<uint32_t>(bytes.size());
        sink.Emit<Layout>(
          size,
          [&](byte_type* mem) IRS_FORCE_INLINE {
            if constexpr (C == Options::Convert::Lower) {
              absl::ascii_internal::AsciiStrToLower(
                reinterpret_cast<char*>(mem), bytes.data(), size);
            } else {
              absl::ascii_internal::AsciiStrToUpper(
                reinterpret_cast<char*>(mem), bytes.data(), size);
            }
            return size;
          },
          Offs{off_start, off_end});
      } else {
        // unicode fold expands unpredictably: grow in the scratch, intern
        _term_buf.clear();
        if constexpr (C == Options::Convert::Lower) {
          to_lower(state.begin(), state.begin(), state.end(),
                   from_utf32_back_inserter(_term_buf));
        } else {
          to_upper(state.begin(), state.begin(), state.end(),
                   from_utf32_back_inserter(_term_buf));
        }
        sink.Emit<Layout>(
          _term_buf.size(),
          [&](byte_type* mem) IRS_FORCE_INLINE {
            std::memcpy(mem, _term_buf.data(), _term_buf.size());
            return static_cast<uint32_t>(_term_buf.size());
          },
          Offs{off_start, off_end});
      }
    }
  }

  [[no_unique_address]] Separate _separate;
  [[no_unique_address]] Accept _accept;

  Options _opts;
};

}  // namespace
}  // namespace irs::analysis
namespace irs {

template<typename Separate, typename Accept>
struct Type<analysis::UnicodeAnalyzerImpl<Separate, Accept>>
  : Type<analysis::SegmentationTokenizer> {};

}  // namespace irs
namespace irs::analysis {

Tokenizer::ptr SegmentationTokenizer::Make(Options options) {
  auto make_analyzer = [&]<typename... Args>(Args&&... args) {
    return Tokenizer::ptr{new UnicodeAnalyzerImpl{options, std::move(args)...}};
  };

  auto make_accept = [&]<typename Separate>(Separate&& separate) {
    switch (options.accept) {
      case Options::Accept::Any:
        return make_analyzer(std::move(separate),
                             [](DataState&) { return true; });
      case Options::Accept::Graphic:
        return make_analyzer(std::move(separate), [](DataState& state) {
          if (state.IsAscii()) {
            return absl::c_any_of(state.Bytes(), absl::ascii_isgraph);
          }
          return !absl::c_all_of(state, utf8_utils::CharIsWhiteSpace);
        });
      case Options::Accept::AlphaNumeric:
        return make_analyzer(std::move(separate), [](DataState& state) {
          if (state.IsAscii()) {
            return absl::c_any_of(state.Bytes(), absl::ascii_isalnum);
          }
          return absl::c_any_of(state, [](auto c) {
            const auto g = utf8_utils::CharPrimaryCategory(c);
            return g == 'L' || g == 'N';
          });
        });
      case Options::Accept::Alpha:
        return make_analyzer(std::move(separate), [](DataState& state) {
          if (state.IsAscii()) {
            return absl::c_any_of(state.Bytes(), absl::ascii_isalpha);
          }
          return absl::c_any_of(state, [](auto c) {
            const auto g = utf8_utils::CharPrimaryCategory(c);
            return g == 'L';
          });
        });
    }
  };
  switch (options.separate) {
    case Options::Separate::None:
      return make_accept(SeparateNone{});
    case Options::Separate::Word:
      return make_accept(SeparateWord{});
  }
}

}  // namespace irs::analysis
