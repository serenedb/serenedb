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
#include "iresearch/analysis/batch/ascii_words.hpp"
#include "iresearch/analysis/batch/term_view.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
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

struct DataState {
  const DataIt& begin_it;
  const DataIt& end_it;
  mutable DataEncoding data_encoding = DataEncoding::Unknown;

  const auto& begin() const { return begin_it; }
  const auto& end() const { return end_it; }

  std::string_view Bytes() const { return {begin_it.base(), end_it.base()}; }

  size_t ByteSize() const { return Bytes().size(); }

  bool IsAscii() const {
    if (data_encoding != DataEncoding::Unknown) [[likely]] {
      return data_encoding == DataEncoding::Ascii;
    }
    const bool is_ascii = absl::c_all_of(Bytes(), absl::ascii_isascii);
    data_encoding = is_ascii ? DataEncoding::Ascii : DataEncoding::UTF32;
    return is_ascii;
  }
};

template<typename Separate, typename Accept, typename Convert>
class UnicodeAnalyzerImpl final
  : public TypedTokenizer<UnicodeAnalyzerImpl<Separate, Accept, Convert>>,
    public SegmentationTokenizer {
 public:
  UnicodeAnalyzerImpl(const Options& opts, Separate&& separate, Accept&& accept,
                      Convert&& convert, bool use_ascii_optimization) noexcept
    : _separate{std::move(separate)},
      _accept{std::move(accept)},
      _convert{std::move(convert)},
      _opts{opts},
      _use_ascii_optimization{use_ascii_optimization} {}

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink) {
    if (_opts.separate == Options::Separate::Word && !_force_unicode &&
        simdutf::validate_ascii(value.data(), value.size())) {
      AsciiFillValue<Layout>(sink, value);
      return true;
    }
    auto utf32 = as_utf32(value.begin(), value.end());
    FillValue<Layout>(sink, utf32.begin(), utf32.end());
    return true;
  }

 private:
  template<TokenLayout Layout>
  void AsciiFillValue(TokenEmitter& sink, std::string_view value) {
    auto& buf = sink.buf;
    ScanAsciiWords(value, [&](const AsciiSegment& seg) {
      switch (_opts.accept) {
        case Options::Accept::Any:
          break;
        case Options::Accept::Graphic: {
          const auto bytes = value.substr(seg.begin, seg.end - seg.begin);
          if (!absl::c_any_of(bytes, absl::ascii_isgraph)) {
            return;
          }
        } break;
        case Options::Accept::AlphaNumeric:
          if (!seg.has_alpha && !seg.has_digit) {
            return;
          }
          break;
        case Options::Accept::Alpha:
          if (!seg.has_alpha) {
            return;
          }
          break;
      }
      const uint32_t size = seg.end - seg.begin;
      const auto i = sink.Next();
      if (_opts.convert == Options::Convert::None) {
        buf.terms[i] = MakeTermView(value.data() + seg.begin, size);
      } else {
        char inline_buf[duckdb::string_t::INLINE_LENGTH];
        char* out = size <= duckdb::string_t::INLINE_LENGTH
                      ? inline_buf
                      : reinterpret_cast<char*>(sink.Reserve(size));
        if (_opts.convert == Options::Convert::Lower) {
          absl::ascii_internal::AsciiStrToLower(out, value.data() + seg.begin,
                                                size);
        } else {
          absl::ascii_internal::AsciiStrToUpper(out, value.data() + seg.begin,
                                                size);
        }
        buf.terms[i] = MakeTermView(out, size);
      }
      if constexpr (Layout == TokenLayout::TermsPosOffs) {
        buf.offs_start[i] = seg.begin;
        buf.offs_end[i] = seg.end;
      }
    });
  }

  template<TokenLayout Layout>
  void FillValue(TokenEmitter& sink, DataIt begin, DataIt end) {
    auto& buf = sink.buf;
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
        _use_ascii_optimization && optimize_accept ? DataEncoding::Unknown
                                                   : DataEncoding::UTF32,
      };
      if (!_accept(state)) {
        continue;
      }
      if (_use_ascii_optimization && !optimize_accept) {
        state.data_encoding = DataEncoding::Unknown;
      }
      const auto term = ViewCast<byte_type>(_convert(state, _term_buf));
      const auto i = sink.Next();
      buf.terms[i] =
        reinterpret_cast<const char*>(term.data()) == _term_buf.data()
          ? sink.Intern(term)
          : duckdb::string_t{reinterpret_cast<const char*>(term.data()),
                             static_cast<uint32_t>(term.size())};
      if constexpr (Layout == TokenLayout::TermsPosOffs) {
        buf.offs_start[i] = off_start;
        buf.offs_end[i] = off_end;
      }
    }
  }

  [[no_unique_address]] Separate _separate;
  [[no_unique_address]] Accept _accept;
  [[no_unique_address]] Convert _convert;

  Options _opts;
  bool _use_ascii_optimization;
};

}  // namespace
}  // namespace irs::analysis
namespace irs {

template<typename Separate, typename Accept, typename Convert>
struct Type<analysis::UnicodeAnalyzerImpl<Separate, Accept, Convert>>
  : Type<analysis::SegmentationTokenizer> {};

}  // namespace irs
namespace irs::analysis {

Tokenizer::ptr SegmentationTokenizer::Make(Options options) {
  auto make_analyzer = [&]<typename... Args>(Args&&... args) {
    return Tokenizer::ptr{new UnicodeAnalyzerImpl{options, std::move(args)...}};
  };
  auto make_convert = [&]<typename... Args>(Args&&... args) {
    switch (options.convert) {
      case Options::Convert::None:
        return make_analyzer(
          std::move(args)...,
          [](DataState& state, std::string&) {
            return ViewCast<byte_type>(state.Bytes());
          },
          /*use_ascii_optimization=*/true);
      case Options::Convert::Lower:
        return make_analyzer(
          std::move(args)...,
          [](DataState& state, std::string& buf) -> std::string_view {
            if (state.IsAscii()) {
              const auto byte_size = state.ByteSize();
              sdb::basics::StrResizeAmortized(buf, byte_size);
              absl::ascii_internal::AsciiStrToLower(
                buf.data(), &*state.begin().base(), byte_size);
            } else {
              buf.clear();
              to_lower(state.begin(), state.begin(), state.end(),
                       from_utf32_back_inserter(buf));
            }
            return buf;
          },
          /*use_ascii_optimization=*/true);
      case Options::Convert::Upper:
        return make_analyzer(
          std::move(args)...,
          [](DataState& state, std::string& buf) -> std::string_view {
            if (state.IsAscii()) {
              const auto byte_size = state.ByteSize();
              sdb::basics::StrResizeAmortized(buf, byte_size);
              absl::ascii_internal::AsciiStrToUpper(
                buf.data(), &*state.begin().base(), byte_size);
            } else {
              buf.clear();
              to_upper(state.begin(), state.begin(), state.end(),
                       from_utf32_back_inserter(buf));
            }
            return buf;
          },
          /*use_ascii_optimization=*/true);
    }
  };

  auto make_accept = [&]<typename Separate>(Separate&& separate) {
    switch (options.accept) {
      case Options::Accept::Any:
        return make_convert(std::move(separate),
                            [](DataState&) { return true; });
      case Options::Accept::Graphic:
        return make_convert(std::move(separate), [](DataState& state) {
          if (state.IsAscii()) {
            return absl::c_any_of(state.Bytes(), absl::ascii_isgraph);
          }
          return !absl::c_all_of(state, utf8_utils::CharIsWhiteSpace);
        });
      case Options::Accept::AlphaNumeric:
        return make_convert(std::move(separate), [](DataState& state) {
          if (state.IsAscii()) {
            return absl::c_any_of(state.Bytes(), absl::ascii_isalnum);
          }
          return absl::c_any_of(state, [](auto c) {
            const auto g = utf8_utils::CharPrimaryCategory(c);
            return g == 'L' || g == 'N';
          });
        });
      case Options::Accept::Alpha:
        return make_convert(std::move(separate), [](DataState& state) {
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
      return make_accept([](const DataIt&, const DataIt& end) { return end; });
    case Options::Separate::Word:
      return make_accept([](const DataIt& it, const DataIt& end) {
        return next_word_break(it, end);
      });
  }
}

}  // namespace irs::analysis
