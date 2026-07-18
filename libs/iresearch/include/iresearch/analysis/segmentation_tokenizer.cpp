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

#include <boost/text/case_mapping.hpp>
#include <boost/text/word_break.hpp>
#include <string_view>

#include "basics/misc.hpp"
#include "basics/string_utils.h"
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
    const bool is_ascii = simdutf::validate_ascii(Bytes());
    data_encoding = is_ascii ? DataEncoding::Ascii : DataEncoding::UTF32;
    return is_ascii;
  }
};

template<typename Separate, typename Accept, typename Convert>
class UnicodeAnalyzerImpl final : public SegmentationTokenizer {
 public:
  UnicodeAnalyzerImpl(Separate&& separate, Accept&& accept, Convert&& convert,
                      bool use_ascii_optimization) noexcept
    : _separate{std::move(separate)},
      _accept{std::move(accept)},
      _convert{std::move(convert)},
      _use_ascii_optimization{use_ascii_optimization} {}

  bool reset(std::string_view data) final {
    auto utf32 = as_utf32(data.begin(), data.end());
    _begin = utf32.begin();
    _end = utf32.end();
    std::get<OffsAttr>(_attrs).end = 0;
    return true;
  }

  bool next() final {
    auto& offset = std::get<OffsAttr>(_attrs);
    while (true) {
      const auto begin = _begin;
      _begin = _separate(_begin, _end);

      const auto length = static_cast<size_t>(_begin.base() - begin.base());
      if (length == 0) {
        return false;
      }
      SDB_ASSERT(length <= std::numeric_limits<uint32_t>::max());

      offset.start = offset.end;
      offset.end += static_cast<uint32_t>(length);
      SDB_ASSERT(offset.start < offset.end);

      const bool optimize_accept = length <= kMaxStringSizeToOptimizeAccept;
      DataState state{
        begin,
        _begin,
        _use_ascii_optimization && optimize_accept ? DataEncoding::Unknown
                                                   : DataEncoding::UTF32,
      };
      if (_accept(state)) {
        if (_use_ascii_optimization && !optimize_accept) {
          state.data_encoding = DataEncoding::Unknown;
        }
        std::get<TermAttr>(_attrs).value =
          ViewCast<byte_type>(_convert(state, _term_buf));
        return true;
      }
    }
  }

 private:
  [[no_unique_address]] Separate _separate;
  [[no_unique_address]] Accept _accept;
  [[no_unique_address]] Convert _convert;

  DataIt _begin;
  DataIt _end;
  bool _use_ascii_optimization;
};

}  // namespace

Analyzer::ptr SegmentationTokenizer::Make(Options options) {
  auto make_analyzer = [&]<typename... Args>(Args&&... args) {
    return Analyzer::ptr{new UnicodeAnalyzerImpl{std::move(args)...}};
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
