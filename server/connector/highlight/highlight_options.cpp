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

#include "connector/highlight/highlight_options.h"

#include <array>
#include <span>

#include "pg/option_help.h"
#include "pg/options_parser.h"

namespace sdb::connector::highlight {
namespace {

using namespace std::string_view_literals;

inline constexpr pg::OptionInfo kStartSel{"startsel", "<b>"sv,
                                          "Wrap before each matched range"};
inline constexpr pg::OptionInfo kStopSel{"stopsel", "</b>"sv,
                                         "Wrap after each matched range"};
inline constexpr pg::OptionInfo kMaxWords{
  "maxwords", 35, "Hard cap on tokens per fragment", pg::CheckPositiveInt};
inline constexpr pg::OptionInfo kHighlightAll{
  "highlightall", false, "Whole document, no passage selection"};
inline constexpr pg::OptionInfo kMaxFragments{
  "maxfragments", 0,
  "0 = single best fragment; >0 = up to N top-scored fragments",
  pg::CheckNonNegativeInt};
inline constexpr pg::OptionInfo kFragmentDelimiter{
  "fragmentdelimiter", " ... "sv, "Joiner between fragments"};
inline constexpr pg::OptionInfo kMaxOffsets{
  "maxoffsets", 0,
  "Limit on offset pairs the synthesised ts_offsets() emits per doc "
  "(0 = unlimited)",
  pg::CheckNonNegativeInt};

inline constexpr std::array kOptions = {
  kStartSel,     kStopSel,           kMaxWords,   kHighlightAll,
  kMaxFragments, kFragmentDelimiter, kMaxOffsets,
};

inline constexpr pg::OptionGroup kGroup{
  .name = "ts_highlight",
  .options = std::span<const pg::OptionInfo>{kOptions},
};

class HighlightOptionsParser final : public pg::OptionsParser {
 public:
  using pg::OptionsParser::OptionsParser;

  HighlightOptions Parse() {
    HighlightOptions opts;
    ParseOptions([&] {
      opts.start_sel = EraseOptionOrDefault<kStartSel, std::string>();
      opts.stop_sel = EraseOptionOrDefault<kStopSel, std::string>();
      opts.fragment_delim =
        EraseOptionOrDefault<kFragmentDelimiter, std::string>();
      // Negatives rejected upstream by the constraint fns.
      opts.max_words = static_cast<uint32_t>(EraseOptionOrDefault<kMaxWords>());
      opts.max_fragments =
        static_cast<uint32_t>(EraseOptionOrDefault<kMaxFragments>());
      opts.max_offsets =
        static_cast<uint32_t>(EraseOptionOrDefault<kMaxOffsets>());
      opts.highlight_all = EraseOptionOrDefault<kHighlightAll>();
    });
    return opts;
  }
};

}  // namespace

HighlightOptions ParseHighlightOptions(std::string_view text) {
  HighlightOptionsParser parser{
    pg::OptionsParser::MakeOptions(text), kGroup,
    pg::OptionsContext{.operation = "ts_highlight"}};
  return parser.Parse();
}

}  // namespace sdb::connector::highlight
