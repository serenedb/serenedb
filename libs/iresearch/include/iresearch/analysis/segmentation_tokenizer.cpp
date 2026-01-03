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

#include <frozen/unordered_map.h>
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <boost/text/case_mapping.hpp>
#include <boost/text/word_break.hpp>
#include <string_view>

#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/utf8_character_utils.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kConvertName = "case";
constexpr std::string_view kAcceptName = "break";

using Options = SegmentationTokenizer::Options;

constexpr frozen::unordered_map<std::string_view, Options::Convert, 3>
  kConvertMap = {
    {"none", Options::Convert::None},
    {"lower", Options::Convert::Lower},
    {"upper", Options::Convert::Upper},
};

constexpr frozen::unordered_map<std::string_view, Options::Accept, 3>
  kAcceptMap = {
    {"all", Options::Accept::Any},
    {"graphic", Options::Accept::Graphic},
    {"alpha", Options::Accept::AlphaNumeric},
};

bool ParseVPackOptions(const vpack::Slice slice, Options& options) {
  if (!slice.isObject()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Slice for segmentation analyzer is not an object");
    return false;
  }
  if (auto convert_slice = slice.get(kConvertName); !convert_slice.isNone()) {
    if (!convert_slice.isString()) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid type '", kConvertName,
               "' (string expected) for segmentation analyzer from "
               "VPack arguments");
      return false;
    }
    auto convert = convert_slice.stringView();
    const auto* it = kConvertMap.find(convert);
    if (it == kConvertMap.end()) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid value in '",
               kConvertName,
               "' for segmentation analyzer from VPack arguments");
      return false;
    }
    options.convert = it->second;
  }
  if (auto accept_slice = slice.get(kAcceptName); !accept_slice.isNone()) {
    if (!accept_slice.isString()) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid type '", kAcceptName,
               "' (string expected) for segmentation analyzer from "
               "VPack arguments");
      return false;
    }
    auto accept = accept_slice.stringView();
    const auto* it = kAcceptMap.find(accept);
    if (it == kAcceptMap.end()) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid value in '",
               kAcceptName, "' for segmentation analyzer from VPack arguments");
      return false;
    }
    options.accept = it->second;
  }
  return true;
}

bool MakeVPackConfig(const Options& options, vpack::Builder* builder) {
  vpack::ObjectBuilder object(builder);
  {
    const auto it = absl::c_find_if(
      kConvertMap,
      [v = options.convert](const auto& m) { return m.second == v; });
    if (it != kConvertMap.end()) {
      builder->add(kConvertName, it->first);
    } else {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid value in '",
               kConvertName,
               "' for normalizing segmentation analyzer from Value is: ",
               options.convert);
      return false;
    }
  }
  {
    const auto it = absl::c_find_if(
      kAcceptMap,
      [v = options.accept](const auto& m) { return m.second == v; });
    if (it != kAcceptMap.end()) {
      builder->add(kAcceptName, it->first);
    } else {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid value in '",
               kAcceptName,
               "' for normalizing segmentation analyzer from Value is: ",
               options.accept);
      return false;
    }
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a VPack slice object with the following attributes:
///        "case"(string enum): modify token case
///        "break"(string enum): word breaking method
////////////////////////////////////////////////////////////////////////////////
Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  try {
    Options options;
    if (!ParseVPackOptions(slice, options)) {
      return nullptr;
    }
    return SegmentationTokenizer::make(std::move(options));
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat(
        "Caught error '", ex.what(),
        "' while constructing segmentation analyzer from VPack arguments"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while constructing segmentation analyzer from VPack "
      "arguments");
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

bool NormalizeVPackConfig(const vpack::Slice slice,
                          vpack::Builder* vpack_builder) {
  Options options;
  try {
    if (ParseVPackOptions(slice, options)) {
      return MakeVPackConfig(options, vpack_builder);
    }
    return false;

  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat(
        "Caught error '", ex.what(),
        "' while normalizing segmentation analyzer from VPack arguments"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while normalizing segmentation analyzer from VPack "
              "arguments");
  }
  return false;
}

bool NormalizeVPackConfig(std::string_view args, std::string& config) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  vpack::Builder builder;
  if (NormalizeVPackConfig(slice, &builder)) {
    config.assign(builder.slice().startAs<char>(), builder.slice().byteSize());
    return true;
  }
  return false;
}

Analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while constructing segmentation analyzer");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing segmentation analyzer from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while constructing segmentation analyzer from JSON");
  }
  return nullptr;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing segmentation analyzer");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    vpack::Builder builder;
    if (NormalizeVPackConfig(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while normalizing segmentation analyzer from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while normalizing segmentation analyzer from JSON");
  }
  return false;
}

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
    std::get<irs::OffsAttr>(_attrs).end = 0;
    return true;
  }

  bool next() final {
    auto& offset = std::get<irs::OffsAttr>(_attrs);
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

void SegmentationTokenizer::init() {
  REGISTER_ANALYZER_VPACK(SegmentationTokenizer, MakeVPack,
                          NormalizeVPackConfig);
  REGISTER_ANALYZER_JSON(SegmentationTokenizer, MakeJson, NormalizeJsonConfig);
}

Analyzer::ptr SegmentationTokenizer::make(Options&& options) {
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
          options.use_ascii_optimization);
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
          options.use_ascii_optimization);
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
          options.use_ascii_optimization);
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
