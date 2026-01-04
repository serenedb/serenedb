////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "ngram_tokenizer.hpp"

#include <frozen/unordered_map.h>
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

#include "basics/utf8_utils.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kMinParamName = "min";
constexpr std::string_view kMaxParamName = "max";
constexpr std::string_view kPreserveOriginalParamName = "preserveOriginal";
constexpr std::string_view kStreamTypeParamName = "streamType";
constexpr std::string_view kStartMarkerParamName = "startMarker";
constexpr std::string_view kEndMarkerParamName = "endMarker";

constexpr frozen::unordered_map<std::string_view, NGramTokenizerBase::InputType,
                                2>
  kStreamTypeConvertMap = {{"binary", NGramTokenizerBase::InputType::Binary},
                           {"utf8", NGramTokenizerBase::InputType::UTF8}};

bool ParseVPackOptions(const vpack::Slice slice,
                       NGramTokenizerBase::Options& options) {
  if (!slice.isObject()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Slice for ngram_token_stream is not an object");
    return false;
  }

  uint64_t min = 0, max = 0;
  bool preserve_original = false;
  auto stream_bytes_type = NGramTokenizerBase::InputType::Binary;
  std::string_view start_marker, end_marker;

  // min
  auto min_type_slice = slice.get(kMinParamName);
  if (min_type_slice.isNone()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to read '", kMinParamName,
                           "' attribute as number while constructing "
                           "ngram_token_stream from VPack arguments"));
    return false;
  }

  if (!min_type_slice.isNumber()) {
    SDB_WARN(
      "xxxxx", sdb::Logger::IRESEARCH, "Invalid type '", kMinParamName,
      "' (unsigned int expected) for ngram_token_stream from VPack arguments");
    return false;
  }
  min = min_type_slice.getNumber<decltype(min)>();

  // max
  auto max_type_slice = slice.get(kMaxParamName);
  if (max_type_slice.isNone()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to read '", kMaxParamName,
                           "' attribute as number while constructing "
                           "ngram_token_stream from VPack arguments"));
    return false;
  }
  if (!max_type_slice.isNumber()) {
    SDB_WARN(
      "xxxxx", sdb::Logger::IRESEARCH, "Invalid type '", kMaxParamName,
      "' (unsigned int expected) for ngram_token_stream from VPack arguments");
    return false;
  }
  max = max_type_slice.getNumber<decltype(max)>();

  min = std::max<decltype(min)>(min, 1);
  max = std::max(max, min);

  options.min_gram = min;
  options.max_gram = max;

  // preserve original
  auto preserve_type_slice = slice.get(kPreserveOriginalParamName);
  if (preserve_type_slice.isNone()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to read '", kPreserveOriginalParamName,
                           "' attribute as boolean while constructing "
                           "ngram_token_stream from VPack arguments"));
    return false;
  }
  if (!preserve_type_slice.isBool()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid type '",
             kPreserveOriginalParamName,
             "' (bool expected) for ngram_token_stream from VPack arguments");
    return false;
  }
  preserve_original = preserve_type_slice.getBool();
  options.preserve_original = preserve_original;

  // start marker
  if (auto start_marker_type_slice = slice.get(kStartMarkerParamName);
      !start_marker_type_slice.isNone()) {
    if (!start_marker_type_slice.isString()) {
      SDB_WARN(
        "xxxxx", sdb::Logger::IRESEARCH, "Invalid type '",
        kStartMarkerParamName,
        "' (string expected) for ngram_token_stream from VPack arguments");
      return false;
    }
    start_marker = start_marker_type_slice.stringView();
  }
  options.start_marker = ViewCast<byte_type>(start_marker);

  // end marker
  if (auto end_marker_type_slice = slice.get(kEndMarkerParamName);
      !end_marker_type_slice.isNone()) {
    if (!end_marker_type_slice.isString()) {
      SDB_WARN(
        "xxxxx", sdb::Logger::IRESEARCH, "Invalid type '", kEndMarkerParamName,
        "' (string expected) for ngram_token_stream from VPack arguments");
      return false;
    }
    end_marker = end_marker_type_slice.stringView();
  }
  options.end_marker = ViewCast<byte_type>(end_marker);

  // stream bytes
  if (auto stream_type_slice = slice.get(kStreamTypeParamName);
      !stream_type_slice.isNone()) {
    if (!stream_type_slice.isString()) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-string value in '",
               kStreamTypeParamName,
               "' while constructing ngram_token_stream from VPack arguments");
      return false;
    }
    auto stream_type = stream_type_slice.stringView();
    const auto* itr = kStreamTypeConvertMap.find(
      std::string_view(stream_type.data(), stream_type.size()));
    if (itr == kStreamTypeConvertMap.end()) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid value in '",
               kStreamTypeParamName,
               "' while constructing ngram_token_stream from VPack arguments");
      return false;
    }
    stream_bytes_type = itr->second;
  }
  options.stream_bytes_type = stream_bytes_type;

  return true;
}

bool MakeVPackConfig(const NGramTokenizerBase::Options& options,
                     vpack::Builder* builder) {
  // ensure disambiguating casts below are safe. Casts required for clang
  // compiler on Mac
  static_assert(sizeof(uint64_t) >= sizeof(size_t),
                "sizeof(uint64_t) >= sizeof(size_t)");

  vpack::ObjectBuilder object(builder);
  {
    // min_gram
    builder->add(kMinParamName, options.min_gram);

    // max_gram
    builder->add(kMaxParamName, options.max_gram);

    // preserve_original
    builder->add(kPreserveOriginalParamName, options.preserve_original);

    // stream type
    const auto stream_type_value =
      absl::c_find_if(kStreamTypeConvertMap, [&options](const auto& v) {
        return v.second == options.stream_bytes_type;
      });

    if (stream_type_value != kStreamTypeConvertMap.end()) {
      builder->add(kStreamTypeParamName, stream_type_value->first);
    } else {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Invalid ", kStreamTypeParamName,
                             " value in ngram analyzer options: ",
                             static_cast<int>(options.stream_bytes_type)));
      return false;
    }

    // start_marker
    builder->add(kStartMarkerParamName,
                 ViewCast<char>(bytes_view{options.start_marker}));
    // end_marker
    builder->add(kEndMarkerParamName,
                 ViewCast<char>(bytes_view{options.end_marker}));
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a jSON encoded object with the following attributes:
///        "min" (number): minimum ngram size
///        "max" (number): maximum ngram size
///        "preserveOriginal" (boolean): preserve or not the original term
////////////////////////////////////////////////////////////////////////////////
Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  NGramTokenizerBase::Options options;
  if (ParseVPackOptions(slice, options)) {
    switch (options.stream_bytes_type) {
      case NGramTokenizerBase::InputType::Binary:
        return NGramTokenizer<NGramTokenizerBase::InputType::Binary>::make(
          options);
      case NGramTokenizerBase::InputType::UTF8:
        return NGramTokenizer<NGramTokenizerBase::InputType::UTF8>::make(
          options);
    }
  }

  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}
///////////////////////////////////////////////////////////////////////////////
/// @brief builds analyzer config from internal options in json format
///////////////////////////////////////////////////////////////////////////////
bool NormalizeVPackConfig(const vpack::Slice slice,
                          vpack::Builder* vpack_builder) {
  NGramTokenizerBase::Options options;
  if (ParseVPackOptions(slice, options)) {
    return MakeVPackConfig(options, vpack_builder);
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
                "Null arguments while constructing ngram_token_stream");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing ngram_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing ngram_token_stream from JSON");
  }
  return nullptr;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing ngram_token_stream");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    vpack::Builder builder;
    if (NormalizeVPackConfig(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Caught error '", ex.what(),
                           "' while normalizing ngram_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while normalizing ngram_token_stream from JSON");
  }
  return false;
}

}  // namespace

template<NGramTokenizerBase::InputType StreamType>
Analyzer::ptr NGramTokenizer<StreamType>::make(const Options& options) {
  return std::make_unique<NGramTokenizer<StreamType>>(options);
}

void NGramTokenizerBase::init() {
  REGISTER_ANALYZER_VPACK(NGramTokenizerBase, MakeVPack, NormalizeVPackConfig);
  REGISTER_ANALYZER_JSON(NGramTokenizerBase, MakeJson, NormalizeJsonConfig);
}

NGramTokenizerBase::NGramTokenizerBase(const Options& options)
  : _options(options),
    _start_marker_empty(options.start_marker.empty()),
    _end_marker_empty(options.end_marker.empty()) {
  _options.min_gram = std::max<size_t>(_options.min_gram, 1);
  _options.max_gram = std::max(_options.max_gram, _options.min_gram);
}

template<NGramTokenizerBase::InputType StreamType>
NGramTokenizer<StreamType>::NGramTokenizer(const Options& options)
  : NGramTokenizerBase{options} {
  SDB_ASSERT(StreamType == _options.stream_bytes_type);
}

void NGramTokenizerBase::emit_original() noexcept {
  auto& term = std::get<TermAttr>(_attrs);
  auto& offset = std::get<irs::OffsAttr>(_attrs);
  auto& inc = std::get<IncAttr>(_attrs);

  switch (_emit_original) {
    case EmitOriginal::WithoutMarkers:
      term.value = _data;
      SDB_ASSERT(_data.size() <= std::numeric_limits<uint32_t>::max());
      offset.end = uint32_t(_data.size());
      _emit_original = EmitOriginal::None;
      inc.value = _next_inc_val;
      break;
    case EmitOriginal::WithEndMarker:
      _marked_term_buffer.clear();
      SDB_ASSERT(_marked_term_buffer.capacity() >=
                 (_options.end_marker.size() + _data.size()));
      _marked_term_buffer.append(_data.data(), _data_end);
      _marked_term_buffer.append(_options.end_marker.begin(),
                                 _options.end_marker.end());
      term.value = _marked_term_buffer;
      SDB_ASSERT(_marked_term_buffer.size() <=
                 std::numeric_limits<uint32_t>::max());
      offset.start = 0;
      offset.end = uint32_t(_data.size());
      _emit_original = EmitOriginal::None;  // end marker is emitted last, so we
                                            // are done emitting original
      inc.value = _next_inc_val;
      break;
    case EmitOriginal::WithStartMarker:
      _marked_term_buffer.clear();
      SDB_ASSERT(_marked_term_buffer.capacity() >=
                 (_options.start_marker.size() + _data.size()));
      _marked_term_buffer.append(_options.start_marker.begin(),
                                 _options.start_marker.end());
      _marked_term_buffer.append(_data.data(), _data_end);
      term.value = _marked_term_buffer;
      SDB_ASSERT(_marked_term_buffer.size() <=
                 std::numeric_limits<uint32_t>::max());
      offset.start = 0;
      offset.end = uint32_t(_data.size());
      _emit_original = _options.end_marker.empty()
                         ? EmitOriginal::None
                         : EmitOriginal::WithEndMarker;
      inc.value = _next_inc_val;
      break;
    case EmitOriginal::None:
      SDB_ASSERT(false);
      break;
  }
  _next_inc_val = 0;
}

bool NGramTokenizerBase::reset(std::string_view value) noexcept {
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    // can't handle data which is longer than
    // std::numeric_limits<uint32_t>::max()
    return false;
  }

  auto& term = std::get<TermAttr>(_attrs);
  auto& offset = std::get<irs::OffsAttr>(_attrs);

  // reset term attribute
  term.value = {};

  // reset offset attribute
  offset.start = std::numeric_limits<uint32_t>::max();
  offset.end = std::numeric_limits<uint32_t>::max();

  // reset stream
  _data = ViewCast<byte_type>(value);
  _begin = _data.data();
  _ngram_end = _begin;
  _data_end = _data.data() + _data.size();
  offset.start = 0;
  _length = 0;
  if (_options.preserve_original) {
    if (!_start_marker_empty) {
      _emit_original = EmitOriginal::WithStartMarker;
    } else if (!_end_marker_empty) {
      _emit_original = EmitOriginal::WithEndMarker;
    } else {
      _emit_original = EmitOriginal::WithoutMarkers;
    }
  } else {
    _emit_original = EmitOriginal::None;
  }
  _next_inc_val = 1;
  SDB_ASSERT(_length < _options.min_gram);
  const size_t max_marker_size =
    std::max(_options.start_marker.size(), _options.end_marker.size());
  if (max_marker_size > 0) {
    // we have at least one marker. As we need to append marker to ngram and
    // provide term value as continious buffer, we can`t return pointer to some
    // byte inside input stream but rather we return pointer to buffer with
    // copied values of ngram and marker For sake of performance we allocate
    // requested memory right now
    size_t buffer_size = _options.preserve_original
                           ? _data.size()
                           : std::min(_data.size(), _options.max_gram);
    buffer_size += max_marker_size;
    _marked_term_buffer.reserve(buffer_size);
  }
  return true;
}

template<NGramTokenizerBase::InputType StreamType>
bool NGramTokenizer<StreamType>::NextSymbol(
  const byte_type*& it) const noexcept {
  SDB_ASSERT(it);
  if (it == _data_end) [[unlikely]] {
    return false;
  }
  if constexpr (StreamType == InputType::Binary) {
    ++it;
  } else if constexpr (StreamType == InputType::UTF8) {
    it = utf8_utils::Next(it, _data_end);
  }
  return true;
}

template<NGramTokenizerBase::InputType StreamType>
bool NGramTokenizer<StreamType>::next() noexcept {
  auto& term = std::get<TermAttr>(_attrs);
  auto& offset = std::get<irs::OffsAttr>(_attrs);
  auto& inc = std::get<IncAttr>(_attrs);

  while (_begin < _data_end) {
    if (_length < _options.max_gram && NextSymbol(_ngram_end)) {
      // we have next ngram from current position
      ++_length;
      if (_length >= _options.min_gram) {
        SDB_ASSERT(_begin <= _ngram_end);
        SDB_ASSERT(static_cast<size_t>(std::distance(_begin, _ngram_end)) <=
                   std::numeric_limits<uint32_t>::max());
        const auto ngram_byte_len =
          static_cast<uint32_t>(std::distance(_begin, _ngram_end));
        if (EmitOriginal::None == _emit_original || 0 != offset.start ||
            ngram_byte_len != _data.size()) {
          offset.end = offset.start + ngram_byte_len;
          inc.value = _next_inc_val;
          _next_inc_val = 0;
          if ((0 != offset.start || _start_marker_empty) &&
              (_end_marker_empty || _ngram_end != _data_end)) {
            term.value = irs::bytes_view(_begin, ngram_byte_len);
          } else if (0 == offset.start && !_start_marker_empty) {
            _marked_term_buffer.clear();
            SDB_ASSERT(_marked_term_buffer.capacity() >=
                       (_options.start_marker.size() + ngram_byte_len));
            _marked_term_buffer.append(_options.start_marker.begin(),
                                       _options.start_marker.end());
            _marked_term_buffer.append(_begin, ngram_byte_len);
            term.value = _marked_term_buffer;
            SDB_ASSERT(_marked_term_buffer.size() <=
                       std::numeric_limits<uint32_t>::max());
            if (ngram_byte_len == _data.size() && !_end_marker_empty) {
              // this term is whole original stream and we have end marker, so
              // we need to emit this term again with end marker just like
              // original, so pretend we need to emit original
              _emit_original = EmitOriginal::WithEndMarker;
            }
          } else {
            SDB_ASSERT(!_end_marker_empty && _ngram_end == _data_end);
            _marked_term_buffer.clear();
            SDB_ASSERT(_marked_term_buffer.capacity() >=
                       (_options.end_marker.size() + ngram_byte_len));
            _marked_term_buffer.append(_begin, ngram_byte_len);
            _marked_term_buffer.append(_options.end_marker.begin(),
                                       _options.end_marker.end());
            term.value = _marked_term_buffer;
          }
        } else {
          // if ngram covers original stream we need to process it specially
          emit_original();
        }
        return true;
      }
    } else if (EmitOriginal::None == _emit_original) {
      // need to move to next position
      if (!NextSymbol(_begin)) [[unlikely]] {
        return false;  // stream exhausted
      }
      _next_inc_val = 1;
      _length = 0;
      _ngram_end = _begin;
      offset.start = static_cast<uint32_t>(std::distance(_data.data(), _begin));
    } else {
      // as stream has unsigned incremet attribute
      // we cannot go back, so we must emit original before we leave start pos
      // in stream (as it starts from pos=0 in stream)
      emit_original();
      return true;
    }
  }
  return false;
}

template class NGramTokenizer<NGramTokenizerBase::InputType::Binary>;
template class NGramTokenizer<NGramTokenizerBase::InputType::UTF8>;

}  // namespace irs::analysis
