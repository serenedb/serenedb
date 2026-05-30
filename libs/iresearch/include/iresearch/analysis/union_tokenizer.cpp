////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "union_tokenizer.hpp"

#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/iterator.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kUnionParamName = "union";
constexpr std::string_view kTypeParamName = "type";
constexpr std::string_view kPropertiesParamName = "properties";

using OptionsNormalize = std::vector<std::pair<std::string, std::string>>;

template<typename T>
bool ParseVPackOptions(const vpack::Slice slice, T& options) {
  if constexpr (std::is_same_v<T, UnionTokenizer::OptionsT>) {
    SDB_ASSERT(options.empty());
  }
  if (!slice.isObject()) {
    SDB_ERROR(IRESEARCH,
              "Not a VPack object passed while constructing union_tokenizer");
    return false;
  }

  if (auto union_slice = slice.get(kUnionParamName); !union_slice.isNone()) {
    if (union_slice.isArray()) {
      for (const auto member : vpack::ArrayIterator(union_slice)) {
        if (member.isObject()) {
          std::string_view type;
          if (auto type_attr_slice = member.get(kTypeParamName);
              !type_attr_slice.isNone()) {
            if (type_attr_slice.isString()) {
              type = type_attr_slice.stringView();
            } else {
              SDB_ERROR(IRESEARCH, "Failed to read '",
                        kTypeParamName, "' attribute of '", kUnionParamName,
                        "' member as string while constructing "
                        "union_tokenizer from VPack arguments");
              return false;
            }
          } else {
            SDB_ERROR(IRESEARCH, "Failed to get '",
                      kTypeParamName, "' attribute of '", kUnionParamName,
                      "' member while constructing union_tokenizer "
                      "from VPack arguments");
            return false;
          }
          if (auto properties_attr_slice = member.get(kPropertiesParamName);
              !properties_attr_slice.isNone()) {
            if constexpr (std::is_same_v<T, UnionTokenizer::OptionsT>) {
              auto analyzer =
                analyzers::Get(type, irs::Type<text_format::VPack>::get(),
                               {properties_attr_slice.startAs<char>(),
                                properties_attr_slice.byteSize()});

              // fallback to json format if vpack isn't available
              if (!analyzer) {
                analyzer =
                  analyzers::Get(type, irs::Type<text_format::Json>::get(),
                                 slice_to_string(properties_attr_slice));
              }

              if (analyzer) {
                options.push_back(std::move(analyzer));
              } else {
                SDB_ERROR(IRESEARCH,
                          "Failed to create union member of type '", type,
                          "' with properties '",
                          slice_to_string(properties_attr_slice),
                          "' while constructing union_tokenizer "
                          "from VPack arguments");
                return false;
              }
            } else {
              std::string normalized;
              if (analyzers::Normalize(normalized, type,
                                       irs::Type<text_format::VPack>::get(),
                                       {properties_attr_slice.startAs<char>(),
                                        properties_attr_slice.byteSize()})) {
                options.emplace_back(std::piecewise_construct,
                                     std::forward_as_tuple(type),
                                     std::forward_as_tuple(normalized));

                // fallback to json format if vpack isn't available
              } else if (analyzers::Normalize(
                           normalized, type,
                           irs::Type<text_format::Json>::get(),
                           slice_to_string(properties_attr_slice))) {
                // in options we'll store vpack as string
                auto vpack = vpack::Parser::fromJson(normalized.c_str(),
                                                     normalized.size());
                options.emplace_back(
                  std::piecewise_construct, std::forward_as_tuple(type),
                  std::forward_as_tuple(vpack->slice().startAs<char>(),
                                        vpack->slice().byteSize()));
              } else {
                SDB_ERROR(IRESEARCH,
                          "Failed to normalize union member of type '", type,
                          "' with properties '",
                          slice_to_string(properties_attr_slice),
                          "' while constructing union_tokenizer "
                          "from VPack arguments");
                return false;
              }
            }
          } else {
            SDB_ERROR(IRESEARCH, "Failed to get '",
                      kPropertiesParamName, "' attribute of '", kUnionParamName,
                      "' member while constructing union_tokenizer "
                      "from VPack arguments");
            return false;
          }
        } else {
          SDB_ERROR(IRESEARCH, "Failed to read '",
                    kUnionParamName,
                    "' member as object while constructing "
                    "union_tokenizer from VPack arguments");
          return false;
        }
      }
    } else {
      SDB_ERROR(IRESEARCH, "Failed to read '",
                kUnionParamName,
                "' attribute as array while constructing "
                "union_tokenizer from VPack arguments");
      return false;
    }
  } else {
    SDB_ERROR(IRESEARCH, "Not found parameter '",
              kUnionParamName, "' while constructing union_tokenizer");
    return false;
  }
  if (options.empty()) {
    SDB_ERROR(IRESEARCH,
              "Empty union found while constructing union_tokenizer");
    return false;
  }
  return true;
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  OptionsNormalize options;
  if (ParseVPackOptions(slice, options)) {
    vpack::ObjectBuilder object(builder);
    {
      vpack::ArrayBuilder array(builder, kUnionParamName.data());
      {
        for (const auto& analyzer : options) {
          vpack::ObjectBuilder analyzers_obj(builder);
          {
            builder->add(kTypeParamName, analyzer.first);
            vpack::Slice sub_slice(
              reinterpret_cast<const uint8_t*>(analyzer.second.c_str()));
            builder->add(kPropertiesParamName, sub_slice);
          }
        }
      }
    }
    return true;
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

Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  UnionTokenizer::OptionsT options;
  if (ParseVPackOptions(slice, options)) {
    return std::make_unique<UnionTokenizer>(std::move(options));
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

Analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      SDB_ERROR(IRESEARCH,
                "Null arguments while constructing union_tokenizer");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(IRESEARCH, "Caught error '", ex.what(),
              "' while constructing union_tokenizer from JSON");
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              "Caught error while constructing union_tokenizer from JSON");
  }
  return nullptr;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR(IRESEARCH,
                "Null arguments while normalizing union_tokenizer");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    vpack::Builder builder;
    if (NormalizeVPackConfig(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(IRESEARCH, "Caught error '", ex.what(),
              "' while normalizing union_tokenizer from JSON");
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              "Caught error while normalizing union_tokenizer from JSON");
  }
  return false;
}

PayAttr* FindPayload(std::span<const Analyzer::ptr> subs) {
  for (auto it = subs.rbegin(); it != subs.rend(); ++it) {
    auto* payload = irs::GetMutable<PayAttr>(it->get());
    if (payload) {
      return payload;
    }
  }
  return nullptr;
}

}  // namespace

UnionTokenizer::UnionTokenizer(UnionTokenizer::OptionsT&& options)
  : _attrs{{}, {}, FindPayload(options) ? &_payload : AttributePtr<PayAttr>{}} {
  _subs.reserve(options.size());
  for (auto& p : options) {
    SDB_ASSERT(p);
    _subs.emplace_back(std::move(p));
  }
  options.clear();  // mimic move semantic
  if (_subs.empty()) {
    _subs.emplace_back();
  }
}

uint32_t UnionTokenizer::FindMinPosition() const noexcept {
  uint32_t min_pos = std::numeric_limits<uint32_t>::max();
  for (const auto& sub : _subs) {
    if (sub.has_token && sub.position < min_pos) {
      min_pos = sub.position;
    }
  }
  return min_pos;
}

// Interleaves tokens from all sub-tokenizers by position.
// At each position, tokens are emitted in sub-tokenizer index order (0,
// 1, 2...). Within a single sub, all same-position tokens (inc=0) are emitted
// before moving to the next sub.
//
// IncAttr is computed as delta from the last emitted union position:
//   inc = current_min_pos - last_emitted_pos
// This is always valid because positions are monotonically non-decreasing.
bool UnionTokenizer::next() {
  for (;;) {
    // Scan from _emit_index for a sub at _current_min_pos
    while (_emit_index < _subs.size()) {
      auto& sub = _subs[_emit_index];
      if (sub.has_token && sub.position == _current_min_pos) {
        // Copy term bytes into owning buffer
        _term_buf.assign(sub.term->value.begin(), sub.term->value.end());
        std::get<TermAttr>(_attrs).value = bytes_view{_term_buf};

        // Copy payload if this sub has one, else clear
        if (sub.pay) {
          _payload_buf.assign(sub.pay->value.begin(), sub.pay->value.end());
          _payload.value = bytes_view{_payload_buf};
        } else {
          _payload.value = {};
        }

        // IncAttr: delta from last emitted position
        SDB_ASSERT(_current_min_pos >= _last_emitted_pos);
        std::get<IncAttr>(_attrs).value = _current_min_pos - _last_emitted_pos;
        _last_emitted_pos = _current_min_pos;

        // Advance this sub.
        sub.Advance();

        // Stay at this index if sub still has a token at _current_min_pos
        // (handles inc=0 tokens within a single sub-tokenizer)
        if (!(sub.has_token && sub.position == _current_min_pos)) {
          ++_emit_index;
        }
        return true;
      }
      ++_emit_index;
    }

    // All subs at _current_min_pos exhausted; find next minimum.
    _current_min_pos = FindMinPosition();
    if (_current_min_pos == std::numeric_limits<uint32_t>::max()) {
      return false;  // all sub-tokenizers exhausted
    }
    _emit_index = 0;
  }
}

bool UnionTokenizer::reset(std::string_view data) {
  bool any_has_token = false;
  for (auto& sub : _subs) {
    if (sub.DoReset(data)) {
      any_has_token = true;
    }
  }
  _last_emitted_pos = 0;
  _emit_index = 0;
  _current_min_pos = FindMinPosition();
  return any_has_token;
}

void UnionTokenizer::init() {
  REGISTER_ANALYZER_JSON(UnionTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(UnionTokenizer, MakeVPack, NormalizeVPackConfig);
}

UnionTokenizer::SubAnalyzer::SubAnalyzer(Analyzer::ptr a)
  : term(irs::get<TermAttr>(*a)),
    inc(irs::get<IncAttr>(*a)),
    pay(irs::GetMutable<PayAttr>(a.get())),
    _analyzer(std::move(a)) {
  SDB_ASSERT(inc);
  SDB_ASSERT(term);
}

UnionTokenizer::SubAnalyzer::SubAnalyzer()
  : _analyzer(std::make_unique<EmptyAnalyzer>()) {}

bool UnionTokenizer::SubAnalyzer::DoReset(std::string_view data) {
  position = 0;
  has_token = false;
  if (!_analyzer->reset(data)) {
    return false;
  }
  if (_analyzer->next()) {
    position = inc->value;  // typically 1 (first valid position)
    has_token = true;
    return true;
  }
  return false;
}

bool UnionTokenizer::SubAnalyzer::Advance() {
  if (_analyzer->next()) {
    position += inc->value;
    has_token = true;
    return true;
  }
  has_token = false;
  return false;
}

}  // namespace irs::analysis
