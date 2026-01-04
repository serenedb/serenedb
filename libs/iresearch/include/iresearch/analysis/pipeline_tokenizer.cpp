////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "pipeline_tokenizer.hpp"

#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/iterator.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kPipelineParamName = "pipeline";
constexpr std::string_view kTypeParamName = "type";
constexpr std::string_view kPropertiesParamName = "properties";

constexpr irs::OffsAttr kNoOffset;
using OptionsNormalize = std::vector<std::pair<std::string, std::string>>;

template<typename T>
bool ParseVPackOptions(const vpack::Slice slice, T& options) {
  if constexpr (std::is_same_v<T, PipelineTokenizer::options_t>) {
    SDB_ASSERT(options.empty());
  }
  if (!slice.isObject()) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Not a VPack object passed while constructing pipeline_token_stream ");
    return false;
  }

  if (auto pipeline_slice = slice.get(kPipelineParamName);
      !pipeline_slice.isNone()) {
    if (pipeline_slice.isArray()) {
      for (const auto pipe : vpack::ArrayIterator(pipeline_slice)) {
        if (pipe.isObject()) {
          std::string_view type;
          if (auto type_attr_slice = pipe.get(kTypeParamName);
              !type_attr_slice.isNone()) {
            if (type_attr_slice.isString()) {
              type = type_attr_slice.stringView();
            } else {
              SDB_ERROR(
                "xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to read '", kTypeParamName,
                             "' attribute of '", kPipelineParamName,
                             "' member as string while constructing "
                             "pipeline_token_stream from VPack arguments"));
              return false;
            }
          } else {
            SDB_ERROR(
              "xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to get '", kTypeParamName,
                           "' attribute of '", kPipelineParamName,
                           "' member while constructing pipeline_token_stream "
                           "from VPack arguments"));
            return false;
          }
          if (auto properties_attr_slice = pipe.get(kPropertiesParamName);
              !properties_attr_slice.isNone()) {
            if constexpr (std::is_same_v<T, PipelineTokenizer::options_t>) {
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
                SDB_ERROR(
                  "xxxxx", sdb::Logger::IRESEARCH,
                  absl::StrCat("Failed to create pipeline member of type '",
                               type, "' with properties '",
                               slice_to_string(properties_attr_slice),
                               "' while constructing pipeline_token_stream "
                               "from VPack arguments"));
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
                std::string normalized_vpack_str;
                normalized_vpack_str.assign(vpack->slice().startAs<char>(),
                                            vpack->slice().byteSize());
                options.emplace_back(
                  std::piecewise_construct, std::forward_as_tuple(type),
                  std::forward_as_tuple(vpack->slice().startAs<char>(),
                                        vpack->slice().byteSize()));
              } else {
                SDB_ERROR(
                  "xxxxx", sdb::Logger::IRESEARCH,
                  absl::StrCat("Failed to normalize pipeline member of type '",
                               type, "' with properties '",
                               slice_to_string(properties_attr_slice),
                               "' while constructing pipeline_token_stream "
                               "from VPack arguments"));
                return false;
              }
            }
          } else {
            SDB_ERROR(
              "xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to get '", kPropertiesParamName,
                           "' attribute of '", kPipelineParamName,
                           "' member while constructing pipeline_token_stream "
                           "from VPack arguments"));
            return false;
          }
        } else {
          SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                    absl::StrCat("Failed to read '", kPipelineParamName,
                                 "' member as object while constructing "
                                 "pipeline_token_stream from VPack arguments"));
          return false;
        }
      }
    } else {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to read '", kPipelineParamName,
                             "' attribute as array while constructing "
                             "pipeline_token_stream from VPack arguments"));
      return false;
    }
  } else {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Not found parameter '", kPipelineParamName,
                           "' while constructing pipeline_token_stream"));
    return false;
  }
  if (options.empty()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Empty pipeline found while constructing pipeline_token_stream");
    return false;
  }
  return true;
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  OptionsNormalize options;
  if (ParseVPackOptions(slice, options)) {
    vpack::ObjectBuilder object(builder);
    {
      vpack::ArrayBuilder array(builder, kPipelineParamName.data());
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

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a jSON encoded object with the following attributes:
/// pipeline: Array of objects containing analyzers definition inside pipeline.
/// Each definition is an object with the following attributes:
/// type: analyzer type name (one of registered analyzers type)
/// properties: object with properties for corresponding analyzer
////////////////////////////////////////////////////////////////////////////////
Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  PipelineTokenizer::options_t options;
  if (ParseVPackOptions(slice, options)) {
    return std::make_unique<PipelineTokenizer>(std::move(options));
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
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while constructing pipeline_token_stream");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing pipeline_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while constructing pipeline_token_stream from JSON");
  }
  return nullptr;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing pipeline_token_stream");
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
                   "' while normalizing pipeline_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while normalizing pipeline_token_stream from JSON");
  }
  return false;
}

irs::PayAttr* FindPayload(std::span<const Analyzer::ptr> pipeline) {
  for (auto it = pipeline.rbegin(); it != pipeline.rend(); ++it) {
    auto* payload = irs::GetMutable<irs::PayAttr>(it->get());
    if (payload) {
      return payload;
    }
  }
  return nullptr;
}

bool AllHaveOffset(std::span<const Analyzer::ptr> pipeline) {
  return absl::c_all_of(pipeline, [](const Analyzer::ptr& v) {
    return nullptr != irs::get<irs::OffsAttr>(*v);
  });
}

}  // namespace

PipelineTokenizer::PipelineTokenizer(PipelineTokenizer::options_t&& options)
  : _attrs{{},
           options.empty() ? nullptr
                           : irs::GetMutable<TermAttr>(options.back().get()),
           AllHaveOffset(options) ? &_offs : AttributePtr<OffsAttr>{},
           FindPayload(options)} {
  const auto track_offset = irs::get<OffsAttr>(*this) != nullptr;
  _pipeline.reserve(options.size());
  for (auto& p : options) {
    SDB_ASSERT(p);
    _pipeline.emplace_back(std::move(p), track_offset);
  }
  options.clear();  // mimic move semantic
  if (_pipeline.empty()) {
    _pipeline.emplace_back();
  }
  _top = _pipeline.begin();
  _bottom = --_pipeline.end();
}

/// Moves pipeline to next token.
/// Term is taken from last analyzer in pipeline
/// Offset is recalculated accordingly (only if ALL analyzers in pipeline )
/// Payload is taken from lowest analyzer having this attribute
/// Increment is calculated according to following position change rules
///  - If none of pipeline members changes position - whole pipeline holds
///  position
///  - If one or more pipeline member moves - pipeline moves( change from max->0
///  is not move, see rules below!).
///    All position gaps are accumulated (e.g. if one member has inc 2(1 pos
///    gap) and other has inc 3(2 pos gap)  - pipeline has inc 4 (1+2 pos gap))
///  - All position changes caused by parent analyzer move next (e.g. transfer
///  from max->0 by first next after reset) are collapsed.
///    e.g if parent moves after next, all its children are resetted to new
///    token and also moves step froward - this whole operation is just one step
///    for pipeline (if any of children has moved more than 1 step - gaps are
///    preserved!)
///  - If parent after next is NOT moved (inc == 0) than pipeline makes one step
///  forward if at least one child changes
///    position from any positive value back to 0 due to reset (additional gaps
///    also preserved!) as this is not change max->0 and position is indeed
///    changed.
bool PipelineTokenizer::next() {
  uint32_t pipeline_inc = 0;
  bool step_for_rollback = false;
  do {
    while (!_current->next()) {
      if (_current == _top) {
        // reached pipeline top and next has failed - we are done
        return false;
      }
      --_current;
    }
    pipeline_inc = _current->inc->value;
    const auto top_holds_position = _current->inc->value == 0;
    // go down to lowest pipe to get actual tokens
    while (_current != _bottom) {
      const auto prev_term = _current->term->value;
      const auto prev_start = _current->start();
      const auto prev_end = _current->end();
      ++_current;
      // check do we need to do step forward due to rollback to 0.
      step_for_rollback |=
        top_holds_position && _current->pos != 0 &&
        _current->pos != std::numeric_limits<uint32_t>::max();
      if (!_current->reset(prev_start, prev_end, ViewCast<char>(prev_term))) {
        return false;
      }
      if (!_current->next()) {  // empty one found. Another round from the very
                                // beginning.
        SDB_ASSERT(_current != _top);
        --_current;
        break;
      }
      pipeline_inc += _current->inc->value;
      SDB_ASSERT(_current->inc->value >
                 0);  // first increment after reset should
                      // be positive to give 0 or next pos!
      SDB_ASSERT(pipeline_inc > 0);
      pipeline_inc--;  // compensate placing sub_analyzer from max to 0 due to
                       // reset as this step actually does not move whole
                       // pipeline sub analyzer just stays same pos as it`s
                       // parent (step for rollback to 0 will be done below if
                       // necessary!)
    }
  } while (_current != _bottom);
  if (step_for_rollback) {
    pipeline_inc++;
  }
  std::get<IncAttr>(_attrs).value = pipeline_inc;
  _offs.start = _current->start();
  _offs.end = _current->end();
  return true;
}

bool PipelineTokenizer::reset(std::string_view data) {
  _current = _top;
  return _pipeline.front().reset(0, static_cast<uint32_t>(data.size()), data);
}

void PipelineTokenizer::init() {
  REGISTER_ANALYZER_JSON(PipelineTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(PipelineTokenizer, MakeVPack, NormalizeVPackConfig);
}

PipelineTokenizer::SubAnalyzerT::SubAnalyzerT(Analyzer::ptr a,
                                              bool track_offset)
  : term(irs::get<TermAttr>(*a)),
    inc(irs::get<irs::IncAttr>(*a)),
    offs(track_offset ? irs::get<irs::OffsAttr>(*a) : &kNoOffset),
    _analyzer(std::move(a)) {
  SDB_ASSERT(inc);
  SDB_ASSERT(term);
}

PipelineTokenizer::SubAnalyzerT::SubAnalyzerT()
  : term(nullptr),
    inc(nullptr),
    offs(nullptr),
    _analyzer(std::make_unique<EmptyAnalyzer>()) {}

}  // namespace irs::analysis
