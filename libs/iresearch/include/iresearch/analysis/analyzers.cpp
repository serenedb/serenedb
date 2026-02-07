////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/analysis/analyzers.hpp"

#include <vpack/builder.h>
#include <vpack/parser.h>

#include "basics/register.hpp"
#include "iresearch/analysis/classification_tokenizer.hpp"
#include "iresearch/analysis/collation_tokenizer.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/geo_analyzer.hpp"
#include "iresearch/analysis/minhash_tokenizer.hpp"
#include "iresearch/analysis/multi_delimited_tokenizer.hpp"
#include "iresearch/analysis/nearest_neighbors_tokenizer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/path_hierarchy_tokenizer.hpp"
#include "iresearch/analysis/pipeline_tokenizer.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/analysis/stemming_tokenizer.hpp"
#include "iresearch/analysis/stopwords_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

struct Key {
  explicit Key(std::string_view type, const TypeInfo& args_format)
    : type{type}, args_format{args_format} {}

  bool operator==(const Key& other) const = default;

  template<typename H>
  friend H AbslHashValue(H h, const Key& key) {
    return H::combine(std::move(h), key.args_format.id(), key.type);
  }

  const std::string_view type;
  TypeInfo args_format;
};

struct Value {
  explicit Value(factory_f factory = nullptr,
                 normalizer_f normalizer = nullptr) noexcept
    : factory{factory}, normalizer{normalizer} {}

  bool operator==(const Value& other) const noexcept = default;

  const factory_f factory;
  const normalizer_f normalizer;
};

constexpr std::string_view kFileNamePrefix = "libanalyzer-";

class AnalyzerRegister final
  : public TaggedGenericRegister<Key, Value, std::string_view,
                                 AnalyzerRegister> {};

constexpr std::string_view kTypeParam = "type";
constexpr std::string_view kPropertiesParam = "properties";
constexpr std::string_view kAnalyzerParam = "analyzer";

std::string_view GetType(vpack::Slice& input) {
  SDB_ASSERT(input.isObject());
  input = input.get(kAnalyzerParam);
  if (input.isNone() || input.isNull()) {
    return StringTokenizer::type_name();
  }
  if (!input.isObject()) {
    return {};
  }
  auto type = input.get(kTypeParam);
  if (!type.isString()) {
    return {};
  }
  return type.stringView();
}

}  // namespace

AnalyzerRegistrar::AnalyzerRegistrar(
  const TypeInfo& type, const TypeInfo& args_format,
  Analyzer::ptr (*factory)(std::string_view args),
  bool (*normalizer)(std::string_view args, std::string& config),
  const char* source /*= nullptr*/) {
  const auto source_ref =
    source ? std::string_view{source} : std::string_view{};
  const Value new_entry{factory, normalizer};
  auto entry = AnalyzerRegister::instance().set(
    Key{type.name(), args_format}, new_entry,
    IsNull(source_ref) ? nullptr : &source_ref);

  _registered = entry.second;

  if (!_registered && new_entry != entry.first) {
    const auto* registered_source =
      AnalyzerRegister::instance().tag(Key{type.name(), args_format});

    if (source && registered_source) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "type name collision detected while registering analyzer, "
               "ignoring: type '",
               type.name(), "' from ", source, ", previously from ",
               *registered_source);
    } else if (source) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "type name collision detected while registering analyzer, "
               "ignoring: type '",
               type.name(), "' from ", source);
    } else if (registered_source) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "type name collision detected while registering analyzer, "
               "ignoring: type '",
               type.name(), "', previously from ", *registered_source);
    } else {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "type name collision detected while registering analyzer, "
               "ignoring: type '",
               type.name(), "'");
    }
  }
}

namespace analyzers {

bool Exists(std::string_view name, const TypeInfo& args_format,
            bool load_library /*= true*/) {
  return AnalyzerRegister::instance()
    .get(Key{name, args_format}, load_library)
    .factory;
}

bool Normalize(std::string& out, std::string_view name,
               const TypeInfo& args_format, std::string_view args,
               bool load_library /*= true*/) noexcept {
  try {
    auto* normalizer = AnalyzerRegister::instance()
                         .get(Key{name, args_format}, load_library)
                         .normalizer;

    return normalizer ? normalizer(args, out) : false;
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught exception while normalizing analyzer '", name,
              "' arguments");
  }

  return false;
}

Analyzer::ptr Get(std::string_view name, const TypeInfo& args_format,
                  std::string_view args,
                  bool load_library /*= true*/) noexcept {
  try {
    auto* factory = AnalyzerRegister::instance()
                      .get(Key{name, args_format}, load_library)
                      .factory;

    return factory ? factory(args) : nullptr;
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught exception while getting an analyzer instance");
  }

  return nullptr;
}

void LoadAll(std::string_view path) {
  LoadLibraries(path, kFileNamePrefix, "");
}

bool Visit(
  const std::function<bool(std::string_view, const TypeInfo&)>& visitor) {
  return AnalyzerRegister::instance().visit(
    [&](const Key& key) { return visitor(key.type, key.args_format); });
}

bool MakeAnalyzer(vpack::Slice input, Analyzer::ptr& output) {
  auto type = GetType(input);
  if (type.empty()) {
    return false;
  }
  if (type == StringTokenizer::type_name()) {
    output = {};
    return true;
  }
  input = input.get(kPropertiesParam);
  if (input.isNone()) {
    input = vpack::Slice::emptyObjectSlice();
  }
  output = Get(type, irs::Type<text_format::VPack>::get(),
               {input.startAs<char>(), input.byteSize()});
  if (!output) {
    // fallback to json format if vpack isn't available
    output =
      Get(type, irs::Type<text_format::Json>::get(), slice_to_string(input));
  }
  return output != nullptr;
}

bool NormalizeAnalyzer(vpack::Slice input, vpack::Builder& output) {
  auto type = GetType(input);
  if (type.empty()) {
    return false;
  }
  if (type == StringTokenizer::type_name()) {
    return true;
  }
  vpack::ObjectBuilder scope{&output, kAnalyzerParam};
  output.add(kTypeParam, type);
  input = input.get(kPropertiesParam);
  if (input.isNone()) {
    input = vpack::Slice::emptyObjectSlice();
  }
  std::string normalized;
  if (Normalize(normalized, type, irs::Type<text_format::VPack>::get(),
                {input.startAs<char>(), input.byteSize()})) {
    output.add(
      kPropertiesParam,
      vpack::Slice{reinterpret_cast<const uint8_t*>(normalized.data())});
    return true;
  }
  // fallback to json format if vpack isn't available
  if (Normalize(normalized, type, irs::Type<text_format::Json>::get(),
                slice_to_string(input))) {
    auto vpack = vpack::Parser::fromJson(normalized);
    output.add(kPropertiesParam, vpack->slice());
    return true;
  }
  return false;
}

void Init() {
  ClassificationTokenizer::init();
  CollationTokenizer::init();
  DelimitedTokenizer::init();
  MinHashTokenizer::init();
  NearestNeighborsTokenizer::init();
  PathHierarchyTokenizer::init();
  StopwordsTokenizer::init();
  NGramTokenizerBase::init();
  PipelineTokenizer::init();
  SegmentationTokenizer::init();
  NormalizingTokenizer::init();
  StemmingTokenizer::init();
  TextTokenizer::init();
  MultiDelimitedTokenizer::init();
  GeoAnalyzer::init();
}

}  // namespace analyzers
}  // namespace irs::analysis
