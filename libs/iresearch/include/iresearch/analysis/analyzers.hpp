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

#pragma once

#include <vpack/slice.h>

#include <functional>

#include "analyzer.hpp"
#include "basics/shared.hpp"
#include "iresearch/utils/text_format.hpp"

namespace vpack {

class Builder;

}  // namespace vpack
namespace irs::analysis {

using factory_f = analysis::Analyzer::ptr (*)(std::string_view args);
using normalizer_f = bool (*)(std::string_view args, std::string& config);

class AnalyzerRegistrar {
 public:
  AnalyzerRegistrar(const TypeInfo& type, const TypeInfo& args_format,
                    factory_f factory, normalizer_f normalizer,
                    const char* source = nullptr);

  explicit operator bool() const noexcept { return _registered; }

 private:
  bool _registered;
};

namespace analyzers {

// Checks whether an analyzer with the specified name is registered.
bool Exists(std::string_view name, const TypeInfo& args_format,
            bool load_library = true);

// Normalize arguments for an analyzer specified by name and store them
// in 'out' argument.
// Returns true on success, false - otherwise
bool Normalize(std::string& out, std::string_view name,
               const TypeInfo& args_format, std::string_view args,
               bool load_library = true) noexcept;

// Find an analyzer by name, or nullptr if not found
// indirect call to <class>::make(...).
// NOTE: make(...) MUST be defined in CPP to ensire proper code scope
Analyzer::ptr Get(std::string_view name, const TypeInfo& args_format,
                  std::string_view args, bool load_library = true) noexcept;

// Load all analyzers from plugins directory.
void LoadAll(std::string_view path);

// Visit all loaded analyzers, terminate early if visitor returns false.
bool Visit(
  const std::function<bool(std::string_view, const TypeInfo&)>& visitor);

bool MakeAnalyzer(vpack::Slice input, Analyzer::ptr& output);
bool NormalizeAnalyzer(vpack::Slice input, vpack::Builder& output);

void Init();

}  // namespace analyzers
}  // namespace irs::analysis

#define REGISTER_ANALYZER_IMPL(analyzer_name, args_format, factory,      \
                               normalizer, line, source)                 \
  static ::irs::analysis::AnalyzerRegistrar analyzer_registrar##_##line( \
    ::irs::Type<analyzer_name>::get(), ::irs::Type<args_format>::get(),  \
    &factory, &normalizer, source)
#define REGISTER_ANALYZER_EXPANDER(analyzer_name, args_format, factory,   \
                                   normalizer, file, line)                \
  REGISTER_ANALYZER_IMPL(analyzer_name, args_format, factory, normalizer, \
                         line, file ":" IRS_TO_STRING(line))
#define REGISTER_ANALYZER(analyzer_name, args_format, factory, normalizer)    \
  REGISTER_ANALYZER_EXPANDER(analyzer_name, args_format, factory, normalizer, \
                             __FILE__, __LINE__)
#define REGISTER_ANALYZER_JSON(analyzer_name, factory, normalizer)    \
  REGISTER_ANALYZER(analyzer_name, ::irs::text_format::Json, factory, \
                    normalizer)
#define REGISTER_ANALYZER_TEXT(analyzer_name, factory, normalizer)    \
  REGISTER_ANALYZER(analyzer_name, ::irs::text_format::Text, factory, \
                    normalizer)
#define REGISTER_ANALYZER_VPACK(analyzer_name, factory, normalizer)    \
  REGISTER_ANALYZER(analyzer_name, ::irs::text_format::VPack, factory, \
                    normalizer)
