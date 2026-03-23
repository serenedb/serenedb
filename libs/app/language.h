////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <unicode/locid.h>

#include <memory>
#include <string>

#include "app/app_feature.h"
#include "basics/utf8_helper.h"

namespace sdb {
namespace options {

class ProgramOptions;
}

class LanguageFeature final : public app::AppFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Language"; }

  explicit LanguageFeature(app::AppServer& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;
  static std::string prepareIcu(const std::string& binary_path,
                                const std::string& binary_execution_path,
                                std::string& path,
                                const std::string& binary_name);
  icu::Locale& getLocale();
  std::tuple<std::string_view, basics::LanguageType> getLanguage() const;
  bool forceLanguageCheck() const;
  std::string getCollatorLanguage() const;
  void resetLanguage(std::string_view language, basics::LanguageType type);

 private:
  icu::Locale _locale;
  std::string _default_language;
  std::string _icu_language;
  basics::LanguageType _lang_type = basics::LanguageType::Invalid;
  const char* _binary_path;
  std::string _icu_data;
  bool _force_language_check = true;
};

}  // namespace sdb
