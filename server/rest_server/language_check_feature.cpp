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

#include "language_check_feature.h"

#include <vpack/vpack_helper.h>

#include "app/app_server.h"
#include "app/language.h"
#include "basics/application-exit.h"
#include "basics/directories.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/result.h"
#include "basics/utf8_helper.h"
#include "rest_server/database_path_feature.h"

namespace sdb {
namespace {

constexpr std::string_view kDefaultLangKey = "default";
constexpr std::string_view kIcuLangKey = "icu-language";

/// reads previous default langauge from file
Result ReadLanguage(SerenedServer& server, std::string& language,
                    basics::LanguageType& type) {
  auto& database_path = server.getFeature<DatabasePathFeature>();
  std::string filename = database_path.subdirectoryName("LANGUAGE");

  if (!SdbExistsFile(filename.c_str())) {
    return {ERROR_FILE_NOT_FOUND};
  }

  try {
    vpack::Builder builder =
      basics::VPackHelper::vpackFromFile(filename.c_str());
    vpack::Slice content = builder.slice();
    if (!content.isObject()) {
      return {ERROR_INTERNAL};
    }
    vpack::Slice default_slice = content.get(kDefaultLangKey);
    vpack::Slice icu_slice = content.get(kIcuLangKey);

    // both languages are specified in files
    if (default_slice.isString() && icu_slice.isString()) {
      SDB_ERROR("xxxxx", Logger::CONFIG,
                "Only one language should be specified");
      return {ERROR_INTERNAL};
    }

    if (default_slice.isString()) {
      language = default_slice.stringView();
      type = basics::LanguageType::Default;
    } else if (icu_slice.isString()) {
      language = icu_slice.stringView();
      type = basics::LanguageType::ICU;
    } else {
      return {ERROR_INTERNAL};
    }

  } catch (...) {
    // Nothing to free
    return {ERROR_INTERNAL};
  }

  return {};
}

/// writes the default language to file
ErrorCode WriteLanguage(SerenedServer& server, std::string_view lang,
                        basics::LanguageType curr_lang_type) {
  auto& database_path = server.getFeature<DatabasePathFeature>();
  std::string filename = database_path.subdirectoryName("LANGUAGE");

  // generate json
  vpack::Builder builder;
  try {
    builder.openObject();
    switch (curr_lang_type) {
      case basics::LanguageType::Default:
        builder.add(kDefaultLangKey, lang);
        break;

      case basics::LanguageType::ICU:
        builder.add(kIcuLangKey, lang);
        break;

      case basics::LanguageType::Invalid:
      default:
        SDB_ASSERT(false);
        break;
    }

    builder.close();
  } catch (...) {
    // out of memory
    if (basics::LanguageType::Default == curr_lang_type) {
      SDB_ERROR("xxxxx", Logger::CONFIG,
                "cannot save default language in file '", filename,
                "': out of memory");
    } else {
      SDB_ERROR("xxxxx", Logger::CONFIG, "cannot save icu language in file '",
                filename, "': out of memory");
    }

    return {ERROR_OUT_OF_MEMORY};
  }

  // save json info to file
  SDB_DEBUG("xxxxx", Logger::CONFIG, "Writing language to file '", filename,
            "'");
  bool ok = basics::VPackHelper::vpackToFile(filename, builder.slice(), true);
  if (!ok) {
    SDB_ERROR("xxxxx", Logger::CONFIG, "could not save language in file '",
              filename, "': ", LastError());
    return {ERROR_INTERNAL};
  }

  return {};
}

std::tuple<std::string, basics::LanguageType> GetOrSetPreviousLanguage(
  SerenedServer& server, std::string_view collator_lang,
  basics::LanguageType curr_lang_type) {
  std::string prev_language;
  basics::LanguageType prev_type = basics::LanguageType::Invalid;
  Result res = ReadLanguage(server, prev_language, prev_type);

  if (res.ok()) {
    SDB_ASSERT(prev_type != basics::LanguageType::Invalid);
    return {prev_language, prev_type};
  }

  // okay, we didn't find it, let's write out the input instead
  // TODO(mbkkt) why ignore?
  std::ignore = WriteLanguage(server, collator_lang, curr_lang_type);

  return {std::string{collator_lang}, curr_lang_type};
}

}  // namespace

LanguageCheckFeature::LanguageCheckFeature(Server& server)
  : SerenedFeature{server, name()} {
  setOptional(false);
}

void LanguageCheckFeature::start() {
  using namespace basics;

  auto& feature = server().getFeature<LanguageFeature>();
  auto [currLang, currLangType] = feature.getLanguage();
  auto collator_lang = feature.getCollatorLanguage();

  auto [prevLang, prevLangType] =
    GetOrSetPreviousLanguage(server(), collator_lang, currLangType);

  if (basics::LanguageType::Invalid == currLangType) {
    SDB_FATAL("xxxxx", Logger::CONFIG, "Specified language '", collator_lang,
              " has invalid type");
  }

  if (currLang.empty() && basics::LanguageType::Default == currLangType &&
      !prevLang.empty()) {
    // we found something in LANGUAGE file
    // override the empty current setting for default with the previous one
    feature.resetLanguage(prevLang, prevLangType);
    return;
  }

  if (collator_lang != prevLang || prevLangType != currLangType) {
    if (feature.forceLanguageCheck()) {
      // current not empty and not the same as previous, get out!
      SDB_FATAL("xxxxx", Logger::CONFIG, "Specified language '", collator_lang,
                "' with type '",
                (currLangType == basics::LanguageType::Default ? kDefaultLangKey
                                                               : kIcuLangKey),
                "' does not match previously used language '", prevLang,
                "' with type '",
                (prevLangType == basics::LanguageType::Default ? kDefaultLangKey
                                                               : kIcuLangKey),
                "'");
    } else {
      SDB_WARN("xxxxx", Logger::CONFIG, "Specified language '", collator_lang,
               "' does not match previously used language '", prevLang,
               "'. starting anyway due to --default-language-check=false "
               "setting");
    }
  }
}

}  // namespace sdb
