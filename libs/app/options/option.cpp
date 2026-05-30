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

#include "option.h"

#include <vpack/builder.h>

#include <iostream>

#include "app/options/parameters.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"

using namespace sdb::options;

// create an option, consisting of single string
Option::Option(const std::string& value, const std::string& description,
               std::unique_ptr<Parameter> parameter,
               std::underlying_type_t<Flags> flags)
  : section(),
    name(),
    description(description),
    shorthand(),
    parameter(std::move(parameter)),
    flags(flags) {
  auto parts = splitName(value);
  section = parts.first;
  name = parts.second;

  const size_t pos = name.find(',');
  if (pos != std::string::npos) {
    shorthand = stripShorthand(name.substr(pos + 1));
    name.resize(pos);
  }
#ifdef SDB_DEV
  // at least one OS must be supported
  if (!hasFlag(sdb::options::Flags::OsLinux) &&
      !hasFlag(sdb::options::Flags::OsMac) &&
      !hasFlag(sdb::options::Flags::Obsolete)) {
    SDB_THROW(sdb::ERROR_INTERNAL, std::string("option ") + value +
                                " needs to be supported on at least one OS");
  }
#endif
}

void Option::toVPack(vpack::Builder& builder, bool detailed) const {
  parameter->toVPack(builder, detailed);
}

bool Option::hasFlag(Flags flag) const {
  return (std::to_underlying(flag) & flags) == std::to_underlying(flag);
}

// format a version string
std::string Option::toVersionString(uint32_t version) const {
  if (version == 0) {
    return "-";
  }

  std::string result("v");
  // intentionally using integer division here...
  result += std::to_string(version / 10000) + ".";
  version -= (version / 10000) * 10000;
  result += std::to_string(version / 100) + ".";
  version -= (version / 100) * 100;
  result += std::to_string(version);
  return result;
}

// format multiple version strings
std::string Option::toVersionString(
  const std::vector<uint32_t>& versions) const {
  std::string result;
  for (const auto& it : versions) {
    if (!result.empty()) {
      result += ", ";
    }
    result += toVersionString(it);
  }
  return result;
}

// provide a detailed explanation of an option
Option& Option::setLongDescription(std::string_view long_desc) noexcept {
  long_description = long_desc;
  return *this;
}

// specifies in which version the option was introduced. version numbers
// should be specified such as 30402 (version 3.4.2)
// a version number of 0 means "unknown"
Option& Option::setIntroducedIn(uint32_t version) {
  introduced_in_versions.push_back(version);
  return *this;
}

// specifies in which version the option was deprecated. version numbers
// should be specified such as 30402 (version 3.4.2)
// a version number of 0 means "unknown"
Option& Option::setDeprecatedIn(uint32_t version) {
  deprecated_in_versions.push_back(version);
  return *this;
}

// returns whether or not a long description was set
bool Option::hasLongDescription() const noexcept {
  return !long_description.empty();
}

// returns whether or not we know in which version(s) an option was added
bool Option::hasIntroducedIn() const noexcept {
  return !introduced_in_versions.empty();
}

// returns whether or not we know in which version(s) an option was added
bool Option::hasDeprecatedIn() const noexcept {
  return !deprecated_in_versions.empty();
}

// returns the version in which the option was introduced as a proper
// version string - if the version is unknown this will return "-"
std::string Option::introducedInString() const {
  return toVersionString(introduced_in_versions);
}

// returns the version in which the option was deprecated as a proper
// version string - if the version is unknown this will return "-"
std::string Option::deprecatedInString() const {
  return toVersionString(deprecated_in_versions);
}

// get display name for the option
std::string Option::displayName() const { return "--" + fullName(); }

// get full name for the option
std::string Option::fullName() const {
  if (section.empty()) {
    return name;
  }
  return section + '.' + name;
}

// print help for an option
// the special search string "." will show help for all sections, even if hidden
void Option::printHelp(const std::string& search, size_t tw, size_t ow,
                       bool) const {
  if (search == "." || !hasFlag(sdb::options::Flags::Uncommon)) {
    std::cout << "  " << pad(nameWithType(), ow) << "   ";

    std::string value = description;
    if (hasFlag(sdb::options::Flags::Obsolete)) {
      value += " (obsolete option)";
    } else {
      if (hasFlag(sdb::options::Flags::Experimental)) {
        value += " (experimental)";
      }
      std::string description = parameter->description();
      if (!description.empty()) {
        value.append("\n");
        value.append(description);
      }

      if (!hasFlag(sdb::options::Flags::Command)) {
        if (hasFlag(sdb::options::Flags::Dynamic)) {
          value += " (dynamic default: " + parameter->valueString() + ")";
        } else {
          value += " (default: " + parameter->valueString() + ")";
        }
      }
      if (hasIntroducedIn()) {
        value += " (introduced in " + introducedInString() + ")";
      }
      if (hasDeprecatedIn()) {
        value += " (deprecated in " + deprecatedInString() + ")";
      }
    }
    auto parts = wordwrap(value, tw - ow - 6);
    const size_t n = parts.size();
    for (size_t i = 0; i < n; ++i) {
      std::cout << trim(parts[i]) << std::endl;
      if (i < n - 1) {
        std::cout << "  " << pad("", ow) << "   ";
      }
    }
  }
}

std::string Option::nameWithType() const {
  return displayName() + " " + parameter->typeDescription();
}

// determine the width of an option help string
size_t Option::optionsWidth() const {
  if (hasFlag(sdb::options::Flags::Uncommon)) {
    return 0;
  }

  return nameWithType().size();
}

// strip the "--" from a string
std::string Option::stripPrefix(const std::string& name) {
  if (name.starts_with("--")) {
    // strip initial "--"
    return name.substr(2);
  }
  return name;
}

// strip the "-" from a string
std::string Option::stripShorthand(const std::string& name) {
  if (name.starts_with('-')) {
    // strip initial "-"
    return name.substr(1);
  }
  return name;
}

// split an option name at the ".", if it exists
std::pair<std::string, std::string> Option::splitName(std::string name) {
  std::string section;
  name = stripPrefix(name);
  // split at "."
  size_t pos = name.find('.');
  if (pos == std::string::npos) {
    // general option
    section = "";
  } else {
    // section-specific option
    section = name.substr(0, pos);
    name = name.substr(pos + 1);
  }

  return std::make_pair(std::move(section), std::move(name));
}

std::vector<std::string> Option::wordwrap(const std::string& value,
                                          size_t size) {
  std::vector<std::string> result;
  std::string_view next = value;

  if (size == 0) {
    size = value.size();
  }

  while (!next.empty()) {
    size_t skip = 0;
    size_t m = std::min(size, next.size());
    SDB_ASSERT(m > 0);
    size_t n = next.find_last_of("., ", m - 1);
    if (n != std::string_view::npos && next.size() > size && n >= size / 2 &&
        n + 1 <= size) {
      m = n + 1;
    }
    n = next.find('\n');
    if (n != std::string_view::npos && n < m) {
      m = n;
      skip = 1;
    }

    SDB_ASSERT(m <= size);
    SDB_ASSERT(next.substr(0, m).find('\n') == std::string_view::npos);
    result.emplace_back(next.data(), m);

    if (m + skip >= next.size()) {
      break;
    }
    next = next.substr(m + skip);
  }

  return result;
}

// right-pad a string
std::string Option::pad(const std::string& value, size_t length) {
  const size_t value_length = value.size();
  if (value_length > length) {
    return value.substr(0, length);
  }
  if (value_length == length) {
    return value;
  }
  return value + std::string(length - value_length, ' ');
}

std::string Option::trim(const std::string& value) {
  const size_t pos = value.find_first_not_of(" \t\n\r");
  if (pos == std::string::npos) {
    return "";
  }
  return value.substr(pos);
}
