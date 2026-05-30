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

#include "program_options.h"

#include <vpack/builder.h>

#include <algorithm>
#include <iostream>

#include "app/options/option.h"
#include "app/options/parameters.h"
#include "app/options/section.h"
#include "app/options/translator.h"
#include "app/shell_colors.h"
#include "basics/exitcodes.h"
#include "basics/files.h"
#include "basics/levenshtein.h"
#include "basics/terminal-utils.h"
#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#define SERENEDB_PROGRAM_OPTIONS_PROGNAME "#progname#"

using namespace sdb::options;

ProgramOptions::ProcessingResult::ProcessingResult()
  : positionals(), _touched(), _frozen(), exit_code(0) {}

ProgramOptions::ProcessingResult::~ProcessingResult() = default;

// mark an option as being touched during options processing
void ProgramOptions::ProcessingResult::touch(const std::string& name) {
  _touched.emplace(name);
}

// whether or not an option was touched during options processing,
// including the current pass
bool ProgramOptions::ProcessingResult::touched(const std::string& name) const {
  return _touched.contains(Option::stripPrefix(name));
}

// mark an option as being frozen
void ProgramOptions::ProcessingResult::freeze(const std::string& name) {
  _frozen.emplace(name);
}

// whether or not an option was touched during options processing,
// not including the current pass
bool ProgramOptions::ProcessingResult::frozen(const std::string& name) const {
  return _frozen.contains(Option::stripPrefix(name));
}

// mark options processing as failed
void ProgramOptions::ProcessingResult::fail(int exit_code) noexcept {
  this->exit_code = exit_code;
}

// return the registered exit code
int ProgramOptions::ProcessingResult::exitCode() const noexcept {
  return exit_code;
}

int ProgramOptions::ProcessingResult::exitCodeOrFailure() const noexcept {
  if (exit_code != 0) {
    return exit_code;
  }
  return EXIT_FAILURE;
}

ProgramOptions::ProgramOptions(const char* progname, const std::string& usage,
                               const std::string& more, const char* binary_path)
  : _progname(progname),
    _usage(usage),
    _more(more),
    _similarity(Levenshtein),
    _processing_result(),
    _sealed(false),
    _override_options(false),
    _binary_path(binary_path) {
  // find progname wildcard in string
  const size_t pos = _usage.find(SERENEDB_PROGRAM_OPTIONS_PROGNAME);

  if (pos != std::string::npos) {
    // and replace it with actual program name
    _usage = usage.substr(0, pos) + _progname +
             _usage.substr(pos + strlen(SERENEDB_PROGRAM_OPTIONS_PROGNAME));
  }

  _translator = EnvironmentTranslator;
}

std::string ProgramOptions::progname() const { return _progname; }

// sets a value translator
void ProgramOptions::setTranslator(
  const std::function<std::string(const std::string&, const char*)>&
    translator) {
  _translator = translator;
}

// return a const reference to the processing result
const ProgramOptions::ProcessingResult& ProgramOptions::processingResult()
  const {
  return _processing_result;
}

// return a reference to the processing result
ProgramOptions::ProcessingResult& ProgramOptions::processingResult() {
  return _processing_result;
}

// seal the options
// trying to add an option or a section after sealing will throw an error
void ProgramOptions::seal() { _sealed = true; }

// allow or disallow overriding already set options
void ProgramOptions::allowOverride(bool value) {
  checkIfSealed();
  _override_options = value;
}

bool ProgramOptions::allowOverride() const { return _override_options; }

// set context for error reporting
void ProgramOptions::setContext(const std::string& value) { _context = value; }

// adds a sub-headline for one option or a group of options
void ProgramOptions::addHeadline(const std::string& prefix,
                                 const std::string& description) {
  checkIfSealed();

  auto parts = Option::splitName(prefix);
  if (parts.first.empty()) {
    std::swap(parts.first, parts.second);
  }
  auto it = _sections.find(parts.first);

  if (it == _sections.end()) {
    throw std::logic_error(std::string("section '") + parts.first +
                           "' not found");
  }

  (*it).second.headlines[parts.second] = description;
}

// prints usage information
void ProgramOptions::printUsage() const {
  std::cout << _usage << std::endl << std::endl;
}

// prints a help for all options, or the options of a section
// the special search string "." will show help for all sections, even if
// hidden
void ProgramOptions::printHelp(const std::string& search) const {
  const bool colors = (isatty(STDOUT_FILENO) != 0);
  auto ts = terminal_utils::DefaultTerminalSize();
  size_t tw = ts.columns;
  size_t ow = optionsWidth();

  std::string normalized = search;
  if (normalized == "uncommon" || normalized == "hidden") {
    normalized = ".";
  }

  bool show_hidden = normalized == ".";

  printUsage();

  if (!show_hidden) {
    std::cout << "Common options (excluding hidden/uncommon options):"
              << std::endl;
    std::cout << std::endl;
  }

  for (const auto& it : _sections) {
    if (normalized == it.second.name ||
        ((normalized == "*" || show_hidden) && !it.second.obsolete)) {
      it.second.printHelp(normalized, tw, ow, colors);
    }
  }

  if (normalized == "*") {
    printSectionsHelp();
  }

  std::cout << std::endl;
  if (!show_hidden) {
    std::string_view color_start = "";
    std::string_view color_end = "";

    if (isatty(STDOUT_FILENO)) {
      color_start = colors::kBright;
      color_end = colors::kReset;
    }
    std::cout << "More uncommon options are not shown by default. "
              << "To show these options, use  " << color_start
              << "--help-uncommon" << color_end << std::endl;
  }
  std::cout << std::endl;
}

// prints the names for all section help options
void ProgramOptions::printSectionsHelp() const {
  std::string_view color_start = "";
  std::string_view color_end = "";

  if (isatty(STDOUT_FILENO)) {
    color_start = colors::kBright;
    color_end = colors::kReset;
  }

  // print names of sections
  std::cout << _more;
  for (const auto& it : _sections) {
    if (!it.second.name.empty() && it.second.hasOptions() &&
        !it.second.obsolete) {
      std::cout << "  " << color_start << "--help-" << it.second.name
                << color_end;
    }
  }
  std::cout << std::endl;
}

// translate a shorthand option
std::string ProgramOptions::translateShorthand(const std::string& name) const {
  auto it = _shorthands.find(name);

  if (it == _shorthands.end()) {
    return name;
  }
  return (*it).second;
}

void ProgramOptions::walk(
  const std::function<void(const Section&, const Option&)>& callback,
  bool only_touched, bool include_obsolete) const {
  for (const auto& it : _sections) {
    if (!include_obsolete && it.second.obsolete) {
      // obsolete section. ignore it
      continue;
    }
    for (const auto& it2 : it.second.options) {
      if (!include_obsolete &&
          it2.second.hasFlag(sdb::options::Flags::Obsolete)) {
        // obsolete option. ignore it
        continue;
      }
      if (only_touched && !_processing_result.touched(it2.second.fullName())) {
        // option not touched. skip over it
        continue;
      }
      callback(it.second, it2.second);
    }
  }
}

// checks whether a specific option exists
// if the option does not exist, this will flag an error
bool ProgramOptions::require(const std::string& name) {
  const std::string& modernized = modernize(name);

  auto parts = Option::splitName(modernized);
  auto it = _sections.find(parts.first);

  if (it == _sections.end()) {
    unknownOption(modernized);
    return false;
  }

  auto it2 = (*it).second.options.find(parts.second);

  if (it2 == (*it).second.options.end()) {
    unknownOption(modernized);
    return false;
  }

  return true;
}

// sets a value for an option
bool ProgramOptions::setValue(const std::string& name,
                              const std::string& value) {
  const std::string& modernized = modernize(name);

  if (!_override_options && _processing_result.frozen(modernized)) {
    // option already frozen. don't override it
    return true;
  }

  auto parts = Option::splitName(modernized);
  auto it = _sections.find(parts.first);

  if (it == _sections.end()) {
    unknownOption(modernized);
    return false;
  }

  if ((*it).second.obsolete) {
    // section is obsolete. ignore it
    return true;
  }

  auto it2 = (*it).second.options.find(parts.second);

  if (it2 == (*it).second.options.end()) {
    unknownOption(modernized);
    return false;
  }

  auto& option = (*it2).second;
  if (option.hasFlag(sdb::options::Flags::Obsolete)) {
    // option is obsolete. ignore it
    _processing_result.touch(modernized);
    return true;
  }

  if (option.hasFlag(options::Flags::FlushOnFirst) &&
      _already_flushed.find(parts.second) == _already_flushed.end()) {
    _already_flushed.insert(parts.second);
    option.parameter->flushValue();
  }
  std::string result = option.parameter->set(_translator(value, _binary_path));

  if (!result.empty()) {
    // parameter validation failed
    std::string_view color_start1 = "";
    std::string_view color_start2 = "";
    std::string_view color_end = "";

    if (isatty(STDERR_FILENO)) {
      color_start1 = colors::kRed;
      color_start2 = colors::kBoldRed;
      color_end = colors::kReset;
    }
    fail(EXIT_INVALID_OPTION_VALUE,
         absl::StrCat("error setting value for option '", color_start2, "--",
                      modernized, color_end, "': ", color_start1, result,
                      color_end));
    return false;
  }

  _processing_result.touch(modernized);

  return true;
}

// finalizes a pass, copying touched into frozen
void ProgramOptions::endPass() {
  if (_override_options) {
    return;
  }
  for (const auto& it : _processing_result._touched) {
    _processing_result.freeze(it);
  }
}

sdb::containers::FlatHashMap<std::string, std::string>
ProgramOptions::modernizedOptions() const {
  containers::FlatHashMap<std::string, std::string> result;
  for (const auto& name : _already_modernized) {
    auto it = _old_options.find(name);
    if (it != _old_options.end()) {
      result.emplace(name, (*it).second);
    }
  }
  return result;
}

// sets a single old option and its replacement name
void ProgramOptions::addOldOption(const std::string& old,
                                  const std::string& replacement) {
  _old_options[Option::stripPrefix(old)] = Option::stripPrefix(replacement);
}

// adds a section to the options
std::map<std::string, Section>::iterator ProgramOptions::addSection(
  Section&& section) {
  checkIfSealed();

  auto name = section.name;
  auto [it, emplaced] =
    _sections.try_emplace(std::move(name), std::move(section));
  if (!emplaced) {
    // section already present. check if we need to update it
    Section& sec = it->second;
    if (!section.description.empty() && sec.description.empty()) {
      // copy over description
      sec.description = section.description;
    }
  }
  return it;
}

// adds an option to the program options.
Option& ProgramOptions::addOption(const std::string& name,
                                  const std::string& description,
                                  std::unique_ptr<Parameter> parameter,
                                  std::underlying_type_t<Flags> flags) {
  addOption(Option(name, description, std::move(parameter), flags));
  return getOption(name);
}

// adds an option to the program options. old API!
Option& ProgramOptions::addOption(const std::string& name,
                                  const std::string& description,
                                  Parameter* parameter,
                                  std::underlying_type_t<Flags> flags) {
  addOption(name, description, std::unique_ptr<Parameter>(parameter), flags);
  return getOption(name);
}

// adds a deprecated option that has no effect to the program options to not
// throw an unrecognized startup option error after upgrades until fully
// removed. not listed by --help (uncommon option)
Option& ProgramOptions::addObsoleteOption(const std::string& name,
                                          const std::string& description,
                                          bool requires_value) {
  addOption(Option(name, description,
                   std::make_unique<ObsoleteParameter>(requires_value),
                   MakeFlags(Flags::Uncommon, Flags::Obsolete)));
  return getOption(name);
}

// check whether or not an option requires a value
bool ProgramOptions::requiresValue(const std::string& name) {
  const std::string& modernized = modernize(name);

  auto parts = Option::splitName(modernized);
  auto it = _sections.find(parts.first);

  if (it == _sections.end()) {
    return false;
  }

  auto it2 = (*it).second.options.find(parts.second);

  if (it2 == (*it).second.options.end()) {
    return false;
  }

  return (*it2).second.parameter->requiresValue();
}

// returns the option by name. will throw if the option cannot be found
Option& ProgramOptions::getOption(const std::string& name) {
  const std::string& modernized = modernize(name);

  std::string stripped = modernized;
  const size_t pos = stripped.find(',');
  if (pos != std::string::npos) {
    // remove shorthand
    stripped.resize(pos);
  }
  auto parts = Option::splitName(stripped);
  auto it = _sections.find(parts.first);

  if (it == _sections.end()) {
    throw std::logic_error(std::string("option '") + stripped + "' not found");
  }

  auto it2 = (*it).second.options.find(parts.second);

  if (it2 == (*it).second.options.end()) {
    throw std::logic_error(std::string("option '") + stripped + "' not found");
  }

  return (*it2).second;
}

// returns a pointer to an option value, specified by option name
// returns a nullptr if the option is unknown
Parameter* ProgramOptions::getParameter(const std::string& name) const {
  auto parts = Option::splitName(name);

  auto it = _sections.find(parts.first);
  if (it == _sections.end()) {
    return nullptr;
  }

  auto it2 = (*it).second.options.find(parts.second);
  if (it2 == (*it).second.options.end()) {
    return nullptr;
  }

  const Option& option = (*it2).second;
  return option.parameter.get();
}

// returns an option description
std::string ProgramOptions::getDescription(const std::string& name) {
  const std::string& modernized = modernize(name);

  auto parts = Option::splitName(modernized);
  auto it = _sections.find(parts.first);

  if (it == _sections.end()) {
    return "";
  }

  auto it2 = (*it).second.options.find(parts.second);

  if (it2 == (*it).second.options.end()) {
    return "";
  }

  return (*it2).second.description;
}

// handle an unknown option
void ProgramOptions::unknownOption(const std::string& name) {
  std::string_view color_start1 = "";
  std::string_view color_start2 = "";
  std::string_view color_start3 = "";
  std::string_view color_end = "";

  if (isatty(STDERR_FILENO)) {
    color_start1 = colors::kRed;
    color_start2 = colors::kBoldRed;
    color_start3 = colors::kBright;
    color_end = colors::kReset;
  }

  fail(EXIT_INVALID_OPTION_NAME,
       absl::StrCat(color_start1, "unknown option '", color_start2, "--",
                    name + color_start1, "'", color_end));

  auto similar_options = similar(name, 8, 4);
  if (!similar_options.empty()) {
    if (similar_options.size() == 1) {
      std::cerr << "Did you mean this?" << std::endl;
    } else {
      std::cerr << "Did you mean one of these?" << std::endl;
    }
    // determine maximum width
    size_t max_width = 0;
    for (const auto& it : similar_options) {
      max_width = std::max(max_width, it.size());
    }

    for (const auto& it : similar_options) {
      std::cerr << "  " << color_start3 << Option::pad(it, max_width)
                << color_end << "    " << getDescription(it) << std::endl;
    }
    std::cerr << std::endl;
  }

  std::cerr << "Use " << color_start3 << "--help" << color_end << " or "
            << color_start3 << "--help-all" << color_end
            << " to get an overview of available options" << std::endl
            << std::endl;
}

// report an error (callback from parser)
void ProgramOptions::fail(int exit_code, const std::string& message) {
  std::string_view color_start = "";
  std::string_view color_end = "";

  if (isatty(STDERR_FILENO)) {
    color_start = colors::kRed;
    color_end = colors::kReset;
  }
  std::cerr << color_start << "Error while processing " << _context << " for "
            << SdbBasename(_progname) << ":" << color_end << std::endl;
  failNotice(exit_code, message);
  std::cerr << std::endl;
}

void ProgramOptions::failNotice(int exit_code, const std::string& message) {
  _processing_result.fail(exit_code);

  std::cerr << "  " << message << std::endl;
}

// add a positional argument (callback from parser)
void ProgramOptions::addPositional(const std::string& value) {
  _processing_result.positionals.emplace_back(value);
}

// adds an option to the list of options
void ProgramOptions::addOption(Option&& option) {
  checkIfSealed();
  std::map<std::string, Section>::iterator section_it =
    addSection(option.section, "");

  if (!option.shorthand.empty()) {
    if (!_shorthands.try_emplace(option.shorthand, option.fullName()).second) {
      throw std::logic_error(
        std::string("shorthand option already defined for option ") +
        option.displayName());
    }
  }

  Section& section = (*section_it).second;
  section.options.try_emplace(option.name, std::move(option));
}

// modernize an option name
const std::string& ProgramOptions::modernize(const std::string& name) {
  auto it = _old_options.find(Option::stripPrefix(name));
  if (it == _old_options.end()) {
    return name;
  }

  // note which old options have been used
  _already_modernized.emplace(name);

  return (*it).second;
}

// determine maximum width of all options labels
size_t ProgramOptions::optionsWidth() const {
  size_t width = 0;
  for (const auto& it : _sections) {
    width = std::max(width, it.second.optionsWidth());
  }
  return width;
}

// check if the options are already sealed and throw if yes
void ProgramOptions::checkIfSealed() const {
  if (_sealed) {
    throw std::logic_error("program options are already sealed");
  }
}

// get a list of similar options
std::vector<std::string> ProgramOptions::similar(const std::string& value,
                                                 int cut_off,
                                                 size_t max_results) {
  std::vector<std::string> result;

  if (_similarity != nullptr) {
    // build a sorted map of similar values first
    std::multimap<int, std::string> distances;
    // walk over all options
    walk(
      [this, &value, &distances](const Section&, const Option& option) {
        if (option.fullName() != value) {
          distances.emplace(_similarity(value, option.fullName()),
                            option.displayName());
        }
      },
      false);

    // now return the ones that have an edit distance not higher than the
    // cutOff value
    int last = 0;
    for (const auto& it : distances) {
      if (last > 1 && it.first > 2 * last) {
        break;
      }
      if (it.first > cut_off && it.second.substr(0, value.size()) != value) {
        continue;
      }
      result.emplace_back(it.second);
      if (result.size() >= max_results) {
        break;
      }
      last = it.first;
    }
  }

  if (value.size() >= 3) {
    // additionally add all options that have the search string as part
    // of their name
    walk(
      [&value, &result](const Section&, const Option& option) {
        if (option.fullName().find(value) != std::string::npos) {
          result.emplace_back(option.displayName());
        }
      },
      false);
  }

  // produce a unique result
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());

  return result;
}
