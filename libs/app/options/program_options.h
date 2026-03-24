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

#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "app/options/section.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"

namespace sdb::options {

struct Option;
struct Parameter;

// program options data structure
// typically an application will have a single instance of this
class ProgramOptions {
 public:
  // filter function to hide certain options in the outputs of
  // - JavaScript options api: require("internal").options()
  // - HTTP REST API: GET /_admin/options
  // filter function returns false for any option to be filtered out,
  // and true for all options to include in the output.
  static const std::function<bool(const std::string&)> kDefaultOptionsFilter;

  // struct containing the option processing result
  class ProcessingResult {
   public:
    ProcessingResult();
    ~ProcessingResult();

    // mark an option as being touched during options processing
    void touch(const std::string& name);

    // whether or not an option was touched during options processing,
    // including the current pass
    bool touched(const std::string& name) const;

    // mark an option as being frozen
    void freeze(const std::string& name);

    // whether or not an option was touched during options processing,
    // not including the current pass
    bool frozen(const std::string& name) const;

    // mark options processing as failed
    void fail(int exit_code) noexcept;

    // return the registered exit code
    int exitCode() const noexcept;

    int exitCodeOrFailure() const noexcept;

    // values of all positional arguments found
    std::vector<std::string> positionals;

    // which options were touched during option processing
    // this includes options that are touched in the current pass
    containers::FlatHashSet<std::string> _touched;  // NOLINT

    // which options were touched during option processing
    // this does not include options that are touched in the current pass
    containers::FlatHashSet<std::string> _frozen;  // NOLINT

    // registered exit code (== 0 means ok, != 0 means error)
    int exit_code;
  };

  // function type for determining the similarity between two strings
  typedef std::function<int(const std::string&, const std::string&)>
    SimilarityFuncType;

  // no need to copy this
  ProgramOptions(const ProgramOptions&) = delete;
  ProgramOptions& operator=(const ProgramOptions&) = delete;

  ProgramOptions(const char* progname, const std::string& usage,
                 const std::string& more, const char* binary_path);

  std::string progname() const;

  // sets a value translator
  void setTranslator(const std::function<std::string(const std::string&,
                                                     const char*)>& translator);

  // return a const reference to the processing result
  const ProcessingResult& processingResult() const;

  // return a reference to the processing result
  ProcessingResult& processingResult();

  // seal the options
  // trying to add an option or a section after sealing will throw an error
  void seal();

  // allow or disallow overriding already set options
  void allowOverride(bool value);

  bool allowOverride() const;

  // set context for error reporting
  void setContext(const std::string& value);

  // sets a single old option and its replacement name.
  // to be used when an option is renamed to still support the original name
  void addOldOption(const std::string& old, const std::string& replacement);

  // adds a section to the options
  std::map<std::string, Section>::iterator addSection(Section&& section);

  // adds a (regular) section to the program options
  auto addSection(const std::string& name, const std::string& description,
                  const std::string& link = "", bool hidden = false,
                  bool obsolete = false) {
    return addSection(Section(name, description, link, "", hidden, obsolete));
  }

  // adds an option to the program options.
  Option& addOption(
    const std::string& name, const std::string& description,
    std::unique_ptr<Parameter> parameter,
    std::underlying_type_t<Flags> flags = MakeFlags(Flags::Default));

  // adds an option to the program options. old API!
  Option& addOption(
    const std::string& name, const std::string& description,
    Parameter* parameter,
    std::underlying_type_t<Flags> flags = MakeFlags(Flags::Default));

  // adds a deprecated option that has no effect to the program options to not
  // throw an unrecognized startup option error after upgrades until fully
  // removed. not listed by --help (uncommon option)
  Option& addObsoleteOption(const std::string& name,
                            const std::string& description,
                            bool requires_value);

  // adds a sub-headline for one option or a group of options
  void addHeadline(const std::string& prefix, const std::string& description);

  // prints usage information
  void printUsage() const;

  // prints a help for all options, or the options of a section
  // the special search string "*" will show help for all sections
  // the special search string "." will show help for all sections, even if
  // hidden
  void printHelp(const std::string& search) const;

  // prints the names for all section help options
  void printSectionsHelp() const;

  // returns a VPack representation of the option values, with optional
  // filters applied to filter out specific options.
  // the filter function is expected to return true
  // for any options that should become part of the result
  vpack::Builder toVPack(
    bool only_touched, bool detailed,
    const std::function<bool(const std::string&)>& filter) const;

  // translate a shorthand option
  std::string translateShorthand(const std::string& name) const;

  void walk(const std::function<void(const Section&, const Option&)>& callback,
            bool only_touched, bool include_obsolete = false) const;

  // checks whether a specific option exists
  // if the option does not exist, this will flag an error
  bool require(const std::string& name);

  // sets a value for an option
  bool setValue(const std::string& name, const std::string& value);

  // finalizes a pass, copying touched into frozen
  void endPass();

  // check whether or not an option requires a value
  bool requiresValue(const std::string& name);

  // returns the option by name. will throw if the option cannot be found
  Option& getOption(const std::string& name);

  // returns a pointer to an option value, specified by option name
  // returns a nullptr if the option is unknown
  template<typename T>
  T* get(const std::string& name) {
    return dynamic_cast<T*>(getParameter(name));
  }

  // returns an option description
  std::string getDescription(const std::string& name);

  // handle an unknown option
  void unknownOption(const std::string& name);

  // report an error (callback from parser)
  void fail(int exit_code, const std::string& message);

  void failNotice(int exit_code, const std::string& message);

  // add a positional argument (callback from parser)
  void addPositional(const std::string& value);

  // return all auto-modernized options
  containers::FlatHashMap<std::string, std::string> modernizedOptions() const;

 private:
  // returns a pointer to an option value, specified by option name
  // returns a nullptr if the option is unknown
  Parameter* getParameter(const std::string& name) const;

  // adds an option to the list of options
  void addOption(Option&& option);

  // modernize an option name
  const std::string& modernize(const std::string& name);

  // determine maximum width of all options labels
  size_t optionsWidth() const;

  // check if the options are already sealed and throw if yes
  void checkIfSealed() const;

  // get a list of similar options
  std::vector<std::string> similar(const std::string& value, int cut_off,
                                   size_t max_results);

  // name of binary (i.e. argv[0])
  std::string _progname;
  // usage hint, e.g. "usage: #progname# [<options>] ..."
  std::string _usage;
  // help text for section help, e.g. "for more information use"
  std::string _more;
  // context string that's shown when errors are printed
  std::string _context;
  // already seen to flush program options
  containers::FlatHashSet<std::string> _already_flushed;
  // already warned-about old, but modernized options
  containers::FlatHashSet<std::string> _already_modernized;
  // all sections
  std::map<std::string, Section> _sections;
  // shorthands for options, translating from short options to long option names
  // e.g. "-c" to "--configuration"
  containers::FlatHashMap<std::string, std::string> _shorthands;
  // map with old options and their new equivalents, used for printing more
  // meaningful error messages when an invalid (but once valid) option was used
  containers::FlatHashMap<std::string, std::string> _old_options;
  // callback function for determining the similarity between two option names
  SimilarityFuncType _similarity;
  // option processing result
  ProcessingResult _processing_result;
  // whether or not the program options setup is still mutable
  bool _sealed;
  // allow or disallow overriding already set options
  bool _override_options;
  // translate input values
  std::function<std::string(const std::string&, const char*)> _translator;
  // directory of this binary
  const char* _binary_path;
};

}  // namespace sdb::options
