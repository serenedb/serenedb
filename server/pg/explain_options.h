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

#pragma once

#include "pg/options_parser.h"

namespace sdb::pg {

// For EXPLAIN (options) stmt
class ExplainStmtOptionsParser : public OptionsParser {
 public:
  ExplainStmtOptionsParser(const List* options, std::string_view query_string)
    : OptionsParser{options,
                    explain_options::kExplainGroup,
                    {.operation = "EXPLAIN", .query_string = query_string}} {
    ParseOptions([&] { ParseImpl(); });
  }

  auto GetExplainOptions() && { return _result; }

 private:
  template<const OptionInfo& Info>
  void ParseFlag() {
    if (EraseOptionOrDefault<Info>()) {
      explain_options::AddByName(Info.name, _result);
    }
  }

  template<const OptionGroup& Group>
  void ParseGroupFlags() {
    [&]<size_t... I>(std::index_sequence<I...>) {
      (ParseFlag<Group.options[I]>(), ...);
    }(std::make_index_sequence<Group.options.size()>{});
    [&]<size_t... I>(std::index_sequence<I...>) {
      (ParseGroupFlags<Group.subgroups[I]>(), ...);
    }(std::make_index_sequence<Group.subgroups.size()>{});
  }

  void ParseImpl() { ParseGroupFlags<explain_options::kExplainGroup>(); }

  explain_options::ExplainOptions _result;
};

}  // namespace sdb::pg
