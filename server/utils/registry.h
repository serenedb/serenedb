////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <absl/algorithm/container.h>

#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <string_view>
#include <utility>
#include <vector>

namespace sdb {

[[noreturn]] void RegisteredTwice(std::string_view kind, std::string_view name);

template<typename Entry>
class Registry final {
 public:
  class Register final {
   public:
    explicit Register(std::string_view name, auto&&... fields)
      : _entry{std::forward<decltype(fields)>(fields)...} {
      if (!Entries().try_emplace(name, &_entry).second) {
        RegisteredTwice(Entry::kKind, name);
      }
    }

    Register(const Register&) = delete;
    Register& operator=(const Register&) = delete;

   private:
    Entry _entry;
  };

  static const Entry* Find(std::string_view name) {
    const auto& entries = Entries();
    const auto it = entries.find(name);
    return it == entries.end() ? nullptr : it->second;
  }

  static std::vector<std::string_view> Names() {
    const auto& entries = Entries();
    std::vector<std::string_view> names;
    names.reserve(entries.size());
    for (const auto& [name, entry] : entries) {
      names.push_back(name);
    }
    absl::c_sort(names);
    return names;
  }

 private:
  static irs::containers::FlatHashMap<std::string_view, const Entry*>&
  Entries() {
    static irs::containers::FlatHashMap<std::string_view, const Entry*> entries;
    return entries;
  }
};

}  // namespace sdb
