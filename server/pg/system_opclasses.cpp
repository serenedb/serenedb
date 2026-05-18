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

#include "pg/system_opclasses.h"

#include <span>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/static_strings.h"

namespace sdb::pg {
namespace {

using SystemOpClassFactory = std::shared_ptr<catalog::OpClass> (*)();

struct SystemOpClassEntry {
  std::string_view schema;
  std::string_view name;
  SystemOpClassFactory factory;
};

// Built-in opclasses registered alongside the persistent catalog. Entries
// here are constructed once at startup, are not persisted, and are visible
// from every database.
//
// TODO(opclass): add `pg_catalog.text_search_dictionary` with the standard
// text defaults so users can write
//     CREATE INDEX i ON t USING inverted(body text_search_dictionary)
// without first declaring a per-database TSDictionary. Empty for now --
// follow-up commits will design the canonical body and re-use the
// CreateTextOpClass path to produce it.
constexpr std::span<const SystemOpClassEntry> kSystemOpClasses{};

struct KeyHash {
  using is_transparent = void;
  size_t operator()(
    std::pair<std::string_view, std::string_view> p) const noexcept {
    return absl::HashOf(p);
  }
};

struct KeyEq {
  using is_transparent = void;
  bool operator()(std::pair<std::string_view, std::string_view> a,
                  std::pair<std::string_view, std::string_view> b) const noexcept {
    return a == b;
  }
};

using SystemOpClassMap = containers::FlatHashMap<
  std::pair<std::string_view, std::string_view>,
  std::shared_ptr<catalog::OpClass>, KeyHash, KeyEq>;

SystemOpClassMap& Mutable() {
  static SystemOpClassMap instance;
  return instance;
}

}  // namespace

void InitSystemOpClasses() {
  auto& map = Mutable();
  map.clear();
  for (const auto& entry : kSystemOpClasses) {
    auto opclass = entry.factory();
    map.emplace(std::pair{entry.schema, entry.name}, std::move(opclass));
  }
}

std::shared_ptr<catalog::OpClass> GetSystemOpClass(std::string_view schema,
                                                   std::string_view name) {
  const auto& map = Mutable();
  auto it = map.find(std::pair{schema, name});
  return it == map.end() ? nullptr : it->second;
}

void VisitSystemOpClasses(
  std::string_view schema,
  absl::FunctionRef<void(const catalog::OpClass&)> visitor) {
  for (const auto& [key, opclass] : Mutable()) {
    if (key.first == schema) {
      visitor(*opclass);
    }
  }
}

}  // namespace sdb::pg
