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

#include <duckdb/common/constants.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "pg/catalog/builtin/builtin.h"

namespace sdb::pg {

struct BuiltinFunction {
  duckdb::idx_t oid;
  std::string name;
  duckdb::idx_t nsp;
  char kind;
  duckdb::idx_t lang;
  duckdb::idx_t rettype;
  std::vector<duckdb::idx_t> argtypes;
  bool retset;
  bool strict;
  char volatility;
  std::string_view src;
};

class BuiltinFunctions {
 public:
  explicit BuiltinFunctions(duckdb::idx_t version,
                            std::vector<BuiltinFunction> functions);

  duckdb::idx_t Version() const noexcept { return _version; }
  std::span<const BuiltinFunction> All() const noexcept { return _functions; }
  const BuiltinFunction* Find(duckdb::idx_t oid) const {
    return FindByOid(_functions, oid);
  }
  std::span<const uint32_t> Named(std::string_view name) const {
    const auto it = _by_name.find(name);
    return it == _by_name.end() ? std::span<const uint32_t>{} : it->second;
  }

 private:
  duckdb::idx_t _version;
  std::vector<BuiltinFunction> _functions;
  irs::containers::FlatHashMap<std::string_view, std::vector<uint32_t>>
    _by_name;
};

std::shared_ptr<const BuiltinFunctions> GetBuiltinFunctions(
  duckdb::ClientContext& context);

}  // namespace sdb::pg
