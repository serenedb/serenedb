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
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

}  // namespace duckdb
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
  std::string src;
};

class BuiltinFunctions {
 public:
  std::span<const BuiltinFunction> All() const noexcept { return _functions; }
  const BuiltinFunction* Find(duckdb::idx_t oid) const;
  std::span<const uint32_t> Named(std::string_view name) const;

 private:
  friend std::shared_ptr<const BuiltinFunctions> GetBuiltinFunctions(
    duckdb::ClientContext& context);

  std::vector<BuiltinFunction> _functions;
  irs::containers::FlatHashMap<std::string_view, std::vector<uint32_t>>
    _by_name;
  duckdb::idx_t _version = 0;
};

std::shared_ptr<const BuiltinFunctions> GetBuiltinFunctions(
  duckdb::ClientContext& context);

}  // namespace sdb::pg
