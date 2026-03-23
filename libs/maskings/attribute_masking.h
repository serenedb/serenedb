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

#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <memory>
#include <string>
#include <string_view>

#include "basics/common.h"
#include "basics/containers/flat_hash_map.h"
#include "maskings/masking_function.h"
#include "maskings/parse_result.h"
#include "maskings/path.h"

namespace sdb {
namespace maskings {

void InstallMaskings();

class AttributeMasking {
 public:
  static ParseResult<AttributeMasking> parse(Maskings*, vpack::Slice def);
  static void installMasking(
    const std::string& name,
    ParseResult<AttributeMasking> (*func)(Path, Maskings*, vpack::Slice)) {
    gMaskings[name] = func;
  }

 public:
  AttributeMasking() = default;

  AttributeMasking(Path path, std::shared_ptr<MaskingFunction> func)
    : _path(std::move(path)), _func(std::move(func)) {}

  bool match(const std::vector<std::string_view>&) const;

  MaskingFunction* func() const { return _func.get(); }

 private:
  inline static containers::FlatHashMap<std::string,
                                        ParseResult<AttributeMasking> (*)(
                                          Path, Maskings*, vpack::Slice)>
    gMaskings;

 private:
  Path _path;
  std::shared_ptr<MaskingFunction> _func;
};

}  // namespace maskings
}  // namespace sdb
