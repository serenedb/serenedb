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
#include <vpack/slice.h>
#include <vpack/value.h>

#include <string>
#include <string_view>

#include "basics/identifier.h"

namespace sdb {

class ClusterInfo;

class RevisionId final : public basics::Identifier {
 public:
  using Identifier::Identifier;

  static constexpr RevisionId none() { return RevisionId{0}; }
  static constexpr RevisionId max() {
    return RevisionId{std::numeric_limits<uint64_t>::max()};
  }

  static RevisionId lowerBound();
  static void track(RevisionId id);

  static RevisionId create();

  static RevisionId fromObject(vpack::Slice slice);
  static RevisionId fromString(vpack::Slice slice);
  static RevisionId fromHLC(std::string_view rid);
  static RevisionId fromNumber(vpack::Slice slice);

  bool isSet() const { return id() != 0; }

  RevisionId next() const { return RevisionId{id() + 1}; }

  RevisionId prev() const { return RevisionId{id() - 1}; }

  std::string toHLC() const;

  std::string_view toHLC(char* buffer) const;

  void toNumber(vpack::Builder& b) const { b.add(id()); }
};

static_assert(sizeof(RevisionId) == sizeof(RevisionId::BaseType));

}  // namespace sdb
