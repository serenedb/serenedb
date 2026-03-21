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

#include "revision_id.h"

#include <absl/strings/ascii.h>

#include <cstdint>
#include <limits>

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/hybrid_logical_clock.h"
#include "basics/number_utils.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "database/ticks.h"
#include "general_server/state.h"

namespace sdb {

RevisionId RevisionId::lowerBound() {
  static constexpr BaseType kTickLimit =
    BaseType{2016ULL - 1970ULL} * 1000ULL * 60ULL * 60ULL * 24ULL * 365ULL;
  // "2021-01-01T00:00:00.000Z" => 1609459200000 milliseconds since the epoch
  RevisionId value{uint64_t(1609459200000ULL) << 20ULL};
  SDB_ASSERT(value.id() > (kTickLimit << 20ULL));
  return value;
}

void RevisionId::track(RevisionId id) { NewTickHybridLogicalClock(id.id()); }

RevisionId RevisionId::create() {
  return RevisionId{NewTickHybridLogicalClock()};
}

RevisionId RevisionId::fromObject(vpack::Slice slice) {
  if (slice.isObject()) {
    return fromString(slice.get(StaticStrings::kRevString));
  }
  return {};
}

RevisionId RevisionId::fromString(vpack::Slice slice) {
  if (slice.isString()) {
    return fromHLC(slice.stringView());
  }
  return {};
}

RevisionId RevisionId::fromHLC(std::string_view rid) {
  return RevisionId{basics::HybridLogicalClock::decodeTimeStamp(rid)};
}

RevisionId RevisionId::fromNumber(vpack::Slice slice) {
  if (slice.isUInt()) {
    return RevisionId{slice.getUIntUnchecked()};
  }
  SDB_ASSERT(slice.isSmallInt());
  auto v = slice.getSmallIntUnchecked();
  SDB_ASSERT(v >= 0);
  return RevisionId{static_cast<BaseType>(v)};
}

std::string RevisionId::toHLC() const {
  return basics::HybridLogicalClock::encodeTimeStamp(id());
}

std::string_view RevisionId::toHLC(char* buffer) const {
  return basics::HybridLogicalClock::encodeTimeStamp(id(), buffer);
}

}  // namespace sdb
