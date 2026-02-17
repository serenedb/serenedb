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

#include "basics/identifier.h"

namespace sdb {

class ObjectId : public basics::Identifier {
 public:
  using Identifier::Identifier;

  static constexpr ObjectId none() { return ObjectId{0}; }

  bool isSet() const { return id() != 0; }
};

static_assert(sizeof(ObjectId) == sizeof(ObjectId::BaseType));

namespace id {  // system IDs

// User IDs
inline constexpr ObjectId kInvalid{};
inline constexpr ObjectId kRootUser{1000000};

// Database IDs
inline constexpr ObjectId kRoot{1000004};
inline constexpr ObjectId kTombstoneDatabase{1000001};
inline constexpr ObjectId kSystemDB{1000002};
inline constexpr ObjectId kMaxSystem{2000000};
inline constexpr ObjectId kCalculationDB{std::numeric_limits<uint64_t>::max()};

// Schema IDs
inline constexpr ObjectId kPgCatalogSchema{11};
inline constexpr ObjectId kPgInformationSchema{1000003};

}  // namespace id

}  // namespace sdb
