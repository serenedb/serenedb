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

inline constexpr ObjectId kInvalid{};
inline constexpr ObjectId kMinSystem{1'000'000};
inline constexpr ObjectId kMaxSystem{2'000'000};
inline constexpr ObjectId kRootUser{kMinSystem};

// Database IDs
inline constexpr ObjectId kInstance{kMinSystem.id() + 4};
inline constexpr ObjectId kTombstoneDatabase{kMinSystem.id() + 1};
inline constexpr ObjectId kSystemDB{kMinSystem.id() + 2};

// Schema IDs
inline constexpr ObjectId kPgCatalogSchema{11};
inline constexpr ObjectId kPgInformationSchema{kMinSystem.id() + 3};

// Type IDs
inline constexpr ObjectId kVariant{kMinSystem.id() + 100};
inline constexpr ObjectId kVariantArray{kMinSystem.id() + 101};

}  // namespace id
}  // namespace sdb
