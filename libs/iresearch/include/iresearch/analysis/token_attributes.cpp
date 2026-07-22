////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "token_attributes.hpp"

#include "basics/shared.hpp"

namespace irs {
namespace {

struct EmptyPosition final : PosAttr {
  Attribute* GetMutable(TypeInfo::type_id /*type*/) noexcept final {
    return nullptr;
  }

  bool next() final { return false; }
};

EmptyPosition gNoPosition;

}  // namespace

PosAttr& PosAttr::empty() noexcept { return gNoPosition; }

REGISTER_ATTRIBUTE(FreqAttr);
REGISTER_ATTRIBUTE(PosAttr);
REGISTER_ATTRIBUTE(OffsAttr);
REGISTER_ATTRIBUTE(PayAttr);

}  // namespace irs
