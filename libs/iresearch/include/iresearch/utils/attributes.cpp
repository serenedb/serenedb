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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "attributes.hpp"

#include "absl/strings/str_cat.h"
#include "basics/register.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {
namespace {

class AttributeRegister
  : public TaggedGenericRegister<std::string_view, TypeInfo, std::string_view,
                                 AttributeRegister> {};

}  // namespace

AttributeRegistrar::AttributeRegistrar(const TypeInfo& type,
                                       const char* source /*= nullptr*/) {
  const auto source_ref =
    source ? std::string_view{source} : std::string_view{};
  auto entry = AttributeRegister::instance().set(
    type.name(), type, IsNull(source_ref) ? nullptr : &source_ref);

  _registered = entry.second;

  if (!_registered && type != entry.first) {
    auto* registered_source = AttributeRegister::instance().tag(type.name());

    if (source && registered_source) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering "
               "attribute, ignoring: type '",
               type.name(), "' from ", source_ref, ", previously from ",
               *registered_source);
    } else if (source) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering "
               "attribute, ignoring: type '",
               type.name(), "' from ", source_ref);
    } else if (registered_source) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering "
               "attribute, ignoring: type '",
               type.name(), "' previously from ", *registered_source);
    } else {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering "
               "attribute, ignoring: type '",
               type.name(), "'");
    }
  }
}

}  // namespace irs
