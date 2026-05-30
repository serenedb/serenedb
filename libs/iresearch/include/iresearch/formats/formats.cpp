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

#include "formats.hpp"

#include "basics/register.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

constexpr std::string_view kFileNamePrefix = "libformat-";

class FormatRegister
  : public TaggedGenericRegister<std::string_view, Format::ptr (*)(),
                                 std::string_view, FormatRegister> {};

}  // namespace

bool formats::Exists(std::string_view name, bool load_library /*= true*/) {
  return nullptr != FormatRegister::instance().get(name, load_library);
}

Format::ptr formats::Get(std::string_view name,
                         bool load_library /*= true*/) noexcept {
  try {
    auto* factory = FormatRegister::instance().get(name, load_library);

    return factory ? factory() : nullptr;
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              "Caught exception while getting a format instance");
  }

  return nullptr;
}

void formats::LoadAll(std::string_view path) {
  LoadLibraries(path, kFileNamePrefix, "");
}

bool formats::Visit(const std::function<bool(std::string_view)>& visitor) {
  return FormatRegister::instance().visit(
    [&](const FormatRegister::key_type& name) { return visitor(name); });
}

FormatRegistrar::FormatRegistrar(const TypeInfo& type, Format::ptr (*factory)(),
                                 const char* source /*= nullptr*/) {
  const auto source_view =
    source ? std::string_view{source} : std::string_view{};

  auto entry = FormatRegister::instance().set(
    type.name(), factory, IsNull(source_view) ? nullptr : &source_view);

  _registered = entry.second;

  if (!_registered && factory != entry.first) {
    const auto* registered_source = FormatRegister::instance().tag(type.name());

    if (source && registered_source) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering format, "
               "ignoring: type '",
               type.name(), "' from ", source, ", previously from ",
               *registered_source);
    } else if (source) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering format, "
               "ignoring: type '",
               type.name(), "' from ", source);
    } else if (registered_source) {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering format, "
               "ignoring: type '",
               type.name(), "', previously from ", *registered_source);
    } else {
      SDB_WARN(IRESEARCH,
               "type name collision detected while registering format, "
               "ignoring: type '",
               type.name(), "'");
    }
  }
}

}  // namespace irs
