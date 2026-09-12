////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string_view>

#include "iresearch/utils/shared.hpp"

namespace irs {

////////////////////////////////////////////////////////////////////////////////
/// @class type_info
/// @brief holds meta information obout a type, e.g. name and identifier
////////////////////////////////////////////////////////////////////////////////
class TypeInfo {
 public:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief unique type identifier
  /// @note can be used to get an instance of underlying type
  //////////////////////////////////////////////////////////////////////////////
  using type_id = TypeInfo (*)() noexcept;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief default constructor produces invalid type identifier
  //////////////////////////////////////////////////////////////////////////////
  constexpr TypeInfo() noexcept : TypeInfo{nullptr, {}} {}

  //////////////////////////////////////////////////////////////////////////////
  /// @return true if type_info is valid, false - otherwise
  //////////////////////////////////////////////////////////////////////////////
  constexpr explicit operator bool() const noexcept { return nullptr != _id; }

  //////////////////////////////////////////////////////////////////////////////
  /// @return true if current object is equal to a denoted by 'rhs'
  //////////////////////////////////////////////////////////////////////////////
  constexpr bool operator==(const TypeInfo& rhs) const noexcept {
    return _id == rhs._id;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @return type name
  //////////////////////////////////////////////////////////////////////////////
  constexpr std::string_view name() const noexcept { return _name; }

  //////////////////////////////////////////////////////////////////////////////
  /// @return type identifier
  //////////////////////////////////////////////////////////////////////////////
  constexpr type_id id() const noexcept { return _id; }

  template<typename H>
  friend H AbslHashValue(H h, const TypeInfo& info) {
    return H::combine(std::move(h), info._id);
  }

 private:
  template<typename T>
  friend struct Type;

  constexpr TypeInfo(type_id id, std::string_view name) noexcept
    : _id(id), _name(name) {}

  type_id _id;
  std::string_view _name;
};

}  // namespace irs
