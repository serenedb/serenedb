////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include "basics/memory.hpp"
#include "iresearch/utils/type_id.hpp"

namespace irs {

// Base struct for all attribute types that can be used with attribute_provider.
struct Attribute {};

// Base class for all objects with externally visible attributes
struct AttributeProvider : memory::Managed {
  // Return pointer to attribute of a specified type.
  // External users should prefer using const version.
  // External users should avoid modifying attributes treat that as UB.
  virtual Attribute* GetMutable(TypeInfo::type_id type) = 0;
};

// Convenient helper for getting mutable attribute of a specific type.
template<typename T, typename Provider>
inline T* GetMutable(Provider* absl_nonnull attrs) {
  static_assert(std::is_base_of_v<Attribute, T>);
  return static_cast<T*>(attrs->GetMutable(Type<T>::id()));
}

// Convenient helper for getting immutable attribute of a specific type.
template<typename T, typename Provider>
inline const T* get(const Provider& attrs) {
  return GetMutable<T>(const_cast<Provider*>(&attrs));
}

}  // namespace irs
