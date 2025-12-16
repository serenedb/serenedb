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

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <iosfwd>
#include <utility>

namespace sdb::basics {

class Identifier {
 public:
  using BaseType = uint64_t;

  constexpr Identifier() noexcept = default;
  constexpr explicit Identifier(BaseType id) noexcept : _id{id} {}

  constexpr BaseType id() const noexcept { return _id; }
  const BaseType* data() const noexcept { return &_id; }

  bool operator==(const Identifier& other) const noexcept = default;
  auto operator<=>(const Identifier& other) const noexcept = default;

  template<typename H>
  friend H AbslHashValue(H h, Identifier id) {
    return H::combine(std::move(h), id.id());
  }

  template<typename Sink>
  friend void AbslStringify(Sink& sink, Identifier value) {
    sink.Append(absl::StrCat(value.id()));
  }

 private:
  BaseType _id{};
};

static_assert(sizeof(Identifier) == sizeof(Identifier::BaseType));

void VPackRead(auto ctx, Identifier& id) {
  id = Identifier{ctx.vpack().getUInt()};
}

void VPackWrite(auto ctx, Identifier id) { ctx.vpack().add(id.id()); }

}  // namespace sdb::basics
