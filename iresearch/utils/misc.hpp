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

#pragma once

#include <array>
#include <memory>
#include <type_traits>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs {

// Convenient helper for simulating 'try/catch/finally' semantic
template<typename Func>
class [[nodiscard]] Finally {
 public:
  static_assert(std::is_nothrow_invocable_v<Func>);

  // If you need some of it, please use absl::Cleanup
  Finally(Finally&&) = delete;
  Finally(const Finally&) = delete;
  Finally& operator=(Finally&&) = delete;
  Finally& operator=(const Finally&) = delete;

  Finally(Func&& func) : _func{std::move(func)} {}

  ~Finally() noexcept { _func(); }

 private:
  [[no_unique_address]] Func _func;
};

}  // namespace irs
