////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <string>
#include <string_view>
#include <text_locale.hpp>

#include "iresearch/utils/serializer.hpp"

namespace duckdb::text {

template<typename Context>
void SerdeWrite(Context ctx, const Locale& locale) {
  irs::utils::detail::WriteString(ctx.io(), std::string_view{locale.GetName()});
}

template<typename Context>
void SerdeRead(Context ctx, Locale& locale) {
  const std::string name = ctx.io().ReadString();
  locale = name.empty() ? Locale{} : Locale::FromName(name);
}

}  // namespace duckdb::text
