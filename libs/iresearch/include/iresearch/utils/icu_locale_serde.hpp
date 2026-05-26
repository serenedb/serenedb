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

#include "basics/serializer.h"

#include <string>
#include <string_view>
#include <unicode/locid.h>
#include <unicode/uversion.h>

namespace irs {

inline icu::Locale MakeBogusLocale() {
  icu::Locale l{"C"};
  l.setToBogus();
  return l;
}

}  // namespace irs

U_NAMESPACE_BEGIN

template<typename Context>
void SerdeWrite(Context ctx, const icu::Locale& locale) {
  if (locale.isBogus()) {
    sdb::basics::detail::WriteString(ctx.io(), std::string_view{});
  } else {
    sdb::basics::detail::WriteString(ctx.io(), std::string_view{locale.getName()});
  }
}

template<typename Context>
void SerdeRead(Context ctx, icu::Locale& locale) {
  const std::string name = ctx.io().ReadString();
  if (name.empty()) {
    locale = icu::Locale{"C"};
    locale.setToBogus();
    return;
  }
  locale = icu::Locale::createFromName(name.c_str());
}

U_NAMESPACE_END
