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

#include "uniform_character.h"

#include <absl/algorithm/container.h>

#include "iresearch/utils/string_utils.h"
#include "server/utils/random_generator.h"

namespace irs::random {

char UniformCharacter::randomChar() const {
  size_t r = random::Interval(static_cast<uint32_t>(_characters.size() - 1));
  return _characters[r];
}

std::string UniformCharacter::random(size_t length) const {
  std::string buffer;
  basics::StrResize(buffer, length);
  for (auto& c : buffer) {
    c = randomChar();
  }
  return buffer;
}

}  // namespace irs::random
