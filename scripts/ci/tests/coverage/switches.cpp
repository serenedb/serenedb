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

#include <utility>

namespace sdb::coverage {

enum class Kind {
  A,
  B,
};

int Exhaustive(Kind kind) {
  switch (kind) {
    case Kind::A:
      return 1;
    case Kind::B:
      return 2;
  }
  std::unreachable();
}

int WithDefault(int x) {
  switch (x) {
    case 1:
      return 10;
    case 2:
    case 3:
      return 20;
    default:
      return 30;
  }
}

int Run(int x) {
  return Exhaustive(Kind::A) + Exhaustive(Kind::B) + WithDefault(x) +
         WithDefault(x + 2) + WithDefault(x + 4);
}

}  // namespace sdb::coverage

int main(int argc, char**) { return sdb::coverage::Run(argc) - 63; }
