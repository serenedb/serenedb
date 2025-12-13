// Copyright 2005-2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Google compatibility definitions.

#include <fst/compat.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#include <vector>

namespace fst {

CheckSummer::CheckSummer() : count_(0) {
  check_sum_.resize(kCheckSumLength, '\0');
}

void CheckSummer::Reset() {
  count_ = 0;
  for (int i = 0; i < kCheckSumLength; ++i) check_sum_[i] = '\0';
}

void CheckSummer::Update(std::string_view data) {
  for (int i = 0; i < data.size(); ++i) {
    check_sum_[(count_++) % kCheckSumLength] ^= data[i];
  }
}

}  // namespace fst
