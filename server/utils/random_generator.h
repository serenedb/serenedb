////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <cstdint>

namespace sdb::random {

void Reset();
void Ensure();

int16_t Interval(int16_t l, int16_t r);
int32_t Interval(int32_t l, int32_t r);
int64_t Interval(int64_t l, int64_t r);

uint16_t Interval(uint16_t r);
uint32_t Interval(uint32_t r);
uint64_t Interval(uint64_t r);

uint64_t RandU64();

}  // namespace sdb::random
