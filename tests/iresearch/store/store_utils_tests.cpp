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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/utils/bytes_utils.hpp>

#include "tests_shared.hpp"

namespace {

template<typename T>
void VencodeFromArray(T expected_value, size_t expected_length) {
  ASSERT_EQ(expected_length, irs::bytes_io<T>::vsize(expected_value));
  irs::byte_type buf[irs::bytes_io<T>::kMaxVSize]{};

  auto* end = buf;
  irs::WriteVarint<T>(expected_value, end);
  ASSERT_EQ(expected_length, std::distance(buf, end));

  const auto* ptr = buf;
  ASSERT_EQ(expected_value, irs::vread<T>(ptr));
  ASSERT_EQ(expected_length,
            std::distance(static_cast<const irs::byte_type*>(buf), ptr));
}

}  // namespace

TEST(store_utils_tests, vint_from_array) {
  VencodeFromArray(UINT32_C(0), 1);
  VencodeFromArray(UINT32_C(1), 1);
  VencodeFromArray(UINT32_C(100), 1);
  VencodeFromArray(UINT32_C(127), 1);
  VencodeFromArray(UINT32_C(128), 2);
  VencodeFromArray(UINT32_C(16383), 2);
  VencodeFromArray(UINT32_C(16384), 3);
  VencodeFromArray(UINT32_C(32000), 3);
  VencodeFromArray(UINT32_C(2097151), 3);
  VencodeFromArray(UINT32_C(2097152), 4);
  VencodeFromArray(UINT32_C(268435455), 4);
  VencodeFromArray(UINT32_C(268435456), 5);
  VencodeFromArray(std::numeric_limits<uint32_t>::max(), 5);
}

TEST(store_utils_tests, vlong_from_array) {
  VencodeFromArray(UINT64_C(0), 1);
  VencodeFromArray(UINT64_C(1), 1);
  VencodeFromArray(UINT64_C(100), 1);
  VencodeFromArray(UINT64_C(127), 1);
  VencodeFromArray(UINT64_C(128), 2);
  VencodeFromArray(UINT64_C(16383), 2);
  VencodeFromArray(UINT64_C(16384), 3);
  VencodeFromArray(UINT64_C(32000), 3);
  VencodeFromArray(UINT64_C(2097151), 3);
  VencodeFromArray(UINT64_C(2097152), 4);
  VencodeFromArray(UINT64_C(268435455), 4);
  VencodeFromArray(UINT64_C(268435456), 5);
  VencodeFromArray(UINT64_C(34359738367), 5);
  VencodeFromArray(UINT64_C(34359738368), 6);
  VencodeFromArray(UINT64_C(4398046511103), 6);
  VencodeFromArray(UINT64_C(4398046511104), 7);
  VencodeFromArray(UINT64_C(72057594037927935), 8);
  VencodeFromArray(UINT64_C(72057594037927936), 9);
  VencodeFromArray(std::numeric_limits<uint64_t>::max(), 10);
}
