////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/utils/file_utils_ext.hpp>

#include "tests_shared.hpp"

TEST(file_utils_tests, residency_map) {
  irs::file_utils::ResidencyMap map;
  map.Reset(200);
  ASSERT_FALSE(map.Valid(7));
  ASSERT_TRUE(map.Adopt(7));
  ASSERT_TRUE(map.Valid(7));
  ASSERT_FALSE(map.Test(0, 199));

  map.Set(60, 130);
  ASSERT_TRUE(map.Test(60, 130));
  ASSERT_TRUE(map.Test(63, 64));
  ASSERT_TRUE(map.Test(128));
  ASSERT_FALSE(map.Test(59));
  ASSERT_FALSE(map.Test(131));
  ASSERT_FALSE(map.Test(59, 130));
  ASSERT_FALSE(map.Test(60, 131));

  map.Set(199);
  ASSERT_TRUE(map.Test(199));
  ASSERT_FALSE(map.Test(198, 199));

  ASSERT_FALSE(map.Adopt(6));
  ASSERT_TRUE(map.Valid(7));
  ASSERT_TRUE(map.Test(60, 130));

  ASSERT_TRUE(map.Adopt(8));
  ASSERT_TRUE(map.Valid(8));
  ASSERT_FALSE(map.Valid(7));
  ASSERT_FALSE(map.Test(60));
  ASSERT_FALSE(map.Test(199));
}

TEST(file_utils_tests, residency_epoch) {
  const auto epoch = irs::file_utils::ResidencyEpoch();
  irs::file_utils::InvalidateResidency();
  ASSERT_NE(epoch, irs::file_utils::ResidencyEpoch());
}
