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

#ifdef _WIN32
#define STRING(str) L##str
#else
#define STRING(str) str
#endif

TEST(file_utils_tests, path_parts) {
  typedef irs::file_utils::PathPartsT::ref_t RefT;

  // nullptr
  {
    auto parts = irs::file_utils::PathParts(nullptr);
    ASSERT_EQ(RefT{}, parts.dirname);
    ASSERT_EQ(RefT{}, parts.basename);
    ASSERT_EQ(RefT{}, parts.stem);
    ASSERT_EQ(RefT{}, parts.extension);
  }

  // no parent

  // no parent, stem(empty), no extension
  {
    auto data = STRING("");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT{}, parts.dirname);
    ASSERT_EQ(RefT(), parts.basename);
    ASSERT_EQ(RefT(), parts.stem);
    ASSERT_EQ(RefT{}, parts.extension);
  }

  // no parent, stem(empty), extension(empty)
  {
    auto data = STRING(".");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT{}, parts.dirname);
    ASSERT_EQ(RefT(STRING(".")), parts.basename);
    ASSERT_EQ(RefT(STRING("")), parts.stem);
    ASSERT_EQ(RefT(STRING("")), parts.extension);
  }

  // no parent, stem(empty), extension(non-empty)
  {
    auto data = STRING(".xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT{}, parts.dirname);
    ASSERT_EQ(RefT(STRING(".xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

  // no parent, stem(non-empty), no extension
  {
    auto data = STRING("abc");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT{}, parts.dirname);
    ASSERT_EQ(RefT(STRING("abc")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc")), parts.stem);
    ASSERT_EQ(RefT{}, parts.extension);
  }

  // no parent, stem(non-empty), extension(empty)
  {
    auto data = STRING("abc.");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT{}, parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc")), parts.stem);
    ASSERT_EQ(RefT(STRING("")), parts.extension);
  }

  // no parent, stem(non-empty), extension(non-empty)
  {
    auto data = STRING("abc.xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT{}, parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

  // no parent, stem(non-empty), extension(non-empty) (multi-extension)
  {
    auto data = STRING("abc.def..xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT{}, parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.def..xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc.def.")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

  // empty parent

  // parent(empty), stem(empty), no extension
  {
    auto data = STRING("/");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("")), parts.dirname);
    ASSERT_EQ(RefT(STRING("")), parts.basename);
    ASSERT_EQ(RefT(STRING("")), parts.stem);
    ASSERT_EQ(RefT{}, parts.extension);
  }

  // parent(empty), stem(empty), extension(empty)
  {
    auto data = STRING("/.");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("")), parts.dirname);
    ASSERT_EQ(RefT(STRING(".")), parts.basename);
    ASSERT_EQ(RefT(STRING("")), parts.stem);
    ASSERT_EQ(RefT(STRING("")), parts.extension);
  }

  // parent(empty), stem(empty), extension(non-empty)
  {
    auto data = STRING("/.xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("")), parts.dirname);
    ASSERT_EQ(RefT(STRING(".xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

  // parent(empty), stem(non-empty), no extension
  {
    auto data = STRING("/abc");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("")), parts.dirname);
    ASSERT_EQ(RefT(STRING("abc")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc")), parts.stem);
    ASSERT_EQ(RefT{}, parts.extension);
  }

  // parent(empty), stem(non-empty), extension(empty)
  {
    auto data = STRING("/abc.");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("")), parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc")), parts.stem);
    ASSERT_EQ(RefT(STRING("")), parts.extension);
  }

  // parent(empty), stem(non-empty), extension(non-empty)
  {
    auto data = STRING("/abc.xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("")), parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

  // parent(empty), stem(non-empty), extension(non-empty) (multi-extension)
  {
    auto data = STRING("/abc.def..xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("")), parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.def..xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc.def.")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

  // non-empty parent

  // parent(non-empty), stem(empty), no extension
  {
    auto data = STRING("klm/");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("klm")), parts.dirname);
    ASSERT_EQ(RefT(STRING("")), parts.basename);
    ASSERT_EQ(RefT(STRING("")), parts.stem);
    ASSERT_EQ(RefT{}, parts.extension);
  }

  // parent(non-empty), stem(empty), extension(empty)
  {
    auto data = STRING("klm/.");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("klm")), parts.dirname);
    ASSERT_EQ(RefT(STRING(".")), parts.basename);
    ASSERT_EQ(RefT(STRING("")), parts.stem);
    ASSERT_EQ(RefT(STRING("")), parts.extension);
  }

  // parent(non-empty), stem(empty), extension(non-empty)
  {
    auto data = STRING("klm/.xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("klm")), parts.dirname);
    ASSERT_EQ(RefT(STRING(".xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

  // parent(non-empty), stem(non-empty), no extension
  {
    auto data = STRING("klm/abc");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("klm")), parts.dirname);
    ASSERT_EQ(RefT(STRING("abc")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc")), parts.stem);
    ASSERT_EQ(RefT{}, parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(empty)
  {
    auto data = STRING("klm/abc.");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("klm")), parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc")), parts.stem);
    ASSERT_EQ(RefT(STRING("")), parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(non-empty)
  {
    auto data = STRING("klm/abc.xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("klm")), parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(non-empty) (multi-extension)
  {
    auto data = STRING("klm/abc.def..xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("klm")), parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.def..xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc.def.")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(non-empty) (multi-parent,
  // multi-extension)
  {
    auto data = STRING("/123/klm/abc.def..xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(RefT(STRING("/123/klm")), parts.dirname);
    ASSERT_EQ(RefT(STRING("abc.def..xyz")), parts.basename);
    ASSERT_EQ(RefT(STRING("abc.def.")), parts.stem);
    ASSERT_EQ(RefT(STRING("xyz")), parts.extension);
  }

#ifdef _WIN32
  // win32 non-empty parent

  // parent(non-empty), stem(empty), no extension
  {
    auto data = STRING("klm\\");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(ref_t(STRING("klm")), parts.dirname);
    ASSERT_EQ(ref_t(STRING("")), parts.basename);
    ASSERT_EQ(ref_t(STRING("")), parts.stem);
    ASSERT_EQ(ref_t{}, parts.extension);
  }

  // parent(non-empty), stem(empty), extension(empty)
  {
    auto data = STRING("klm\\.");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(ref_t(STRING("klm")), parts.dirname);
    ASSERT_EQ(ref_t(STRING(".")), parts.basename);
    ASSERT_EQ(ref_t(STRING("")), parts.stem);
    ASSERT_EQ(ref_t(STRING("")), parts.extension);
  }

  // parent(non-empty), stem(empty), extension(non-empty)
  {
    auto data = STRING("klm\\.xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(ref_t(STRING("klm")), parts.dirname);
    ASSERT_EQ(ref_t(STRING(".xyz")), parts.basename);
    ASSERT_EQ(ref_t(STRING("")), parts.stem);
    ASSERT_EQ(ref_t(STRING("xyz")), parts.extension);
  }

  // parent(non-empty), stem(non-empty), no extension
  {
    auto data = STRING("klm\\abc");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(ref_t(STRING("klm")), parts.dirname);
    ASSERT_EQ(ref_t(STRING("abc")), parts.basename);
    ASSERT_EQ(ref_t(STRING("abc")), parts.stem);
    ASSERT_EQ(ref_t{}, parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(empty)
  {
    auto data = STRING("klm\\abc.");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(ref_t(STRING("klm")), parts.dirname);
    ASSERT_EQ(ref_t(STRING("abc.")), parts.basename);
    ASSERT_EQ(ref_t(STRING("abc")), parts.stem);
    ASSERT_EQ(ref_t(STRING("")), parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(non-empty)
  {
    auto data = STRING("klm\\abc.xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(ref_t(STRING("klm")), parts.dirname);
    ASSERT_EQ(ref_t(STRING("abc.xyz")), parts.basename);
    ASSERT_EQ(ref_t(STRING("abc")), parts.stem);
    ASSERT_EQ(ref_t(STRING("xyz")), parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(non-empty) (multi-extension)
  {
    auto data = STRING("klm\\abc.def..xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(ref_t(STRING("klm")), parts.dirname);
    ASSERT_EQ(ref_t(STRING("abc.def..xyz")), parts.basename);
    ASSERT_EQ(ref_t(STRING("abc.def.")), parts.stem);
    ASSERT_EQ(ref_t(STRING("xyz")), parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(non-empty) (multi-parent,
  // multi-extension)
  {
    auto data = STRING("/123\\klm/abc.def..xyz");
    auto parts = irs::file_utils::PathParts(data);
    ASSERT_EQ(ref_t(STRING("/123\\klm")), parts.dirname);
    ASSERT_EQ(ref_t(STRING("abc.def..xyz")), parts.basename);
    ASSERT_EQ(ref_t(STRING("abc.def.")), parts.stem);
    ASSERT_EQ(ref_t(STRING("xyz")), parts.extension);
  }
#endif
}

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
