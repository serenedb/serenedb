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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "tests_shared.hpp"

using namespace irs;

TEST(field_meta_test, ctor) {
  {
    const FieldMeta fm;
    ASSERT_FALSE(field_limits::valid(fm.id));
    ASSERT_EQ(irs::IndexFeatures::None, fm.index_features);
    ASSERT_FALSE(field_limits::valid(fm.norm));
  }

  {
    const FieldMeta fm(42, irs::IndexFeatures::Offs);
    ASSERT_EQ(42u, fm.id);
    ASSERT_FALSE(field_limits::valid(fm.norm));
    ASSERT_EQ(irs::IndexFeatures::Offs, fm.index_features);
  }
}

TEST(field_meta_test, compare) {
  FieldMeta lhs;
  lhs.id = 7;
  lhs.norm = 42;
  FieldMeta rhs = lhs;
  rhs.norm = irs::field_limits::invalid();
  // Identity is `(id, index_features)`; `norm` is metadata only.
  ASSERT_EQ(lhs, rhs);

  rhs.id = 8;
  ASSERT_NE(lhs, rhs);
  lhs.id = 8;
  ASSERT_EQ(lhs, rhs);
}
