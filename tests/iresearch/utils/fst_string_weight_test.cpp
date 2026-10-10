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

#include <iresearch/utils/fst/fst_string_ref_weight.hpp>
#include <iresearch/utils/fst/fst_string_weight.hpp>
#include <vector>

#include "tests_shared.hpp"

namespace {

irs::ByteWeight W(std::vector<irs::byte_type> bytes) {
  return irs::ByteWeight(bytes.begin(), bytes.end());
}

irs::bytes_view V(const irs::ByteWeight& w) { return w; }

}  // namespace

TEST(fst_byte_weight_test, construct) {
  const irs::ByteWeight empty;
  ASSERT_TRUE(empty.Empty());
  ASSERT_EQ(0, empty.Size());

  auto w = W({1, 2, 3});
  ASSERT_EQ(3, w.Size());
  ASSERT_EQ(2, w[1]);
  w.PushBack(4);
  w.PushBack(irs::bytes_view{W({5, 6})});
  ASSERT_EQ(V(W({1, 2, 3, 4, 5, 6})), V(w));
  w.Resize(2);
  ASSERT_EQ(V(W({1, 2})), V(w));
  w.Clear();
  ASSERT_TRUE(w.Empty());

  const irs::bstring moved = W({7, 8});
  ASSERT_EQ(irs::bytes_view{V(W({7, 8}))}, irs::bytes_view{moved});
}

TEST(fst_byte_weight_test, ref_weight) {
  const auto w = W({1, 2, 3});
  const irs::ByteRefWeight ref{V(w)};
  ASSERT_EQ(3, ref.Size());
  ASSERT_EQ(V(w), irs::bytes_view{ref});
  ASSERT_EQ(ref, irs::ByteRefWeight{V(w)});
  ASSERT_TRUE(irs::ByteRefWeight{}.Empty());
}

TEST(fst_byte_weight_test, plus) {
  ASSERT_EQ(V(W({1, 2})), irs::Plus(W({1, 2, 3, 4, 5, 6}), W({1, 2, 4})));
  ASSERT_EQ(V(W({1, 2})), irs::Plus(W({1, 2, 4}), W({1, 2, 3, 4, 5, 6})));
  ASSERT_EQ(V(W({1, 2, 3})), irs::Plus(W({1, 2, 3}), W({1, 2, 3})));
  ASSERT_EQ(V(W({1, 2})), irs::Plus(W({1, 2}), W({1, 2, 3})));
  ASSERT_TRUE(irs::Plus(W({1, 2, 3}), W({2, 3})).empty());
  ASSERT_TRUE(irs::Plus(W({1, 2, 3, 4, 5, 6}), irs::ByteWeight{}).empty());
  ASSERT_TRUE(irs::Plus(irs::ByteWeight{}, W({1, 2, 3, 4, 5, 6})).empty());
}

TEST(fst_byte_weight_test, times) {
  ASSERT_EQ(V(W({1, 2, 3, 4, 5, 6, 1, 2, 4})),
            V(irs::Times(W({1, 2, 3, 4, 5, 6}), W({1, 2, 4}))));
  ASSERT_EQ(V(W({1, 2, 4, 1, 2, 3, 4, 5, 6})),
            V(irs::Times(W({1, 2, 4}), W({1, 2, 3, 4, 5, 6}))));
  ASSERT_EQ(V(W({1, 2, 3})), V(irs::Times(W({1, 2, 3}), irs::ByteWeight{})));
  ASSERT_EQ(V(W({1, 2, 3})), V(irs::Times(irs::ByteWeight{}, W({1, 2, 3}))));
  ASSERT_TRUE(irs::Times(irs::ByteWeight{}, irs::ByteWeight{}).Empty());
}

TEST(fst_byte_weight_test, divide) {
  ASSERT_EQ(V(W({3, 4, 5, 6})),
            irs::DivideLeft(W({1, 2, 3, 4, 5, 6}), W({1, 2})));
  ASSERT_TRUE(irs::DivideLeft(W({1, 2}), W({1, 2, 3, 4, 5, 6})).empty());
  ASSERT_TRUE(irs::DivideLeft(W({1, 2, 3}), W({1, 2, 3})).empty());
  ASSERT_EQ(V(W({1, 2, 3, 4, 5, 6})),
            irs::DivideLeft(W({1, 2, 3, 4, 5, 6}), irs::ByteWeight{}));
  ASSERT_TRUE(irs::DivideLeft(irs::ByteWeight{}, W({1, 2, 3})).empty());
}
