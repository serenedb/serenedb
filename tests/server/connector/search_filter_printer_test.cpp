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

#include <iresearch/search/filters/filter.hpp>
#include <iresearch/utils/type_id.hpp>
#include <string_view>

#include "connector/search_filter_printer.hpp"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

class Unprintable final : public irs::Filter {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "Unprintable";
  }

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader&, const irs::PrepareContext&) const final {
    return irs::QueryBuilder::Empty();
  }

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<Unprintable>::id();
  }
};

TEST(search_filter_printer, a_filter_it_cannot_describe_prints_its_type) {
  const Unprintable filter;
  const auto node = irs::ToExplainNode(filter);
  EXPECT_EQ("Unknown", node.label);
  ASSERT_EQ(1, node.attributes.size());
  EXPECT_EQ("Type", node.attributes.begin()->first);
  EXPECT_EQ("Unprintable", node.attributes.begin()->second.ToString());
  EXPECT_TRUE(node.children.empty());
}

}  // namespace
}  // namespace sdb::connector
