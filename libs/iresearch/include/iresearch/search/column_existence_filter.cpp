////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "column_existence_filter.hpp"

#include "iresearch/index/index_reader.hpp"

namespace irs {
namespace {

class ColumnExistenceQuery : public Filter::Query {
 public:
  ColumnExistenceQuery(std::string_view field, score_t boost)
    : _field{field}, _boost{boost} {}

  DocIterator::ptr execute(const ExecutionContext& /*ctx*/) const override {
    return DocIterator::empty();
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 protected:
  std::string _field;
  score_t _boost;
};

}  // namespace

Filter::Query::ptr ByColumnExistence::prepare(const PrepareContext& ctx) const {
  return memory::make_tracked<ColumnExistenceQuery>(ctx.memory, field(),
                                                    ctx.boost * Boost());
}

}  // namespace irs
