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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/node_hash_map.h>

#include <functional>

#include "basics/down_cast.h"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/utils/hash_utils.hpp"

namespace irs {

struct IndexReader;
struct PreparedStateVisitor;

struct PrepareContext {
  const IndexReader& index;
  IResourceManager& memory = IResourceManager::gNoop;
  const Scorers& scorers = Scorers::kUnordered;
  const AttributeProvider* ctx = nullptr;
  score_t boost = kNoBoost;

  PrepareContext Boost(score_t boost) const noexcept {
    auto ctx = *this;
    ctx.boost *= boost;
    return ctx;
  }
};

struct ExecutionContext {
  const SubReader& segment;
  IResourceManager& memory = IResourceManager::gNoop;
  const Scorers& scorers = Scorers::kUnordered;
  const AttributeProvider* ctx = nullptr;
  // If enabled, wand would use first scorer from scorers
  WandContext wand;
};

// Base class for all user-side filters
class Filter {
 public:
  // Base class for all prepared(compiled) queries
  class Query : public memory::Managed {
   public:
    using ptr = memory::managed_ptr<const Query>;

    static Query::ptr empty();

    virtual DocIterator::ptr execute(const ExecutionContext& ctx) const = 0;

    virtual void visit(const SubReader& segment, PreparedStateVisitor& visitor,
                       score_t boost) const = 0;

    // test only member
    virtual score_t Boost() const noexcept = 0;
  };

  using ptr = std::unique_ptr<Filter>;

  virtual ~Filter() = default;

  IRS_FORCE_INLINE bool operator==(const Filter& rhs) const noexcept {
    return equals(rhs);
  }

  virtual Query::ptr prepare(const PrepareContext& ctx) const = 0;

  virtual TypeInfo::type_id type() const noexcept = 0;

  // kludge for optimization in And::prepare
  virtual score_t BoostImpl() const noexcept { return kNoBoost; }

 protected:
  virtual bool equals(const Filter& rhs) const noexcept {
    return type() == rhs.type();
  }
};

class FilterWithBoost : public Filter {
 public:
  score_t Boost() const noexcept { return _boost; }

  void boost(score_t boost) noexcept { _boost = boost; }

 private:
  score_t BoostImpl() const noexcept final { return Boost(); }

  score_t _boost = kNoBoost;
};

template<typename Type>
class FilterWithType : public FilterWithBoost {
 public:
  using FilterType = Type;

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<Type>::id();
  }
};

// Convenient base class filters with options
template<typename Options>
class FilterWithOptions : public FilterWithType<typename Options::FilterType> {
 public:
  using options_type = Options;
  using FilterType = typename options_type::FilterType;

  const options_type& options() const noexcept { return _options; }
  options_type* mutable_options() noexcept { return &_options; }

 protected:
  bool equals(const Filter& rhs) const noexcept override {
    return Filter::equals(rhs) &&
           _options == sdb::basics::downCast<FilterType>(rhs)._options;
  }

 private:
  [[no_unique_address]] options_type _options;
};

// Convenient base class for single field filters
template<typename Options>
class FilterWithField : public FilterWithOptions<Options> {
 public:
  using options_type = typename FilterWithOptions<Options>::options_type;
  using FilterType = typename options_type::FilterType;

  std::string_view field() const noexcept { return _field; }
  std::string* mutable_field() noexcept { return &_field; }

 protected:
  bool equals(const Filter& rhs) const noexcept final {
    return FilterWithOptions<options_type>::equals(rhs) &&
           _field == sdb::basics::downCast<FilterType>(rhs)._field;
  }

 private:
  std::string _field;
};

// Filter which returns no documents
class Empty final : public FilterWithType<Empty> {
 public:
  Query::ptr prepare(const PrepareContext& ctx) const final;
};

struct FilterVisitor;
using field_visitor =
  std::function<void(const SubReader&, const TermReader&, FilterVisitor&)>;

}  // namespace irs
