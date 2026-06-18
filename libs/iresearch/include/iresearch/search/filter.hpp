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
#include <absl/functional/any_invocable.h>

#include <functional>
#include <span>

#include "basics/down_cast.h"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/utils/hash_utils.hpp"

namespace irs {

struct IndexReader;
struct PreparedStateVisitor;

struct PrepareContext {
  PrepareCollector* collector = nullptr;
  IResourceManager& memory = IResourceManager::gNoop;
  const AttributeProvider* ctx = nullptr;
  score_t boost = kNoBoost;

  void Boost(score_t b) noexcept { boost *= b; }
};

struct ExecutionContext {
  IResourceManager& memory = IResourceManager::gNoop;
  const AttributeProvider* ctx = nullptr;
  const DocumentMask* pending_docs_mask = nullptr;
  // If enabled, wand would use first scorer from scorers
  WandContext wand{};
};

inline IndexFeatures GetFeatures(const Scorer* scorer) noexcept {
  return scorer ? scorer->GetIndexFeatures() : IndexFeatures::None;
}

// Per-segment query builder
class QueryBuilder : public memory::Managed {
 public:
  using ptr = memory::managed_ptr<const QueryBuilder>;

  QueryBuilder(const SubReader& segment) noexcept : _segment{segment} {}

  virtual ~QueryBuilder() = default;

  static QueryBuilder::ptr Empty();
  virtual DocIterator::ptr Execute(const ExecutionContext& ctx,
                                   const StatsBuffer& stats) const = 0;

  virtual void Visit(PreparedStateVisitor&, score_t boost) const = 0;

  virtual score_t Boost() const noexcept = 0;

 protected:
  const SubReader& _segment;
};

// Base class for all user-side filters
class Filter {
 public:
  using ptr = std::unique_ptr<Filter>;

  virtual ~Filter() = default;

  IRS_FORCE_INLINE bool operator==(const Filter& rhs) const noexcept {
    return equals(rhs);
  }

  virtual QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx) const = 0;

  // Allocate the statistics collector this filter expects in PrepareSegment.
  // The default collects nothing.
  virtual PrepareCollector::ptr MakeCollector(const Scorer* scorer) const;

  virtual TypeInfo::type_id type() const noexcept = 0;

  virtual std::span<Filter::ptr> GetChildren() { return {}; }

  // kludge for optimization in And::prepare
  virtual score_t BoostImpl() const noexcept { return kNoBoost; }

  static Filter::ptr empty();

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

template<typename Options>
class FilterWithField : public FilterWithOptions<Options> {
 public:
  using options_type = typename FilterWithOptions<Options>::options_type;
  using FilterType = typename options_type::FilterType;

  irs::field_id field_id() const noexcept { return _field_id; }
  irs::field_id* mutable_field_id() noexcept { return &_field_id; }

 protected:
  bool equals(const Filter& rhs) const noexcept final {
    if (!FilterWithOptions<options_type>::equals(rhs)) {
      return false;
    }
    const auto& r = sdb::basics::downCast<FilterType>(rhs);
    return _field_id == r._field_id;
  }

 private:
  irs::field_id _field_id{irs::field_limits::invalid()};
};

// Filter which returns no documents
class Empty final : public FilterWithType<Empty> {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
};

struct FilterVisitor;
using field_visitor =
  absl::AnyInvocable<void(const SubReader&, const TermReader&, FilterVisitor&)>;

}  // namespace irs
