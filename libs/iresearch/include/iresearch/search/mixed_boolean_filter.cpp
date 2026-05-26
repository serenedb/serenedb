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

#include "mixed_boolean_filter.hpp"

#include "basics/down_cast.h"
#include "iresearch/search/boolean_query.hpp"

namespace irs {

class MixedBooleanFilter::Buffer final : public Filter::PrepareBuffer {
 public:
  Buffer(const PrepareContext& ctx, const And& and_filter, const Or& or_filter)
    : _req{and_filter.CreateBuffer(ctx)} {
    const auto opt_ctx = ctx.Boost(or_filter.Boost());
    _opt.reserve(or_filter.size());
    for (const auto& f : or_filter) {
      _opt.emplace_back(f->CreateBuffer(opt_ctx));
    }
  }

  void PrepareSegment(const SubReader& segment) final {
    _req->PrepareSegment(segment);
    for (auto& b : _opt) {
      b->PrepareSegment(segment);
    }
  }

  void Merge(PrepareBuffer&& other) final {
    auto& rhs = sdb::basics::downCast<Buffer>(other);
    _req->Merge(std::move(*rhs._req));
    SDB_ASSERT(_opt.size() == rhs._opt.size());
    for (size_t i = 0; i < _opt.size(); ++i) {
      _opt[i]->Merge(std::move(*rhs._opt[i]));
    }
  }

  bool Empty() const noexcept final { return false; }

  Filter::Query::ptr Compile(const PrepareContext& ctx) && final {
    if (_req->Empty()) {
      return Filter::Query::empty();
    }
    auto req = std::move(*_req).Compile(ctx);
    std::vector<Query::ptr> opts;
    opts.reserve(_opt.size());
    for (auto& b : _opt) {
      opts.emplace_back(b->Empty() ? Filter::Query::empty()
                                   : std::move(*b).Compile(ctx));
    }
    auto q = memory::make_tracked<BoostQuery>(ctx.memory);
    q->PrepareFromQueries(std::move(req), std::move(opts));
    return q;
  }

 private:
  std::unique_ptr<PrepareBuffer> _req;
  std::vector<std::unique_ptr<PrepareBuffer>> _opt;
};

std::unique_ptr<Filter::PrepareBuffer> MixedBooleanFilter::CreateBuffer(
  const PrepareContext& ctx) const {
  if (_and->empty()) {
    return _or->CreateBuffer(ctx);
  }
  if (_or->empty()) {
    return _and->CreateBuffer(ctx);
  }
  return std::make_unique<Buffer>(ctx, *_and, *_or);
}

Filter::Query::ptr MixedBooleanFilter::prepare(
  const PrepareContext& ctx) const {
  if (_and->empty()) {
    return _or->prepare(ctx);
  }
  if (_or->empty()) {
    return _and->prepare(ctx);
  }
  Buffer buf{ctx, *_and, *_or};
  for (const auto& segment : ctx.index) {
    buf.PrepareSegment(segment);
  }
  return std::move(buf).Compile(ctx);
}

bool MixedBooleanFilter::equals(const Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<MixedBooleanFilter>(rhs);
  return *_and == *typed_rhs._and && *_or == *typed_rhs._or;
}

}  // namespace irs
