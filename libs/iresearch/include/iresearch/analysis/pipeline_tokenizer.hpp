////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <tuple>

#include "basics/down_cast.h"
#include "basics/serializer.h"
#include "basics/shared.hpp"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "token_attributes.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

struct TokenizerConfig;

// An tokenizer capable of chaining other analyzers
class PipelineTokenizer final : public TypedAnalyzer<PipelineTokenizer>,
                                private util::Noncopyable {
 public:
  struct Options {
    using Owner = PipelineTokenizer;
    std::vector<std::unique_ptr<TokenizerConfig>> children;
  };
  static Analyzer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "pipeline"; }

  explicit PipelineTokenizer(std::vector<Analyzer::ptr> children);
  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    auto* attr = irs::GetMutable(_attrs, id);
    if (!attr) {
      // if attribute is not strictly pipeline-controlled let`s find nearest to
      // end provider with desired attribute
      if (irs::Type<PayAttr>::id() != id && irs::Type<OffsAttr>::id() != id) {
        for (auto it = rbegin(_pipeline); it != rend(_pipeline); ++it) {
          attr = const_cast<Analyzer&>(it->get_stream()).GetMutable(id);
          if (attr) {
            break;
          }
        }
      }
    }
    return attr;
  }
  bool next() final;
  bool reset(std::string_view data) final;

  /// @brief calls visitor on pipeline members in respective order. Visiting is
  /// interrupted on first visitor returning false.
  /// @param visitor visitor to call
  /// @return true if all visits returned true, false otherwise
  template<typename Visitor>
  bool visit_members(Visitor&& visitor) const {
    for (const auto& sub : _pipeline) {
      const auto& stream = sub.get_stream();
      if (stream.type() == type()) {
        // pipe inside pipe - forward visiting
        const auto& sub_pipe = sdb::basics::downCast<PipelineTokenizer>(stream);
        if (!sub_pipe.visit_members(visitor)) {
          return false;
        }
      } else if (!visitor(sub.get_stream())) {
        return false;
      }
    }
    return true;
  }

 private:
  struct SubAnalyzerT {
    explicit SubAnalyzerT(Analyzer::ptr a, bool track_offset);
    SubAnalyzerT();

    bool reset(uint32_t start, uint32_t end, std::string_view data) {
      data_size = data.size();
      data_start = start;
      data_end = end;
      pos = std::numeric_limits<uint32_t>::max();
      return _analyzer->reset(data);
    }
    bool next() {
      if (_analyzer->next()) {
        pos += inc->value;
        return true;
      }
      return false;
    }

    uint32_t start() const noexcept {
      SDB_ASSERT(offs);
      return data_start + offs->start;
    }

    uint32_t end() const noexcept {
      SDB_ASSERT(offs);
      return offs->end == data_size ? data_end
                                    : start() + offs->end - offs->start;
    }
    const TermAttr* term;
    const IncAttr* inc;
    const OffsAttr* offs;
    size_t data_size{0};
    uint32_t data_start{0};
    uint32_t data_end{0};
    uint32_t pos{std::numeric_limits<uint32_t>::max()};

    const Analyzer& get_stream() const noexcept {
      SDB_ASSERT(_analyzer);
      return *_analyzer;
    }

   private:
    // sub analyzer should be operated through provided next/release
    // methods to properly track positions/offsets
    Analyzer::ptr _analyzer;
  };
  using pipeline_t = std::vector<SubAnalyzerT>;
  using attributes = std::tuple<IncAttr, AttributePtr<TermAttr>,
                                AttributePtr<OffsAttr>, AttributePtr<PayAttr>>;

  pipeline_t _pipeline;
  pipeline_t::iterator _current;
  pipeline_t::iterator _top;
  pipeline_t::iterator _bottom;
  OffsAttr _offs;
  attributes _attrs;
};

template<typename Context>
void SerdeWrite(Context ctx, const PipelineTokenizer::Options& o) {
  if constexpr (std::is_same_v<typename Context::Format,
                               sdb::basics::ObjectFormat>) {
    sdb::basics::WriteObject(ctx.io(), std::tie(o.children), ctx.arg());
  } else {
    sdb::basics::WriteTuple(ctx.io(), std::tie(o.children), ctx.arg());
  }
}

template<typename Context>
void SerdeRead(Context ctx, PipelineTokenizer::Options& o) {
  auto refs = std::tie(o.children);
  if constexpr (std::is_same_v<typename Context::Format,
                               sdb::basics::ObjectFormat>) {
    sdb::basics::ReadObject(ctx.io(), refs, ctx.arg());
  } else {
    sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
  }
}

}  // namespace irs::analysis
