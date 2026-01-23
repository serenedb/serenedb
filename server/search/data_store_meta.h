////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vpack/builder.h>

#include <iresearch/index/index_writer.hpp>

namespace sdb::search {

struct DataStoreMeta {
  class ConsolidationPolicy {
   public:
    ConsolidationPolicy() = default;
    ConsolidationPolicy(irs::ConsolidationPolicy&& policy,
                        vpack::Builder&& properties) noexcept
      : _policy(std::move(policy)), _properties(std::move(properties)) {}

    const irs::ConsolidationPolicy& policy() const noexcept { return _policy; }

    vpack::Slice properties() const noexcept { return _properties.slice(); }

   private:
    irs::ConsolidationPolicy _policy;
    vpack::Builder _properties;
  };

  DataStoreMeta();

  void storeFull(const DataStoreMeta& other);
  void storeFull(DataStoreMeta&& other) noexcept;
  void storePartial(DataStoreMeta&& other) noexcept;

  struct Mask {
    bool cleanup_interval_step;
    bool commit_interval_msec;
    bool consolidation_interval_msec;
    bool consolidation_policy;
    bool version;
    bool writebuffer_active;
    bool writebuffer_idle;
    bool writebuffer_size_max;
    explicit Mask(bool mask = false) noexcept;
  };

  bool operator==(const DataStoreMeta& other) const noexcept;

  bool init(vpack::Slice slice, std::string& error_field,
            const DataStoreMeta& defaults, Mask* mask) noexcept;

  bool json(vpack::Builder& builder,
            const DataStoreMeta* ignore_equal = nullptr,
            const Mask* mask = nullptr) const;

  size_t cleanup_interval_step{};
  size_t commit_interval_msec{};
  size_t consolidation_interval_msec{};
  ConsolidationPolicy consolidation_policy;
  uint32_t version{};
  size_t writebuffer_active{};
  size_t writebuffer_idle{};
  size_t writebuffer_size_max{};
};

}  // namespace sdb::search
