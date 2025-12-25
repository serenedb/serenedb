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
#include "metrics/metrics_feature.h"

#include <frozen/unordered_set.h>
#include <vpack/builder.h>

#include <chrono>

#include "app/app_server.h"
#include "app/logger_feature.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/application-exit.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "general_server/state.h"
#include "metrics/metric.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "statistics/statistics_feature.h"
#include "storage_engine/engine_feature.h"

#ifdef SDB_CLUSTER
#include "agency/node.h"
#include "aql/query_registry_feature.h"
#include "cluster/cluster_metrics_feature.h"
#endif

namespace sdb::metrics {

MetricsFeature::MetricsFeature(Server& server)
  : SerenedFeature{server, name()},
    _export{true},
    _export_read_write_metrics{false},
    _ensure_whitespace{true},
    _usage_tracking_mode_string{"disabled"},
    _usage_tracking_mode{UsageTrackingMode::Disabled} {
  setOptional(false);
}

void MetricsFeature::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  _server_statistics =
    std::make_unique<ServerStatistics>(*this, StatisticsFeature::time());

  options->addOption(
    "--server.export-metrics-api", "Whether to enable the metrics API.",
    new options::BooleanParameter(&_export),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));

  options->addOption(
    "--server.export-read-write-metrics",
    "Whether to enable metrics for document reads and writes.",
    new options::BooleanParameter(&_export_read_write_metrics),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));

  options
    ->addOption(
      "--server.ensure-whitespace-metrics-format",
      "Set to `true` to ensure whitespace between the exported metric "
      "value and the preceding token (metric name or labels) in the "
      "metrics output.",
      new options::BooleanParameter(&_ensure_whitespace),
      sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon))

    .setLongDescription(R"(Using the whitespace characters in the output may
be required to make the metrics output compatible with some processing tools,
although Prometheus itself doesn't need it.)");

  options
    ->addOption(
      "--server.export-shard-usage-metrics",
      "Whether or not to export shard usage metrics.",
      new options::DiscreteValuesParameter<options::StringParameter>(
        &_usage_tracking_mode_string,
        {
          "disabled",
          "enabled-per-shard",
          "enabled-per-shard-per-user",
        }),
      sdb::options::MakeFlags(sdb::options::Flags::DefaultNoComponents,
                              sdb::options::Flags::OnDBServer))

    .setLongDescription(R"(This option can be used to make DB-Servers export
detailed shard usage metrics.

- By default, this option is set to `disabled` so that no shard usage metrics
  are exported.

- Set the option to `enabled-per-shard` to make DB-Servers collect per-shard
  usage metrics whenever a shard is accessed.

- Set this option to `enabled-per-shard-per-user` to make DB-Servers collect
  usage metrics per shard and per user whenever a shard is accessed.

Note that enabling shard usage metrics can produce a lot of metrics if there
are many shards and/or users in the system.)");
}

std::shared_ptr<Metric> MetricsFeature::doAdd(Builder& builder) {
  auto metric = builder.build();
  SDB_ASSERT(metric != nullptr);
  MetricKeyView key{metric->name(), metric->labels()};
  std::lock_guard lock{_mutex};
  auto [it, inserted] = _registry.try_emplace(key, metric);
  if (!inserted) {
    SDB_THROW(ERROR_INTERNAL,
              absl::StrCat(builder.type(), " ", metric->name(), ":",
                           metric->labels(), " already exists"));
  }
  return (*it).second;
}

std::shared_ptr<Metric> MetricsFeature::doAddDynamic(Builder& builder) {
  auto metric = builder.build();
  metric->setDynamic();
  SDB_ASSERT(metric != nullptr);
  MetricKeyView key{metric->name(), metric->labels()};
  {
    // happy path: check if metric already exists and if so, return it
    absl::ReaderMutexLock lock{&_mutex};
    if (auto it = _registry.find(key); it != _registry.end()) {
      return (*it).second;
    }
  }
  // slow path: create new metric under exclusive lock
  std::lock_guard lock{_mutex};
  // insertion can fail here because someone else concurrently inserted the
  // metric. this is fine, because in that case we simply return that
  // version.
  auto [it, inserted] = _registry.try_emplace(key, metric);
  return (*it).second;
}

Metric* MetricsFeature::get(const MetricKeyView& key) const {
  absl::ReaderMutexLock lock{&_mutex};
  auto it = _registry.find(key);
  if (it == _registry.end()) {
    return nullptr;
  }
  return it->second.get();
}

bool MetricsFeature::remove(const Builder& builder) {
  MetricKeyView key{builder.name(), builder.labels()};
  std::lock_guard guard{_mutex};
  return _registry.erase(key) != 0;
}

bool MetricsFeature::exportAPI() const noexcept { return _export; }

bool MetricsFeature::ensureWhitespace() const noexcept {
  return _ensure_whitespace;
}

MetricsFeature::UsageTrackingMode MetricsFeature::usageTrackingMode()
  const noexcept {
  return _usage_tracking_mode;
}

void MetricsFeature::validateOptions(std::shared_ptr<options::ProgramOptions>) {
  if (_export_read_write_metrics) {
    serverStatistics().setupDocumentMetrics();
  }

  // translate usage tracking mode string to enum value
  if (_usage_tracking_mode_string == "enabled-per-shard") {
    _usage_tracking_mode = UsageTrackingMode::EnabledPerShard;
  } else if (_usage_tracking_mode_string == "enabled-per-shard-per-user") {
    _usage_tracking_mode = UsageTrackingMode::EnabledPerShardPerUser;
  } else {
    _usage_tracking_mode = UsageTrackingMode::Disabled;
  }
}

void MetricsFeature::toPrometheus(std::string& result,
                                  MetricsParts metrics_parts,
                                  CollectMode mode) const {
  // minimize reallocs
  result.reserve(64 * 1024);

#ifdef SDB_CLUSTER
  if (metrics_parts.includeStandardMetrics()) {
    // QueryRegistryFeature only provides standard metrics.
    // update only necessary if these metrics should be included
    // in the output
    auto& q = server().getFeature<QueryRegistryFeature>();
    q.updateMetrics();
  }
#endif

  [[maybe_unused]] bool has_globals = false;
  {
    auto lock = initGlobalLabels();
    has_globals = _has_shortname && _has_role;
    std::string_view last;
    std::string_view curr;
    for (const auto& i : _registry) {
      SDB_ASSERT(i.second);
      if (i.second->isDynamic()) {
        if (!metrics_parts.includeDynamicMetrics()) {
          continue;
        }
      } else {
        if (!metrics_parts.includeStandardMetrics()) {
          continue;
        }
      }
      curr = i.second->name();
      if (last != curr) {
        last = curr;
        Metric::addInfo(result, curr, i.second->help(), i.second->type());
      }
      i.second->toPrometheus(result, _globals, _ensure_whitespace);
    }
    for (const auto& [_, batch] : _batch) {
      SDB_ASSERT(batch);
      // TODO(mbkkt) merge vector::reserve's between IBatch::toPrometheus
      batch->toPrometheus(result, _globals, _ensure_whitespace);
    }
  }

  if (metrics_parts.includeStandardMetrics()) {
    // StatisticsFeature only provides standard metrics
    auto& sf = server().getFeature<StatisticsFeature>();
    auto time = std::chrono::duration<double, std::milli>(
      std::chrono::system_clock::now().time_since_epoch());
    sf.toPrometheus(result, time.count(), _globals, _ensure_whitespace);

    // Storage engine only provides standard metrics
    auto& es = server().getFeature<EngineFeature>().engine();
    es.toPrometheus(result, _globals, _ensure_whitespace);

#ifdef SDB_CLUSTER
    // ClusterMetricsFeature only provides standard metrics
    auto& cm = server().getFeature<ClusterMetricsFeature>();
    if (has_globals && cm.isEnabled() && mode != CollectMode::Local) {
      cm.toPrometheus(result, _globals, _ensure_whitespace);
    }

    // agency node metrics only provide standard metrics
    consensus::Node::toPrometheus(result, _globals, _ensure_whitespace);
#endif
  }
}

////////////////////////////////////////////////////////////////////////////////
/// Sets metrics that can be collected by ClusterMetricsFeature
////////////////////////////////////////////////////////////////////////////////
constexpr auto kCoordinatorBatch =
  frozen::make_unordered_set<std::string_view>({
    "serenedb_search_link_stats",
  });

constexpr auto kCoordinatorMetrics =
  frozen::make_unordered_set<std::string_view>({
    "serenedb_search_num_failed_commits",
    "serenedb_search_num_failed_cleanups",
    "serenedb_search_num_failed_consolidations",
    "serenedb_search_commit_time",
    "serenedb_search_cleanup_time",
    "serenedb_search_consolidation_time",
  });

void MetricsFeature::toVPack(vpack::Builder& builder,
                             MetricsParts metrics_parts) const {
  builder.openArray(true);
  std::shared_lock lock{_mutex};
  for (const auto& i : _registry) {
    SDB_ASSERT(i.second);
    const auto name = i.second->name();
    if (kCoordinatorMetrics.count(name)) {
      i.second->toVPack(builder, server());
    }
  }
  for (const auto& [name, batch] : _batch) {
    SDB_ASSERT(batch);
    if (kCoordinatorBatch.count(name)) {
      batch->toVPack(builder, server());
    }
  }
  lock.unlock();
  builder.close();
}

ServerStatistics& MetricsFeature::serverStatistics() noexcept {
  return *_server_statistics;
}

std::shared_lock<absl::Mutex> MetricsFeature::initGlobalLabels() const {
  std::shared_lock reader_lock{_mutex};
  auto instance = ServerState::instance();
  if (!instance || (_has_shortname && _has_role)) {
    return reader_lock;
  }
  reader_lock.unlock();
  std::unique_lock writer_lock{_mutex};
  if (!_has_shortname) {
    // Very early after a server start it is possible that the short name
    // isn't yet known. This check here is to prevent that the label is
    // permanently empty if metrics are requested too early.
    if (auto shortname = instance->GetShortName(); !shortname.empty()) {
      _globals = absl::StrCat("shortname=\"", shortname, "\"",
                              (_globals.empty() ? "" : ","), _globals);
      _has_shortname = true;
    }
  }
  if (!_has_role) {
    if (auto role = instance->GetRole(); role != ServerState::Role::Undefined) {
      absl::StrAppend(&_globals, (_globals.empty() ? "" : ","), "role=\"",
                      ServerState::RoleToStr(role), "\"");
      _has_role = true;
    }
  }
  writer_lock.unlock();
  reader_lock.lock();
  return reader_lock;
}

std::pair<std::shared_lock<absl::Mutex>, metrics::IBatch*>
MetricsFeature::getBatch(std::string_view name) const {
  std::shared_lock lock{_mutex};
  metrics::IBatch* batch = nullptr;
  if (auto it = _batch.find(name); it != _batch.end()) {
    batch = it->second.get();
  } else {
    lock.unlock();
    lock.release();
  }
  return {std::move(lock), batch};
}

void MetricsFeature::batchRemove(std::string_view name,
                                 std::string_view labels) {
  std::unique_lock lock{_mutex};
  auto it = _batch.find(name);
  if (it == _batch.end()) {
    return;
  }
  SDB_ASSERT(it->second);
  if (it->second->remove(labels) == 0) {
    _batch.erase(name);
  }
}

MetricsFeature& GetMetrics() {
  return SerenedServer::Instance().getFeature<metrics::MetricsFeature>();
}

}  // namespace sdb::metrics
