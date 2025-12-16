#pragma once

#include "rest_server/serened.h"

namespace sdb {

// hard-coded limit for maximum replicationFactor value
inline constexpr uint32_t kMaxReplicationFactor = 10;

struct DumpLimits {
  uint64_t docs_per_batch_lower_bound = 10;
  uint64_t docs_per_batch_upper_bound = 1 * 1000 * 1000;
  uint64_t batch_size_lower_bound = 4 * 1024;
  uint64_t batch_size_upper_bound = 1024 * 1024 * 1024;
  uint64_t parallelism_lower_bound = 1;
  uint64_t parallelism_upper_bound = 8;
  uint64_t memory_usage = 512 * 1024 * 1024;
};

struct ServerOptions {
  DumpLimits dump_limits;

  uint64_t descriptors_minimum = 0;

  std::vector<std::string> cluster_agency_endpoints;
  std::string cluster_my_role;
  std::string cluster_my_endpoint;
  std::string cluster_my_advertised_endpoint;
  std::string cluster_api_jwt_policy = "jwt-compat";
  uint32_t cluster_write_concern = 1;
  uint32_t cluster_default_replication_factor = 1;
  uint32_t cluster_min_replication_factor = 1;
  uint32_t cluster_max_replication_factor = kMaxReplicationFactor;
  uint32_t cluster_max_number_of_shards = 1000;
  // maximum number of shards to be moved per rebalance operation
  // if value = 0, no move shards operations will be scheduled
  uint32_t cluster_max_number_of_move_shards = 10;
  // The following value indicates what HTTP status code should be returned if
  // a configured write concern cannot currently be fulfilled. The old
  // behavior (currently the default) means that a 403 Forbidden
  // with an error of 1004 ERROR_SERVER_READ_ONLY is returned. It is possible to
  // adjust the behavior so that an HTTP 503 Service Unavailable with an error
  // of 1429 ERROR_REPLICATION_WRITE_CONCERN_NOT_FULFILLED is returned.
  uint32_t cluster_status_code_failed_write_concern = 403;
  uint32_t cluster_connectivity_check_interval = 3600;  // seconds
  bool cluster_require_persisted_id = false;
  bool cluster_create_waits_for_sync_replication = true;
  /// coordinator timeout for index creation. defaults to 4 days
  double cluster_index_creation_timeout = 72.0 * 3600.0;

  bool database_ignore_datafile_errors = false;

  bool app_print_version = false;
  bool app_print_json_version = false;
};

class ServerOptionsFeature : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept {
    return "ServerOptionsFeature";
  }

  explicit ServerOptionsFeature(Server& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;
  void unprepare() final;

  auto& GetOptions(this auto& self) { return self._options; }

 private:
  ServerOptions _options;
};

const ServerOptions& GetServerOptions();
ServerOptions& MutServerOptions();

}  // namespace sdb
