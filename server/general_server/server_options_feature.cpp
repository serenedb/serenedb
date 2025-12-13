#include "general_server/server_options_feature.h"

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/application-exit.h"
#include "basics/exitcodes.h"
#include "basics/file_descriptors.h"
#include "basics/logger/logger.h"
#include "basics/physical_memory.h"
#include "basics/static_strings.h"
#include "general_server/state.h"
#include "rest/version.h"
#include "rest_server/environment_feature.h"

namespace sdb {

namespace {
uint64_t GetDefaultMemoryUsage() {
  if (physical_memory::GetValue() >= (static_cast<uint64_t>(4) << 30)) {
    // if we have at least 4GB of RAM, the default size is (RAM - 2GB) * 0.2
    return static_cast<uint64_t>(
      (physical_memory::GetValue() - (static_cast<uint64_t>(2) << 30)) * 0.2);
  }
  // if we have at least 2GB of RAM, the default size is 64MB
  return (static_cast<uint64_t>(64) << 20);
}
}  // namespace

ServerOptionsFeature::ServerOptionsFeature(Server& server)
  : SerenedFeature{server, name()} {
  _options.dump_limits.memory_usage = GetDefaultMemoryUsage();
  _options.descriptors_minimum = FileDescriptors::recommendedMinimum();
}

void ServerOptionsFeature::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  options->addOption(
    "--version", "Print the version and other related information, then exit.",
    new options::BooleanParameter(&_options.app_print_version),
    options::MakeDefaultFlags(options::Flags::Command));

  options->addOption(
    "--version-json",
    "Print the version and other related information in JSON "
    "format, then exit.",
    new options::BooleanParameter(&_options.app_print_json_version),
    options::MakeDefaultFlags(options::Flags::Command));

  options->addSection("database", "database options");

  options->addOption(
    "--database.ignore-datafile-errors",
    "Load collections even if datafiles may contain errors.",
    new options::BooleanParameter(&_options.database_ignore_datafile_errors),
    options::MakeDefaultFlags(options::Flags::Uncommon));

  options->addSection("cluster", "cluster");

  options->addOption(
    "--cluster.require-persisted-id",
    "If set to `true`, then the instance only starts if a UUID file is found "
    "in the database directory on startup. This ensures that the instance is "
    "started using an already existing database directory and not a new one. "
    "For the first start, you must either create the UUID file manually or "
    "set the option to `false` for the initial startup.",
    new options::BooleanParameter(&GetOptions().cluster_require_persisted_id));

  options
    ->addOption("--cluster.agency-endpoint",
                "Agency endpoint(s) to connect to.",
                new options::VectorParameter<options::StringParameter>(
                  &GetOptions().cluster_agency_endpoints),
                options::MakeFlags(options::Flags::DefaultNoComponents,
                                   options::Flags::OnCoordinator,
                                   options::Flags::OnDBServer))
    .setLongDescription(R"(You can specify this option multiple times to let
the server use a cluster of Agency servers.

Endpoints have the following pattern:

- `tcp://ipv4-address:port` - TCP/IP endpoint, using IPv4
- `tcp://[ipv6-address]:port` - TCP/IP endpoint, using IPv6
- `ssl://ipv4-address:port` - TCP/IP endpoint, using IPv4, SSL encryption
- `ssl://[ipv6-address]:port` - TCP/IP endpoint, using IPv6, SSL encryption

You must specify at least one endpoint or SereneDB refuses to start. It is
recommended to specify at least two endpoints, so that SereneDB has an
alternative endpoint if one of them becomes unavailable:

`--cluster.agency-endpoint tcp://192.168.1.1:4001
--cluster.agency-endpoint tcp://192.168.1.2:4002 ...`)");

  options
    ->addOption("--cluster.my-role", "This server's role.",
                new options::StringParameter(&GetOptions().cluster_my_role))
    .setLongDescription(R"(For a cluster, the possible values are `DBSERVER`
(backend data server) and `COORDINATOR` (frontend server for external and
application access).)");

  options
    ->addOption("--cluster.my-address",
                "This server's endpoint for cluster-internal communication.",
                new options::StringParameter(&GetOptions().cluster_my_endpoint),
                options::MakeFlags(options::Flags::DefaultNoComponents,
                                   options::Flags::OnCoordinator,
                                   options::Flags::OnDBServer))
    .setLongDescription(R"(If specified, the endpoint needs to be in one of
the following formats:

- `tcp://ipv4-address:port` - TCP/IP endpoint, using IPv4
- `tcp://[ipv6-address]:port` - TCP/IP endpoint, using IPv6
- `ssl://ipv4-address:port` - TCP/IP endpoint, using IPv4, SSL encryption
- `ssl://[ipv6-address]:port` - TCP/IP endpoint, using IPv6, SSL encryption

If you don't specify an endpoint, the server looks up its internal endpoint
address in the Agency. If no endpoint can be found in the Agency for the
server's ID, SereneDB refuses to start.

**Examples**

Listen only on the interface with the address `192.168.1.1`:

`--cluster.my-address tcp://192.168.1.1:8530`

Listen on all IPv4 and IPv6 addresses which are configured on port `8530`:

`--cluster.my-address ssl://[::]:8530`)");

  options
    ->addOption("--cluster.my-advertised-endpoint",
                "This server's advertised endpoint for external "
                "communication (optional, e.g. an external IP address or "
                "load balancer).",
                new options::StringParameter(
                  &GetOptions().cluster_my_advertised_endpoint),
                options::MakeFlags(options::Flags::DefaultNoComponents,
                                   options::Flags::OnCoordinator,
                                   options::Flags::OnDBServer))
    .setLongDescription(R"(If specified, the endpoint needs to be in one of
the following formats:

- `tcp://ipv4-address:port` - TCP/IP endpoint, using IPv4
- `tcp://[ipv6-address]:port` - TCP/IP endpoint, using IPv6
- `ssl://ipv4-address:port` - TCP/IP endpoint, using IPv4, SSL encryption
- `ssl://[ipv6-address]:port` - TCP/IP endpoint, using IPv6, SSL encryption

If you don't specify an advertised endpoint, no external endpoint is
advertised.

**Examples**

If an external interface is available to this server, you can specify it to
communicate with external software / drivers:

`--cluster.my-advertised-endpoint tcp://some.public.place:8530`

All specifications of endpoints apply.)");

  options
    ->addOption(
      "--cluster.write-concern",
      "The global default write concern used for writes to new "
      "collections.",
      new options::UInt32Parameter(&GetOptions().cluster_write_concern,
                                   /*base*/ 1, /*minValue*/ 1,
                                   /*maxValue*/ kMaxReplicationFactor),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnCoordinator))
    .setLongDescription(R"(This value is used as the default write concern
for databases, which in turn is used as the default for collections.

**Warning**: If you use multiple Coordinators, use the same value on all
Coordinators.)");

  options
    ->addOption("--cluster.default-replication-factor",
                "The default replication factor for non-system collections.",
                new options::UInt32Parameter(
                  &GetOptions().cluster_default_replication_factor, /*base*/ 1,
                  /*minValue*/ 1,
                  /*maxValue*/ kMaxReplicationFactor),
                options::MakeFlags(options::Flags::DefaultNoComponents,
                                   options::Flags::OnCoordinator))
    .setLongDescription(R"(If you don't set this option, it defaults to the
value of the `--cluster.min-replication-factor` option. If set, the value must
be between the values of `--cluster.min-replication-factor` and
`--cluster.max-replication-factor`.

Note that you can still adjust the replication factor per collection. This value
is only the default value used for new collections if no replication factor is
specified when creating a collection.

**Warning**: If you use multiple Coordinators, use the same value on all
Coordinators.)");

  options
    ->addOption("--cluster.min-replication-factor",
                "The minimum replication factor for new collections.",
                new options::UInt32Parameter(
                  &GetOptions().cluster_min_replication_factor, /*base*/ 1,
                  /*minValue*/ 1,
                  /*maxValue*/ kMaxReplicationFactor),
                options::MakeFlags(options::Flags::DefaultNoComponents,
                                   options::Flags::OnCoordinator))
    .setLongDescription(R"(If you change the value of this setting and
restart the servers, no changes are applied to existing collections that would
violate the new setting.

**Warning**: If you use multiple Coordinators, use the same value on all
Coordinators.)");

  options
    ->addOption("--cluster.max-replication-factor",
                "The maximum replication factor for new collections "
                "(0 = unrestricted).",
                // 10 is a hard-coded max value for the replication factor
                new options::UInt32Parameter(
                  &GetOptions().cluster_max_replication_factor, /*base*/ 1,
                  /*minValue*/ 0,
                  /*maxValue*/ kMaxReplicationFactor),
                options::MakeFlags(options::Flags::DefaultNoComponents,
                                   options::Flags::OnCoordinator))
    .setLongDescription(R"(If you change the value of this setting and
restart the servers, no changes are applied to existing collections that would
violate the new setting.

**Warning**: If you use multiple Coordinators, use the same value on all
Coordinators.)");

  options
    ->addOption(
      "--cluster.max-number-of-shards",
      "The maximum number of shards that can be configured when creating "
      "new collections (0 = unrestricted).",
      new options::UInt32Parameter(&GetOptions().cluster_max_number_of_shards,
                                   /*base*/ 1,
                                   /*minValue*/ 1),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnCoordinator))
    .setLongDescription(R"(If you change the value of this setting and
restart the servers, no changes are applied to existing collections that would
violate the new setting.

**Warning**: If you use multiple Coordinators, use the same value on all
Coordinators.)");

  options->addOption(
    "--cluster.create-waits-for-sync-replication",
    "Let the active Coordinator wait for all replicas to create collections.",
    new options::BooleanParameter(
      &GetOptions().cluster_create_waits_for_sync_replication),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnCoordinator,
                       options::Flags::OnDBServer, options::Flags::Uncommon));

  options->addOption(
    "--cluster.index-create-timeout",
    "The amount of time (in seconds) the Coordinator waits for an index to "
    "be created before giving up.",
    new options::DoubleParameter(&GetOptions().cluster_index_creation_timeout),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnCoordinator,
                       options::Flags::Uncommon));

  options
    ->addOption(
      "--cluster.api-jwt-policy",
      "Controls the access permissions required for accessing "
      "/_admin/cluster REST APIs (jwt-all = JWT required to access all "
      "operations, jwt-write = JWT required for POST/PUT/DELETE "
      "operations, jwt-compat = 3.7 compatibility mode)",
      new options::DiscreteValuesParameter<options::StringParameter>(
        &GetOptions().cluster_api_jwt_policy,
        {
          "jwt-all",
          "jwt-write",
          "jwt-compat",
        }),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnCoordinator))

    .setLongDescription(R"(The possible values for the option are:

- `jwt-all`: requires a valid JWT for all accesses to `/_admin/cluster` and its
  sub-routes. If you use this configuration, the **CLUSTER** and **NODES**
  sections of the web interface are disabled, as they rely on the ability to
  read data from several cluster APIs.

- `jwt-write`: requires a valid JWT for write accesses (all HTTP methods except
  GET) to `/_admin/cluster`. You can use this setting to allow privileged users
  to read data from the cluster APIs, but not to do any modifications.
  Modifications (carried out by write accesses) are then only possible by
  requests with a valid JWT.

  All existing permission checks for the cluster API routes are still in effect
  with this setting, meaning that read operations without a valid JWT may still
  require dedicated other permissions (as in v3.7).

- `jwt-compat`: no **additional** access checks are in place for the cluster
  APIs. However, all existing permissions checks for the cluster API routes are
  still in effect with this setting, meaning that all operations may still
  require dedicated other permissions (as in v3.7).

The default value is `jwt-compat`, which means that this option does not cause
any extra JWT checks compared to v3.7.)");

  options
    ->addOption("--cluster.max-number-of-move-shards",
                "The number of shards to be moved per rebalance operation. "
                "If set to 0, no shards are moved.",
                new options::UInt32Parameter(
                  &GetOptions().cluster_max_number_of_move_shards),
                options::MakeFlags(options::Flags::DefaultNoComponents,
                                   options::Flags::OnCoordinator))

    .setLongDescription(R"(This option limits the maximum number of move
shards operations that can be made when the **Rebalance Shards** button is
clicked in the web interface. For backwards compatibility, the default value is
`10`. A value of `0` disables the button.)");

  options
    ->addOption(
      "--cluster.failed-write-concern-status-code",
      "The HTTP status code to send if a shard has not enough in-sync "
      "replicas to fulfill the write concern.",
      new options::DiscreteValuesParameter<options::UInt32Parameter>(
        &GetOptions().cluster_status_code_failed_write_concern, {403, 503}),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnDBServer))

    .setLongDescription(R"(The default behavior is to return an HTTP
`403 Forbidden` status code. You can set the option to `503` to return a
`503 Service Unavailable`.)");

  options
    ->addOption("--cluster.connectivity-check-interval",
                "The interval (in seconds) in which cluster-internal "
                "connectivity checks are performed.",
                new options::UInt32Parameter(
                  &GetOptions().cluster_connectivity_check_interval),
                options::MakeFlags(options::Flags::DefaultNoComponents,
                                   options::Flags::OnCoordinator,
                                   options::Flags::OnDBServer))
    .setLongDescription(R"(Setting this option to a value greater than
zero makes Coordinators and DB-Servers run period connectivity checks
with approximately the specified frequency. The first connectivity check
is carried out approximately 15 seconds after server start.
Note that a random delay is added to the interval on each server, so that
different servers do not execute their connectivity checks all at the
same time.
Setting this option to a value of zero disables these connectivity checks.")");

  options->addSection("dump", "Dump limits");

  options
    ->addOption(
      "--dump.max-memory-usage",
      "Maximum memory usage (in bytes) to be used by all ongoing dumps.",
      new options::UInt64Parameter(&_options.dump_limits.memory_usage, 1,
                                   /*minimum*/ 16 * 1024 * 1024),
      sdb::options::MakeFlags(
        sdb::options::Flags::Dynamic, sdb::options::Flags::DefaultNoComponents,
        sdb::options::Flags::OnDBServer, sdb::options::Flags::OnSingle))

    .setLongDescription(
      R"(The approximate per-server maximum allowed memory usage value
for all ongoing dump actions combined.)");

  options
    ->addOption(
      "--dump.max-docs-per-batch",
      "Maximum number of documents per batch that can be used in a dump.",
      new options::UInt64Parameter(
        &_options.dump_limits.docs_per_batch_upper_bound, 1,
        /*minimum*/ _options.dump_limits.docs_per_batch_lower_bound),
      sdb::options::MakeFlags(
        sdb::options::Flags::Uncommon, sdb::options::Flags::DefaultNoComponents,
        sdb::options::Flags::OnDBServer, sdb::options::Flags::OnSingle))

    .setLongDescription(
      R"(Each batch in a dump can grow to at most this size.)");

  options
    ->addOption(
      "--dump.max-batch-size",
      "Maximum batch size value (in bytes) that can be used in a dump.",
      new options::UInt64Parameter(
        &_options.dump_limits.batch_size_upper_bound, 1,
        /*minimum*/ _options.dump_limits.batch_size_lower_bound),
      sdb::options::MakeFlags(
        sdb::options::Flags::Uncommon, sdb::options::Flags::DefaultNoComponents,
        sdb::options::Flags::OnDBServer, sdb::options::Flags::OnSingle))

    .setLongDescription(
      R"(Each batch in a dump can grow to at most this size.)");

  options
    ->addOption(
      "--dump.max-parallelism",
      "Maximum parallelism that can be used in a dump.",
      new options::UInt64Parameter(
        &_options.dump_limits.parallelism_upper_bound, 1,
        /*minimum*/ _options.dump_limits.parallelism_lower_bound),
      sdb::options::MakeFlags(
        sdb::options::Flags::Uncommon, sdb::options::Flags::DefaultNoComponents,
        sdb::options::Flags::OnDBServer, sdb::options::Flags::OnSingle))

    .setLongDescription(R"(Each dump action on a server can use at most
this many parallel threads. Note that end users can still start multiple
dump actions that run in parallel.)");

  options->addOption(
    "--server.descriptors-minimum",
    "The minimum number of file descriptors needed to start (0 = no "
    "minimum)",
    new options::UInt64Parameter(&_options.descriptors_minimum),
    sdb::options::MakeFlags(options::Flags::DefaultNoOs,
                            options::Flags::OsLinux, options::Flags::OsMac));
}

void ServerOptionsFeature::validateOptions(
  std::shared_ptr<options::ProgramOptions>) {
  if (_options.app_print_json_version) {
    vpack::Builder builder;
    {
      vpack::ObjectBuilder ob(&builder);
      rest::Version::getVPack(builder);

      builder.add("version", rest::Version::getServerVersion());
    }

    std::cout << builder.slice().toJson() << std::endl;
    exit(EXIT_SUCCESS);
  }

  if (_options.app_print_version) {
    std::cout << rest::Version::getServerVersion() << std::endl
              << std::endl
              << StaticStrings::kLgplNotice << std::endl
              << std::endl
              << rest::Version::getDetailed() << std::endl;
    exit(EXIT_SUCCESS);
  }

  if (_options.cluster_agency_endpoints.empty()) {
    ServerState::instance()->SetRole(ServerState::Role::Single);
  }

  if (_options.dump_limits.batch_size_lower_bound >
      _options.dump_limits.batch_size_upper_bound) {
    SDB_FATAL("xxxxx", Logger::CONFIG,
              "invalid value for --dump.max-batch-size. Please use a value ",
              "of at least ", _options.dump_limits.batch_size_lower_bound);
  }

  if (_options.dump_limits.parallelism_lower_bound >
      _options.dump_limits.parallelism_upper_bound) {
    SDB_FATAL("xxxxx", Logger::CONFIG,
              "invalid value for --dump.max-parallelism. Please use a value ",
              "of at least ", _options.dump_limits.parallelism_lower_bound);
  }

  if (_options.descriptors_minimum > 0 &&
      (_options.descriptors_minimum < FileDescriptors::kRequiredMinimum ||
       _options.descriptors_minimum > FileDescriptors::kMaximumValue)) {
    SDB_FATAL("xxxxx", Logger::STARTUP,
              "invalid value for --server.descriptors-minimum",
              ". must be between ", FileDescriptors::kRequiredMinimum, " and ",
              FileDescriptors::kMaximumValue);
  }

  if (auto r = FileDescriptors::adjustTo(
        static_cast<FileDescriptors::ValueType>(_options.descriptors_minimum));
      !r.ok()) {
    SDB_FATAL_EXIT_CODE("xxxxx", Logger::SYSCALL, EXIT_RESOURCES_TOO_LOW, r);
  }

  FileDescriptors current;
  if (auto r = FileDescriptors::load(current); !r.ok()) {
    SDB_FATAL_EXIT_CODE("xxxxx", Logger::SYSCALL, EXIT_RESOURCES_TOO_LOW,
                        "cannot get the file descriptors limit value: ", r);
  }

  SDB_INFO("xxxxx", Logger::SYSCALL,
           "file-descriptors (nofiles) hard limit is ",
           FileDescriptors::stringify(current.hard), ", soft limit is ",
           FileDescriptors::stringify(current.soft));

  auto required = std::max(
    static_cast<FileDescriptors::ValueType>(_options.descriptors_minimum),
    FileDescriptors::kRequiredMinimum);

  if (current.soft < required) {
    auto message = absl::StrCat(
      "file-descriptors (nofiles) soft limit is too low, currently ",
      FileDescriptors::stringify(current.soft), ". please raise to at least ",
      required, " (e.g. via ulimit -n ", required,
      ") or adjust the value of the startup option "
      "--server.descriptors-minimum");
    if (_options.descriptors_minimum == 0) {
      SDB_WARN("xxxxx", Logger::SYSCALL, message);
    } else {
      SDB_FATAL_EXIT_CODE("xxxxx", Logger::SYSCALL, EXIT_RESOURCES_TOO_LOW,
                          message);
    }
  }
}

void ServerOptionsFeature::prepare() {
  SDB_INFO("xxxxx", Logger::FIXME, rest::Version::getVerboseVersionString());
#ifdef __GLIBC__
  SDB_INFO("xxxxx", Logger::FIXME, StaticStrings::kLgplNotice);
#endif

  const auto& options = server().options();

#if defined(SDB_DEV) || defined(SDB_GTEST)
  SDB_WARN("xxxxx", Logger::FIXME, "This version is FOR DEVELOPMENT ONLY!");
  SDB_WARN("xxxxx", Logger::FIXME,
           "==================================================================="
           "================");

  if (!options) {
    return;
  }
#endif

  if (const auto& modernized_options = options->modernizedOptions();
      !modernized_options.empty()) {
    for (const auto& it : modernized_options) {
      SDB_WARN("xxxxx", Logger::STARTUP,
               "please note that the specified option '--", it.first,
               " has been renamed to '--", it.second, "'");
    }

    SDB_INFO("xxxxx", Logger::STARTUP,
             "please read the release notes about changed options");
  }

  options->walk(
    [](const auto&, const auto& option) {
      if (option.hasDeprecatedIn()) {
        SDB_WARN("xxxxx", Logger::STARTUP, "option '", option.displayName(),
                 "' is deprecated since ", option.deprecatedInString(),
                 " and may be removed or unsupported in a future version");
      }
    },
    true, true);

  options->walk(
    [](const auto&, const auto& option) {
      if (option.hasFlag(sdb::options::Flags::Obsolete)) {
        SDB_WARN("xxxxx", Logger::STARTUP, "obsolete option '",
                 option.displayName(), "' used in configuration. ",
                 "Setting this option does not have any effect.");
      }
    },
    true, true);

  options->walk(
    [](const auto&, const auto& option) {
      if (option.hasFlag(sdb::options::Flags::Experimental)) {
        SDB_WARN("xxxxx", Logger::STARTUP, "experimental option '",
                 option.displayName(), "' used in configuration.");
      }
    },
    true, true);

  PrintEnvironment();
}

void ServerOptionsFeature::unprepare() {
  SDB_INFO("xxxxx", Logger::FIXME, "SereneDB has been shut down");
}

const ServerOptions& GetServerOptions() {
  auto& feature = SerenedServer::Instance().getFeature<ServerOptionsFeature>();
  return feature.GetOptions();
}

ServerOptions& MutServerOptions() {
  auto& feature = SerenedServer::Instance().getFeature<ServerOptionsFeature>();
  return feature.GetOptions();
}

}  // namespace sdb
