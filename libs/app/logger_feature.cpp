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

#include <absl/strings/str_join.h>

#include <ranges>

#ifdef SERENEDB_HAVE_GETGRGID
#include <grp.h>
#endif

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "app/app_server.h"
#include "app/options/option.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/application-exit.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/file_utils.h"
#include "basics/logger/appender.h"
#include "basics/logger/appender_file.h"
#include "basics/logger/log_time_format.h"
#include "basics/logger/logger.h"
#include "basics/number_utils.h"
#include "basics/operating-system.h"
#include "basics/string_utils.h"
#include "basics/thread.h"
#include "logger_feature.h"

namespace sdb {

using namespace options;

LoggerFeature::LoggerFeature(app::AppServer& server, bool threaded)
  : AppFeature(server, name()),
    _time_format_string(log_time_formats::DefaultFormatName()),
    _threaded(threaded) {
  // note: we use the _threaded option to determine whether we are serened
  // (_threaded = true) or one of the client tools (_threaded = false). in
  // the latter case we disable some options for the Logger, which only make
  // sense when we are running in server mode
  setOptional(false);

  _levels.push_back("info");

  // if stdout is a tty, then the default for _foreground_tty becomes true
  _foreground_tty = (isatty(STDOUT_FILENO) == 1);
}

LoggerFeature::~LoggerFeature() { log::Shutdown(); }

void LoggerFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
  options->addSection("log", "logging");

  options->addOption("--log.color", "Use colors for TTY logging.",
                     new BooleanParameter(&_use_color),
                     options::MakeDefaultFlags(options::Flags::Dynamic));

  options
    ->addOption("--log.escape-control-chars",
                "Escape control characters in log messages.",
                new BooleanParameter(&_use_control_escaped))

    .setLongDescription(R"(This option applies to the control characters,
that have hex codes below `\x20`, and also the character `DEL` with hex code
`\x7f`.

If you set this option to `false`, control characters are retained when they
have a visible representation, and replaced with a space character in case they
do not have a visible representation. For example, the control character `\n`
is visible, so a `\n` is displayed in the log. Contrary, the control character
`BEL` is not visible, so a space is displayed instead.

If you set this option to `true`, the hex code for the character is displayed,
for example, the `BEL` character is displayed as `\x07`.

The default value for this option is `true` to ensure compatibility with
previous versions.

A side effect of turning off the escaping is that it reduces the CPU overhead
for the logging. However, this is only noticeable if logging is set to a very
verbose level (e.g. `debug` or `trace`).)");

  options
    ->addOption("--log.escape-unicode-chars",
                "Escape Unicode characters in log messages.",
                new BooleanParameter(&_use_unicode_escaped))

    .setLongDescription(R"(If you set this option to `false`, Unicode
characters are retained and written to the log as-is. For example, `犬` is
logged as `犬`.

If you set this options to `true`, any Unicode characters are escaped, and the
hex codes for all Unicode characters are logged instead. For example, `犬` is
logged as `\u72AC`.

The default value for this option is set to `false` for compatibility with
previous versions.

A side effect of turning off the escaping is that it reduces the CPU overhead
for the logging. However, this is only noticeable if logging is set to a very
verbose level (e.g. `debug` or `trace`).)");

  options
    ->addOption("--log.output,-o",
                "Log destination(s), e.g. "
                "file:///path/to/file"
                " (any occurrence of $PID is replaced with the process ID).",
                new VectorParameter<StringParameter>(&_output))
    .setLongDescription(R"(This option allows you to direct the global or
per-topic log messages to different outputs. The output definition can be one
of the following:

- `-` for stdin
- `+` for stderr
- `syslog://<syslog-facility>`
- `syslog://<syslog-facility>/<application-name>`
- `file://<relative-or-absolute-path>`

To set up a per-topic output configuration, use
`--log.output <topic>=<definition>`:

`--log.output queries=file://queries.log`

The above example logs query-related messages to the file `queries.log`.

You can specify the option multiple times in order to configure the output
for different log topics:

`--log.level queries=trace --log.output queries=file:///queries.log
--log.level requests=info --log.output requests=file:///requests.log`

The above example logs all query-related messages to the file `queries.log`
and HTTP requests with a level of `info` or higher to the file `requests.log`.

Any occurrence of `$PID` in the log output value is replaced at runtime with
the actual process ID. This enables logging to process-specific files:

`--log.output 'file://serened.log.$PID'`

Note that dollar sign may need extra escaping when specified on a
command-line such as Bash.

If you specify `--log.file-mode <octalvalue>`, then any newly created log
file uses `octalvalue` as file mode. Please note that the `umask` value is
applied as well.

If you specify `--log.file-group <name>`, then any newly created log file tries
to use `<name>` as the group name. Note that you have to be a member of that
group. Otherwise, the group ownership is not changed.

The old `--log.file` option is still available for convenience. It is a
shortcut for the more general option `--log.output file://filename`.

The old `--log.requests-file` option is still available. It is a shortcut for
the more general option `--log.output requests=file://...`.)");

  auto topics = log::GetTopics() | std::views::transform([](const auto* topic) {
                  return topic->GetName();
                });
  std::string topics_joined = absl::StrJoin(topics, ", ");

  options
    ->addOption("--log.level,-l",
                "Set the topic-specific log level, using `--log.level level` "
                "for the general topic or `--log.level topic=level` for the "
                "specified topic (can be specified multiple times).\n"
                "Available log levels: fatal, error, warning, info, debug, "
                "trace.\n"
                "Available log topics: all, " +
                  topics_joined + ".",
                new VectorParameter<StringParameter>(&_levels))
    .setLongDescription(R"(SereneDB's log output is grouped by topics.
`--log.level` can be specified multiple times at startup, for as many topics as
needed. The log verbosity and output files can be adjusted per log topic.

```
serened --log.level all=warning --log.level queries=trace --log.level startup=trace
```

This sets a global log level of `warning` and two topic-specific levels
(`trace` for queries and `info` for startup). Note that `--log.level warning`
does not set a log level globally for all existing topics, but only the
`general` topic. Use the pseudo-topic `all` to set a global log level.

The same in a configuration file:

```
[log]
level = all=warning
level = queries=trace
level = startup=trace
```

The available log levels are:

- `fatal`: Only log fatal errors.
- `error`: Only log errors.
- `warning`: Only log warnings and errors.
- `info`: Log information messages, warnings, and errors.
- `debug`: Log debug and information messages, warnings, and errors.
- `trace`: Logs trace, debug, and information messages, warnings, and errors.

Note that the `debug` and `trace` levels are very verbose.

Some relevant log topics available in SereneDB are:

- `agency`: Information about the cluster Agency.
- `performance`: Performance-related messages.
- `queries`: Executed AQL queries, slow queries.
- `replication`: Replication-related information.
- `requests`: HTTP requests.
- `startup`: Information about server startup and shutdown.
- `threads`: Information about threads.

You can adjust the log levels at runtime via the `PUT /_admin/log/level`
HTTP API endpoint.)");

  options
    ->addOption("--log.max-entry-length",
                "The maximum length of a log entry (in bytes).",
                new UInt32Parameter(&_max_entry_length))
    .setLongDescription(R"(Any log messages longer than the specified value
are truncated and the suffix `...` is added to them.

The purpose of this option is to shorten long log messages in case there is not
a lot of space for log files, and to keep rogue log messages from overusing
resources.

The default value is 128 MB, which is very high and should effectively mean
downwards-compatibility with previous serened versions, which did not restrict
the maximum size of log messages.)");

  options
    ->addOption(
      "--log.time-format", "The time format to use in logs.",
      new DiscreteValuesParameter<StringParameter>(
        &_time_format_string, log_time_formats::GetAvailableFormatNames()))
    .setLongDescription(R"(Overview over the different options:

Format                  | Example                  | Description
:-----------------------|:------------------------ |:-----------
`timestamp`             | 1553766923000            | Unix timestamps, in seconds
`timestamp-millis`      | 1553766923000.123        | Unix timestamps, in seconds, with millisecond precision
`timestamp-micros`      | 1553766923000.123456     | Unix timestamps, in seconds, with microsecond precision
`uptime`                | 987654                   | seconds since server start
`uptime-millis`         | 987654.123               | seconds since server start, with millisecond precision
`uptime-micros`         | 987654.123456            | seconds since server start, with microsecond precision
`utc-datestring`        | 2019-03-28T09:55:23Z     | UTC-based date and time in format YYYY-MM-DDTHH:MM:SSZ
`utc-datestring-millis` | 2019-03-28T09:55:23.123Z | like `utc-datestring`, but with millisecond precision
`local-datestring`      | 2019-03-28T10:55:23      | local date and time in format YYYY-MM-DDTHH:MM:SS)");

  options
    ->addOption("--log.ids", "Log unique message IDs.",
                new BooleanParameter(&_show_ids))
    .setLongDescription(R"(Each log invocation in the SereneDB source code
contains a unique log ID, which can be used to quickly find the location in the
source code that produced a specific log message.

Log IDs are printed as 5-digit hexadecimal identifiers in square brackets
between the log level and the log topic:

`2020-06-22T21:16:48Z [39028] INFO [144fe] {general} using storage engine
'rocksdb'` (where `144fe` is the log ID).)");

  options
    ->addOption("--log.role", "Log the server role.",
                new BooleanParameter(&_show_role))
    .setLongDescription(R"(If you set this option to `true`, log messages
contains a single character with the server's role. The roles are:

- `U`: Undefined / unclear (used at startup)
- `S`: Single server
- `C`: Coordinator
- `P`: Primary / DB-Server
- `A`: Agent)");

  options->addOption(
    "--log.file-mode",
    "mode to use for new log file, umask will be applied as well",
    new StringParameter(&_file_mode));

  if (_threaded) {
    // this option only makes sense for serened, not for serenesh etc.
    options
      ->addOption("--log.api-enabled",
                  "Whether the log API is enabled (true) or not (false), or "
                  "only enabled for superuser JWT (jwt).",
                  new StringParameter(&_api_switch))
      .setLongDescription(R"(Credentials are not written to log files.
Nevertheless, some logged data might be sensitive depending on the context of
the deployment. For example, if request logging is switched on, user requests
and corresponding data might end up in log files. Therefore, a certain care
with log files is recommended.

Since the database server offers an API to control logging and query logging
data, this API has to be secured properly. By default, the API is accessible
for admin users (administrative access to the `_system` database). However,
you can lock this down further.

The possible values for this option are:

 - `true`: The `/_admin/log` API is accessible for admin users.
 - `jwt`: The `/_admin/log` API is accessible for the superuser only
   (authentication with JWT token and empty username).
 - `false`: The `/_admin/log` API is not accessible at all.)");
  }

  options
    ->addOption("--log.use-json-format",
                "Use JSON as output format for logging.",
                new BooleanParameter(&_use_json))

    .setLongDescription(R"(You can use this option to switch the log output
to the JSON format. Each log message then produces a separate line with
JSON-encoded log data, which can be consumed by other applications.

The object attributes produced for each log message are:

| Key        | Value      |
|:-----------|:-----------|
| `time`     | date/time of log message, in format specified by `--log.time-format`
| `prefix`   | only emitted if `--log.prefix` is set
| `pid`      | process id, only emitted if `--log.process` is set
| `tid`      | thread id, only emitted if `--log.thread` is set
| `thread`   | thread name, only emitted if `--log.thread-name` is set
| `role`     | server role (1 character), only emitted if `--log.role` is set
| `level`    | log level (e.g. `"WARN"`, `"INFO"`)
| `file`     | source file name of log message, only emitted if `--log.line-number` is set
| `line`     | source file line of log message, only emitted if `--log.line-number` is set
| `function` | source file function name, only emitted if `--log.line-number` is set
| `topic`    | log topic name
| `id`       | log id (5 digit hexadecimal string), only emitted if `--log.ids` is set
| `hostname` | hostname if `--log.hostname` is set
| `message`  | the actual log message payload)");

#ifdef SERENEDB_HAVE_SETGID
  options->addOption(
    "--log.file-group",
    "group to use for new log file, user must be a member of this group",
    new StringParameter(&_file_group));
#endif

  options
    ->addOption("--log.prefix", "Prefix log message with this string.",
                new StringParameter(&_prefix),
                options::MakeDefaultFlags(options::Flags::Uncommon))
    .setLongDescription(R"(Example: `serened... --log.prefix "-->"`

`2020-07-23T09:46:03Z --> [17493] INFO ...`)");

  options->addOption("--log.file",
                     "shortcut for '--log.output file://<filename>'",
                     new StringParameter(&_file),
                     options::MakeDefaultFlags(options::Flags::Uncommon));

  options->addOption(
    "--log.line-number",
    "Include the function name, file name, and line number of the source "
    "code that issues the log message. Format: `[func@FileName.cpp:123]`",
    new BooleanParameter(&_line_number),
    options::MakeDefaultFlags(options::Flags::Uncommon));

  options->addOption(
    "--log.shorten-filenames",
    "shorten filenames in log output (use with --log.line-number)",
    new BooleanParameter(&_shorten_filenames),
    options::MakeDefaultFlags(options::Flags::Uncommon));

  options
    ->addOption("--log.hostname",
                "The hostname to use in log message. Leave empty for none, "
                "use \"auto\" to automatically determine a hostname.",
                new StringParameter(&_hostname))

    .setLongDescription(R"(You can specify a hostname to be logged at the
beginning of each log message (for regular logging) or inside the `hostname`
attribute (for JSON-based logging).

The default value is an empty string, meaning no hostnames is logged.
If you set this option to `auto`, the hostname is automatically determined.)");

  options->addOption("--log.process",
                     "Show the process identifier (PID) in log messages.",
                     new BooleanParameter(&_process_id),
                     options::MakeDefaultFlags(options::Flags::Uncommon));

  options->addOption("--log.thread",
                     "Show the thread identifier in log messages.",
                     new BooleanParameter(&_thread_id),
                     options::MakeDefaultFlags(options::Flags::Uncommon));

  options->addOption("--log.thread-name", "Show thread name in log messages.",
                     new BooleanParameter(&_thread_name),
                     options::MakeDefaultFlags(options::Flags::Uncommon));

  if (_threaded) {
    // this option only makes sense for serened, not for serenesh etc.
    options->addOption("--log.keep-logrotate",
                       "Keep the old log file after receiving a SIGHUP.",
                       new BooleanParameter(&_keep_log_rotate),
                       options::MakeDefaultFlags(options::Flags::Uncommon));
  }

  options->addOption("--log.foreground-tty", "Also log to TTY if backgrounded.",
                     new BooleanParameter(&_foreground_tty),
                     options::MakeDefaultFlags(options::Flags::Uncommon,
                                               options::Flags::Dynamic));

  options
    ->addOption("--log.force-direct",
                "Do not start a separate thread for logging.",
                new BooleanParameter(&_force_direct),
                options::MakeDefaultFlags(options::Flags::Uncommon))
    .setLongDescription(R"(You can use this option to disable logging in an
extra logging thread. If set to `true`, any log messages are immediately
printed in the thread that triggered the log message. This is non-optimal for
performance but can aid debugging. If set to `false`, log messages are handed
off to an extra logging thread, which asynchronously writes the log messages.)");

  options
    ->addOption(
      "--log.max-queued-entries",
      "Upper limit of log entries that are queued in a background thread.",
      new UInt32Parameter(&_max_queued_log_messages),
      options::MakeDefaultFlags(options::Flags::Uncommon))

    .setLongDescription(R"(Log entries are pushed on a queue for asynchronous
writing unless you enable the `--log.force-direct` startup option. If you use a
slow log output (e.g. syslog), the queue might grow and eventually overflow.

You can configure the upper bound of the queue with this option. If the queue is
full, log entries are written synchronously until the queue has space again.)");

  options->addOption(
    "--log.request-parameters",
    "include full URLs and HTTP request parameters in trace logs",
    new BooleanParameter(&_log_request_parameters),
    options::MakeDefaultFlags(options::Flags::Uncommon));

  // for debugging purpose, we set the log levels NOW
  // this might be overwritten latter
  log::SetLogLevels(_levels);
}

void LoggerFeature::validateOptions(std::shared_ptr<ProgramOptions> options) {
  if (options->processingResult().touched("log.file")) {
    std::string definition;

    if (_file == "+" || _file == "-") {
      definition = _file;
    } else {
      definition = "file://" + _file;
    }

    _output.push_back(definition);
  }

  if (options->processingResult().touched("log.time-format") &&
      (options->processingResult().touched("log.use-microtime") ||
       options->processingResult().touched("log.use-local-time"))) {
    SDB_FATAL("xxxxx", Logger::FIXME,
              "cannot combine `--log.time-format` with either "
              "`--log.use-microtime` or `--log.use-local-time`");
  }

  // convert the deprecated options into the new timeformat
  if (options->processingResult().touched("log.use-local-time")) {
    _time_format_string = "local-datestring";
    // the following call ensures the string is actually valid.
    // if not valid, the following call will throw an exception and
    // abort the startup
    log_time_formats::FormatFromName(_time_format_string);
  } else if (options->processingResult().touched("log.use-microtime")) {
    _time_format_string = "timestamp-micros";
    // the following call ensures the string is actually valid.
    // if not valid, the following call will throw an exception and
    // abort the startup
    log_time_formats::FormatFromName(_time_format_string);
  }

  if (_api_switch == "true" || _api_switch == "on" || _api_switch == "On") {
    _api_enabled = true;
    _api_switch = "true";
  } else if (_api_switch == "jwt" || _api_switch == "JWT") {
    _api_enabled = true;
    _api_switch = "jwt";
  } else {
    _api_enabled = false;
    _api_switch = "false";
  }

  if (!_file_mode.empty()) {
    try {
      int result = std::stoi(_file_mode, nullptr, 8);
      log::AppenderFileFactory::setFileMode(result);
    } catch (...) {
      SDB_FATAL("xxxxx", Logger::FIXME,
                "expecting an octal number for log.file-mode, got '",
                _file_mode, "'");
    }
  }

#ifdef SERENEDB_HAVE_SETGID
  if (!_file_group.empty()) {
    bool valid = false;
    int gid_number = number_utils::AtoiPositive<int>(
      _file_group.data(), _file_group.data() + _file_group.size(), valid);

    if (valid && gid_number >= 0) {
#ifdef SERENEDB_HAVE_GETGRGID
      auto gid = basics::file_utils::FindGroup(_file_group);
      if (!gid) {
        SDB_FATAL("xxxxx", Logger::FIXME, "unknown numeric gid '", _file_group,
                  "'");
      }
#endif
    } else {
#ifdef SERENEDB_HAVE_GETGRNAM
      auto gid = basics::file_utils::FindGroup(_file_group);
      if (gid) {
        gid_number = gid.value();
      } else {
        SetError(ERROR_SYS_ERROR);
        SDB_FATAL("xxxxx", Logger::FIXME, "cannot convert groupname '",
                  _file_group, "' to numeric gid: ", LastError());
      }
#else
      SDB_FATAL("xxxxx", Logger::FIXME, "cannot convert groupname '",
                _file_group, "' to numeric gid");
#endif
    }

    log::AppenderFileFactory::setFileGroup(gid_number);
  }
#endif

  // replace $PID with current process id in filenames
  for (auto& output : _output) {
    output = basics::string_utils::Replace(
      output, "$PID", std::to_string(Thread::currentProcessId()));
  }
}

void LoggerFeature::prepare() {
  // set maximum length for each log entry
  log::GetDefaultLogGroup().maxLogEntryLength(
    std::max<uint32_t>(256, _max_entry_length));

  log::SetLogLevels(_levels);
  log::SetShowIds(_show_ids);
  log::SetShowRole(_show_role);
  log::SetUseColor(_use_color);
  log::SetTimeFormat(log_time_formats::FormatFromName(_time_format_string));
  log::SetUseControlEscaped(_use_control_escaped);
  log::SetUseUnicodeEscaped(_use_unicode_escaped);
  log::SetEscaping();
  log::SetShowLineNumber(_line_number);
  log::SetShortenFilenames(_shorten_filenames);
  log::SetShowProcessIdentifier(_process_id);
  log::SetShowThreadIdentifier(_thread_id);
  log::SetShowThreadName(_thread_name);
  log::SetOutputPrefix(_prefix);
  log::SetHostname(_hostname);
  log::SetKeepLogrotate(_keep_log_rotate);
  log::SetLogRequestParameters(_log_request_parameters);
  log::SetUseJson(_use_json);

  for (const auto& definition : _output) {
    if (_supervisor && definition.starts_with("file://")) {
      log::Appender::addAppender(log::GetDefaultLogGroup(),
                                 absl::StrCat(definition, ".supervisor"));
    } else {
      log::Appender::addAppender(log::GetDefaultLogGroup(), definition);
    }
  }

  if (_foreground_tty) {
    log::Appender::addAppender(log::GetDefaultLogGroup(), "-");
  }

  if (_force_direct || _supervisor || !_threaded) {
    log::Initialize();
  } else {
    log::InitializeAsync(server(), _max_queued_log_messages);
  }
}

void LoggerFeature::unprepare() { log::Flush(); }

}  // namespace sdb
