#include "duckdb.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/printer.hpp"

#include <clickhouse/client.h>
#include <clickhouse/exceptions.h>

#include <cctype>

#include "clickhouse_connection.hpp"

namespace duckdb {

static void ApplyParam(ClickHouseConnectionParams &result, const string &key, const string &value) {
	auto lkey = StringUtil::Lower(key);
	if (lkey == "host" || lkey == "hostname") {
		result.host = value;
	} else if (lkey == "port") {
		result.port = NumericCast<uint16_t>(std::stoul(value));
	} else if (lkey == "user" || lkey == "username") {
		result.user = value;
	} else if (lkey == "password" || lkey == "passwd") {
		result.password = value;
	} else if (lkey == "database" || lkey == "dbname" || lkey == "db") {
		result.database = value;
	} else if (lkey == "secure" || lkey == "ssl") {
		auto lvalue = StringUtil::Lower(value);
		result.secure = lvalue == "true" || lvalue == "1" || lvalue == "yes" || lvalue == "on";
	} else if (lkey == "compression") {
		auto lvalue = StringUtil::Lower(value);
		if (lvalue == "none") {
			result.compression = clickhouse::CompressionMethod::None;
		} else if (lvalue == "zstd") {
			result.compression = clickhouse::CompressionMethod::ZSTD;
		} else if (lvalue == "lz4") {
			result.compression = clickhouse::CompressionMethod::LZ4;
		} else {
			throw InvalidInputException("Unknown ClickHouse compression method \"%s\"", value);
		}
	} else {
		throw InvalidInputException("Unrecognized ClickHouse connection option \"%s\"", key);
	}
}

static ClickHouseConnectionParams ParseURI(const string &connection_string) {
	ClickHouseConnectionParams result;

	string rest = connection_string;
	auto scheme_end = rest.find("://");
	auto scheme = StringUtil::Lower(rest.substr(0, scheme_end));
	if (scheme == "clickhouses" || scheme == "https") {
		result.secure = true;
	}
	rest = rest.substr(scheme_end + 3);

	// Split the authority (userinfo@host:port) from the trailing path/query: the
	// authority ends at the first '/' or '?'. Isolating it BEFORE removing
	// userinfo means a '/' or '?' inside a user/password is never mistaken for
	// the path separator.
	string tail;
	auto authority_end = rest.find_first_of("/?");
	if (authority_end != string::npos) {
		tail = rest.substr(authority_end);
		rest = rest.substr(0, authority_end);
	}

	// optional userinfo: split from host on the LAST '@' (a password may itself
	// contain '@'); within the userinfo the FIRST ':' splits user from password.
	auto at_pos = rest.rfind('@');
	if (at_pos != string::npos) {
		auto userinfo = rest.substr(0, at_pos);
		rest = rest.substr(at_pos + 1);
		auto colon_pos = userinfo.find(':');
		if (colon_pos != string::npos) {
			result.user = userinfo.substr(0, colon_pos);
			result.password = userinfo.substr(colon_pos + 1);
		} else {
			result.user = userinfo;
		}
	}

	// host[:port], IPv6-aware. Forms: "host", "host:port", "[ipv6]", "[ipv6]:port",
	// and a bare "ipv6" literal (e.g. ::1) which contains multiple colons and has
	// no port. A naive rfind(':') would split "::1" into host "::" + port "1".
	bool explicit_port = false;
	if (!rest.empty() && rest.front() == '[') {
		auto close = rest.find(']');
		if (close == string::npos) {
			result.host = rest;
		} else {
			result.host = rest.substr(1, close - 1);
			auto after = rest.substr(close + 1);
			if (!after.empty() && after.front() == ':') {
				result.port = NumericCast<uint16_t>(std::stoul(after.substr(1)));
				explicit_port = true;
			}
		}
	} else {
		size_t ncolon = 0;
		for (char c : rest) {
			if (c == ':') {
				ncolon++;
			}
		}
		if (ncolon > 1) {
			// bare IPv6 literal without brackets or port (e.g. ::1)
			result.host = rest;
		} else if (ncolon == 1) {
			auto colon_pos = rest.rfind(':');
			result.host = rest.substr(0, colon_pos);
			result.port = NumericCast<uint16_t>(std::stoul(rest.substr(colon_pos + 1)));
			explicit_port = true;
		} else {
			result.host = rest;
		}
	}

	// path (database) and query (?key=value&...) options, parsed from the tail
	// split off the authority above.
	if (!tail.empty()) {
		string path = tail, query;
		auto qpos = tail.find('?');
		if (qpos != string::npos) {
			path = tail.substr(0, qpos);
			query = tail.substr(qpos + 1);
		}
		if (path.size() > 1 && path.front() == '/') {
			result.database = path.substr(1);
		}
		while (!query.empty()) {
			auto amp = query.find('&');
			auto kv = query.substr(0, amp);
			auto eq = kv.find('=');
			if (eq != string::npos) {
				auto qkey = kv.substr(0, eq);
				ApplyParam(result, qkey, kv.substr(eq + 1));
				if (StringUtil::Lower(qkey) == "port") {
					explicit_port = true;
				}
			}
			if (amp == string::npos) {
				break;
			}
			query = query.substr(amp + 1);
		}
	}

	if (result.secure && !explicit_port) {
		result.port = 9440;
	}
	return result;
}

ClickHouseConnectionParams ClickHouseConnectionParams::FromConnectionString(const string &connection_string) {
	// Treat the input as a URI only when "://" is a real scheme separator at the
	// start -- before the first '=' and the first whitespace. A key=value DSN
	// whose VALUE legitimately contains "://" (e.g. password='http://tok@x') must
	// not be misrouted to the URI parser.
	{
		auto scheme_sep = connection_string.find("://");
		auto eq = connection_string.find('=');
		auto ws = connection_string.find_first_of(" \t");
		bool is_uri = scheme_sep != string::npos && (eq == string::npos || scheme_sep < eq) &&
		              (ws == string::npos || scheme_sep < ws);
		if (is_uri) {
			return ParseURI(connection_string);
		}
	}

	ClickHouseConnectionParams result;
	bool explicit_port = false;

	// Parse a space-delimited "key=value" connection string. A value may be
	// single-quoted to contain spaces (e.g. password='p w'); inside the quotes
	// a literal quote/backslash is backslash-escaped (libpq DSN rules). Bare
	// (unquoted) values parse exactly as before.
	const auto &s = connection_string;
	size_t i = 0;
	while (i < s.size()) {
		while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) {
			i++;
		}
		if (i >= s.size()) {
			break;
		}
		size_t key_start = i;
		while (i < s.size() && s[i] != '=' && !std::isspace(static_cast<unsigned char>(s[i]))) {
			i++;
		}
		if (i >= s.size() || s[i] != '=') {
			throw InvalidInputException("Invalid ClickHouse connection option \"%s\": expected key=value",
			                            s.substr(key_start, i - key_start));
		}
		string key = s.substr(key_start, i - key_start);
		i++; // skip '='
		string value;
		if (i < s.size() && s[i] == '\'') {
			i++; // opening quote
			while (i < s.size() && s[i] != '\'') {
				if (s[i] == '\\' && i + 1 < s.size()) {
					i++;
				}
				value += s[i++];
			}
			if (i >= s.size()) {
				throw InvalidInputException("Invalid ClickHouse connection string: unterminated quoted value for \"%s\"",
				                            key);
			}
			i++; // closing quote
		} else {
			while (i < s.size() && !std::isspace(static_cast<unsigned char>(s[i]))) {
				value += s[i++];
			}
		}
		if (StringUtil::Lower(key) == "port") {
			explicit_port = true;
		}
		ApplyParam(result, key, value);
	}

	if (result.secure && !explicit_port) {
		result.port = 9440;
	}
	return result;
}

clickhouse::ClientOptions ClickHouseConnectionParams::ToClientOptions() const {
	clickhouse::ClientOptions options;
	options.SetHost(host)
	    .SetPort(port)
	    .SetUser(user)
	    .SetPassword(password)
	    .SetDefaultDatabase(database)
	    .SetCompressionMethod(compression);
	if (secure) {
		options.SetSSLOptions(clickhouse::ClientOptions::SSLOptions {});
	}
	return options;
}

ClickHouseConnection::ClickHouseConnection(std::unique_ptr<clickhouse::Client> client_p)
    : client(std::move(client_p)) {
}

ClickHouseConnection ClickHouseConnection::Open(const ClickHouseConnectionParams &params) {
	try {
		return ClickHouseConnection(std::make_unique<clickhouse::Client>(params.ToClientOptions()));
	} catch (const std::exception &e) {
		throw IOException("Failed to connect to ClickHouse server at %s:%d: %s", params.host, (int)params.port,
		                  e.what());
	}
}

clickhouse::Client &ClickHouseConnection::GetClient() {
	if (!client) {
		throw IOException("ClickHouse connection is not open");
	}
	return *client;
}

static bool debug_clickhouse_print_queries = false;

void ClickHouseConnection::DebugSetPrintQueries(bool print) {
	debug_clickhouse_print_queries = print;
}

clickhouse::Query ClickHouseConnection::MakeQuery(duckdb::ClientContext &context, const std::string &sql) {
	clickhouse::Query query(sql);
	Value timeout_val;
	if (context.TryGetCurrentSetting("ch_statement_timeout_millis", timeout_val) && !timeout_val.IsNull()) {
		auto ms = timeout_val.GetValue<uint64_t>();
		if (ms > 0) {
			// ClickHouse's max_execution_time is in (fractional) seconds; a per-query
			// SETTINGS keeps it off the pooled connection (no sticky session GUC).
			double seconds = static_cast<double>(ms) / 1000.0;
			query.SetSetting("max_execution_time", clickhouse::QuerySettingsField{std::to_string(seconds), 0});
		}
	}
	return query;
}

void ClickHouseConnection::LogQuery(const std::string &sql) {
	if (debug_clickhouse_print_queries) {
		Printer::Print(sql + "\n");
	}
}

void ClickHouseConnection::ThrowError(const char *op, const std::string &sql, const std::exception &error) {
	throw IOException("ClickHouse error %s: %s\nSQL: %s", op, error.what(), sql);
}

static bool clickhouse_connection_cache_enabled = true;

void ClickHouseConnection::SetConnectionCache(bool enabled) {
	clickhouse_connection_cache_enabled = enabled;
}

bool ClickHouseConnection::ConnectionCacheEnabled() {
	return clickhouse_connection_cache_enabled;
}

} // namespace duckdb
