#include "duckdb.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"

#include <clickhouse/client.h>

#include "clickhouse_storage.hpp"
#include "clickhouse_connection.hpp"
#include "clickhouse_secrets.hpp"
#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
	if (secret_entry) {
		return secret_entry;
	}
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}
	return nullptr;
}

static unique_ptr<SecretEntry> GetSecretEntry(ClientContext &context, const string &secret_name) {
	string name = secret_name;
	bool explicit_secret = !name.empty();
	if (!explicit_secret) {
		name = "clickhouse";
	}
	auto secret_entry = GetSecret(context, name);
	if (!secret_entry && explicit_secret) {
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}
	return secret_entry;
}

static void OverlaySecretParams(optional_ptr<SecretEntry> secret_entry, ClickHouseConnectionParams &params) {
	if (!secret_entry) {
		return;
	}
	const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	Value uri_val = kv_secret.TryGetValue("uri");
	if (!uri_val.IsNull()) {
		params = ClickHouseConnectionParams::FromConnectionString(uri_val.ToString());
		return;
	}

	Value host_val = kv_secret.TryGetValue("host");
	if (!host_val.IsNull()) {
		params.host = host_val.ToString();
	}
	Value port_val = kv_secret.TryGetValue("port");
	if (!port_val.IsNull()) {
		params.port = NumericCast<uint16_t>(std::stoul(port_val.ToString()));
	}
	Value user_val = kv_secret.TryGetValue("user");
	if (!user_val.IsNull()) {
		params.user = user_val.ToString();
	}
	Value password_val = kv_secret.TryGetValue("password");
	if (!password_val.IsNull()) {
		params.password = password_val.ToString();
	}
	Value database_val = kv_secret.TryGetValue("database");
	if (!database_val.IsNull()) {
		params.database = database_val.ToString();
	}
	Value secure_val = kv_secret.TryGetValue("secure");
	if (!secure_val.IsNull()) {
		auto lvalue = StringUtil::Lower(secure_val.ToString());
		params.secure = lvalue == "true" || lvalue == "1" || lvalue == "yes" || lvalue == "on";
	}
}

// Quote a value so it round-trips through FromConnectionString even when it
// contains a space/quote/backslash or is empty (single-quote + backslash-escape,
// matching the parser); plain values are emitted unquoted.
static string QuoteValue(const string &v) {
	// Also quote any value containing "://" so it can never be mistaken for a URI
	// scheme separator when the whole connstr is re-parsed.
	bool needs = v.empty() || v.find("://") != string::npos;
	for (char c : v) {
		if (c == ' ' || c == '\t' || c == '\n' || c == '\'' || c == '\\' || c == '=') {
			needs = true;
			break;
		}
	}
	if (!needs) {
		return v;
	}
	string out = "'";
	for (char c : v) {
		if (c == '\'' || c == '\\') {
			out += '\\';
		}
		out += c;
	}
	out += "'";
	return out;
}

static string BuildConnectionString(const ClickHouseConnectionParams &params) {
	string result;
	result += "host=" + QuoteValue(params.host);
	result += " port=" + to_string(params.port);
	result += " user=" + QuoteValue(params.user);
	result += " password=" + QuoteValue(params.password);
	result += " database=" + QuoteValue(params.database);
	result += string(" secure=") + (params.secure ? "true" : "false");
	// Round-trip the compression method too; otherwise it is lost here and the
	// catalog connection silently falls back to the default (LZ4), so an
	// ATTACH ... (compression=none) had no effect.
	const char *compression = "lz4";
	switch (params.compression) {
	case clickhouse::CompressionMethod::None:
		compression = "none";
		break;
	case clickhouse::CompressionMethod::ZSTD:
		compression = "zstd";
		break;
	default:
		compression = "lz4";
		break;
	}
	result += string(" compression=") + compression;
	return result;
}

static unique_ptr<Catalog> ClickHouseAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &attach_options) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Attaching ClickHouse databases is disabled through configuration");
	}

	string secret_name;
	string database_override;
	bool database_overridden = false;
	bool secure_override = false;
	bool secure_overridden = false;
	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else if (lower_name == "database" || lower_name == "dbname") {
			database_override = entry.second.ToString();
			database_overridden = true;
		} else if (lower_name == "secure") {
			secure_override = entry.second.GetValue<bool>();
			secure_overridden = true;
		} else {
			throw BinderException("Unrecognized option for ClickHouse attach: %s", entry.first);
		}
	}

	auto secret_entry = GetSecretEntry(context, secret_name);

	ClickHouseConnectionParams params;
	OverlaySecretParams(secret_entry, params);

	if (!info.path.empty()) {
		if (info.path.find("://") != string::npos) {
			params = ClickHouseConnectionParams::FromConnectionString(info.path);
		} else {
			auto path_params = ClickHouseConnectionParams::FromConnectionString(info.path);
			for (auto &token : StringUtil::Split(info.path, ' ')) {
				auto trimmed = token;
				StringUtil::Trim(trimmed);
				if (trimmed.empty()) {
					continue;
				}
				auto eq_pos = trimmed.find('=');
				auto lkey = StringUtil::Lower(trimmed.substr(0, eq_pos));
				StringUtil::Trim(lkey);
				if (lkey == "host" || lkey == "hostname") {
					params.host = path_params.host;
				} else if (lkey == "port") {
					params.port = path_params.port;
				} else if (lkey == "user" || lkey == "username") {
					params.user = path_params.user;
				} else if (lkey == "password" || lkey == "passwd") {
					params.password = path_params.password;
				} else if (lkey == "database" || lkey == "dbname" || lkey == "db") {
					params.database = path_params.database;
				} else if (lkey == "secure" || lkey == "ssl") {
					params.secure = path_params.secure;
				} else if (lkey == "compression") {
					params.compression = path_params.compression;
				}
			}
		}
	}

	if (database_overridden) {
		params.database = database_override;
	}
	if (secure_overridden) {
		params.secure = secure_override;
	}
	// TLS uses the secure native port (9440); bump only when the port is still
	// the plaintext default so an explicitly chosen port is always respected.
	if (params.secure && params.port == 9000) {
		params.port = 9440;
	}

	try {
		auto conn = ClickHouseConnection::Open(params);
		ClickHouseConnection::LogQuery("SELECT version()");
		conn.GetClient().Execute("SELECT version()");
	} catch (const std::exception &e) {
		throw IOException("Failed to connect to ClickHouse at %s:%d: %s", params.host, (int)params.port, e.what());
	}

	return make_uniq<ClickHouseCatalog>(db, BuildConnectionString(params), attach_options.access_mode);
}

static unique_ptr<TransactionManager>
ClickHouseCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                   Catalog &catalog) {
	auto &clickhouse_catalog = catalog.Cast<ClickHouseCatalog>();
	return make_uniq<ClickHouseTransactionManager>(db, clickhouse_catalog);
}

ClickHouseStorageExtension::ClickHouseStorageExtension() {
	attach = ClickHouseAttach;
	create_transaction_manager = ClickHouseCreateTransactionManager;
}

} // namespace duckdb
