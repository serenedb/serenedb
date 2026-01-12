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

#include "ssl_server_feature.h"

#include <nghttp2/nghttp2.h>
#include <openssl/asn1.h>
#include <openssl/bio.h>
#include <openssl/ec.h>
#include <openssl/objects.h>
#include <openssl/opensslv.h>
#include <openssl/ossl_typ.h>
#include <openssl/safestack.h>
#include <openssl/ssl3.h>
#include <openssl/x509.h>

#include <boost/asio/ssl/context_base.hpp>
#include <boost/asio/ssl/impl/context.ipp>
#include <exception>
#include <ranges>
#include <stdexcept>
#include <vector>

#include "app/app_server.h"
#include "app/options/option.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/application-exit.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/log_level.h"
#include "basics/logger/logger.h"
#include "basics/random/uniform_character.h"
#include "basics/ssl/ssl_helper.h"
#include "general_server/general_server.h"

using namespace sdb;
using namespace sdb::basics;
using namespace sdb::options;

SslServerFeature::SslServerFeature(Server& server)
  : SerenedFeature{server, name()},
    _cafile(),
    _keyfile(),
    _cipher_list("HIGH:!EXPORT:!aNULL@STRENGTH"),
    _ssl_protocol(kTlsGeneric),
    _ssl_options(asio_ns::ssl::context::default_workarounds |
                 asio_ns::ssl::context::single_dh_use),
    _ecdh_curve("prime256v1"),
    _session_cache(false),
    _prefer_http11_in_alpn(false) {
  setOptional(true);
}

void SslServerFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
  options->addSection("ssl", "SSL communication");

  options
    ->addOption("--ssl.cafile", "The CA file used for secure connections.",
                new StringParameter(&_cafile))
    .setLongDescription(R"(You can use this option to specify a file with
CA certificates that are sent to the client whenever the server requests a
client certificate. If you specify a file, the server only accepts client
requests with certificates issued by these CAs. Do not specify this option if
you want clients to be able to connect without specific certificates.

The certificates in the file must be PEM-formatted.)");

  options
    ->addOption("--ssl.keyfile", "The keyfile used for secure connections.",
                new StringParameter(&_keyfile))
    .setLongDescription(R"(If you use SSL-encryption by binding the server to
an SSL endpoint (e.g. `--server.endpoint ssl://127.0.0.1:8529`), you must use
this option to specify the filename of the server's private key. The file must
be PEM-formatted and contain both, the certificate and the server's private key.

You can generate a keyfile using OpenSSL as follows:

```bash
# create private key in file "server.key"
openssl genpkey -out server.key -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -aes-128-cbc

# create certificate signing request (csr) in file "server.csr"
openssl req -new -key server.key -out server.csr

# copy away original private key to "server.key.org"
cp server.key server.key.org

# remove passphrase from the private key
openssl rsa -in server.key.org -out server.key

# sign the csr with the key, creates certificate PEM file "server.crt"
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt

# combine certificate and key into single PEM file "server.pem"
cat server.crt server.key > server.pem
```

You may use certificates issued by a Certificate Authority or self-signed
certificates. Self-signed certificates can be created by a tool of your
choice. When using OpenSSL for creating the self-signed certificate, the
following commands should create a valid keyfile:

```
-----BEGIN CERTIFICATE-----

(base64 encoded certificate)

-----END CERTIFICATE-----
-----BEGIN RSA PRIVATE KEY-----

(base64 encoded private key)

-----END RSA PRIVATE KEY-----
```

For further information please check the manuals of the tools you use to create
the certificate.)");

  options->addOption("--ssl.session-cache",
                     "Enable the session cache for connections.",
                     new BooleanParameter(&_session_cache));

  options
    ->addOption("--ssl.cipher-list",
                "The SSL ciphers to use. See the OpenSSL documentation.",
                new StringParameter(&_cipher_list))
    .setLongDescription(R"(You can use this option to restrict the server to
certain SSL ciphers only, and to define the relative usage preference of SSL
ciphers.

The format of the option's value is documented in the OpenSSL documentation.

To check which ciphers are available on your platform, you may use the
following shell command:

```bash
> openssl ciphers -v

ECDHE-RSA-AES256-SHA    SSLv3 Kx=ECDH     Au=RSA  Enc=AES(256)  Mac=SHA1
ECDHE-ECDSA-AES256-SHA  SSLv3 Kx=ECDH     Au=ECDSA Enc=AES(256)  Mac=SHA1
DHE-RSA-AES256-SHA      SSLv3 Kx=DH       Au=RSA  Enc=AES(256)  Mac=SHA1
DHE-DSS-AES256-SHA      SSLv3 Kx=DH       Au=DSS  Enc=AES(256)  Mac=SHA1
DHE-RSA-CAMELLIA256-SHA SSLv3 Kx=DH       Au=RSA  Enc=Camellia(256)
Mac=SHA1
...
```)");

  options
    ->addOption("--ssl.protocol", AvailableSslProtocolsDescription(),
                new DiscreteValuesParameter<UInt64Parameter>(
                  &_ssl_protocol, AvailableSslProtocols()))
    .setLongDescription(R"(Use this option to specify the default encryption
protocol to be used. The default value is 9 (generic TLS), which allows the
negotiation of the TLS version between the client and the server, dynamically
choosing the highest mutually supported version of TLS.

Note that SSLv2 is unsupported as of version 3.4, because of the inherent
security vulnerabilities in this protocol. Selecting SSLv2 as protocol aborts
the startup.)");

  options
    ->addOption("--ssl.options",
                "The SSL connection options. See the OpenSSL documentation.",
                new UInt64Parameter(&_ssl_options),
                sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon))
    .setLongDescription(R"(You can use this option to set various SSL-related
options. Individual option values must be combined using bitwise OR.

Which options are available on your platform is determined by the OpenSSL
version you use. The list of options available on your platform might be
retrieved by the following shell command:

```bash
 > grep "#define SSL_OP_.*" /usr/include/openssl/ssl.h

 #define SSL_OP_MICROSOFT_SESS_ID_BUG                    0x00000001L
 #define SSL_OP_NETSCAPE_CHALLENGE_BUG                   0x00000002L
 #define SSL_OP_LEGACY_SERVER_CONNECT                    0x00000004L
 #define SSL_OP_NETSCAPE_REUSE_CIPHER_CHANGE_BUG         0x00000008L
 #define SSL_OP_SSLREF2_REUSE_CERT_TYPE_BUG              0x00000010L
 #define SSL_OP_MICROSOFT_BIG_SSLV3_BUFFER               0x00000020L
 ...
```

A description of the options can be found online in the OpenSSL documentation:
http://www.openssl.org/docs/ssl/SSL_CTX_set_options.html))");

  options->addOption(
    "--ssl.ecdh-curve",
    "The SSL ECDH curve, see the output of \"openssl ecparam -list_curves\".",
    new StringParameter(&_ecdh_curve));

  options->addOption("--ssl.prefer-http1-in-alpn",
                     "Allows to let the server prefer HTTP/1.1 over HTTP/2 in "
                     "ALPN protocol negotiations",
                     new BooleanParameter(&_prefer_http11_in_alpn));
}

void SslServerFeature::validateOptions(
  std::shared_ptr<ProgramOptions> options) {
  // check for SSLv2
  if (_ssl_protocol == SslProtocol::kSslV2) {
    SDB_FATAL("xxxxx", sdb::Logger::SSL,
              "SSLv2 is not supported any longer because of security "
              "vulnerabilities in this protocol");
  }
}

void SslServerFeature::prepare() {
  SDB_INFO("xxxxx", sdb::Logger::SSL,
           "using SSL options: ", stringifySslOptions(_ssl_options));

  if (!_cipher_list.empty()) {
    SDB_INFO("xxxxx", sdb::Logger::SSL, "using SSL cipher-list '", _cipher_list,
             "'");
  }

  random::UniformCharacter r(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
  _rctx = r.random(SSL_MAX_SSL_SESSION_ID_LENGTH);
}

void SslServerFeature::unprepare() {
  SDB_TRACE("xxxxx", sdb::Logger::SSL,
            "unpreparing ssl: ", stringifySslOptions(_ssl_options));
}

void SslServerFeature::verifySslOptions() {
  // check keyfile
  if (_keyfile.empty()) {
    SDB_FATAL("xxxxx", sdb::Logger::SSL,
              "no value specified for '--ssl.keyfile'");
  }

  // validate protocol
  if (_ssl_protocol <= kSslUnknown || _ssl_protocol >= kSslLast) {
    SDB_FATAL("xxxxx", sdb::Logger::SSL,
              "invalid SSL protocol version specified. Please use a valid "
              "value for '--ssl.protocol'");
  }

  SDB_DEBUG("xxxxx", sdb::Logger::SSL, "using SSL protocol version '",
            ProtocolName(SslProtocol(_ssl_protocol)), "'");

  if (!file_utils::Exists(_keyfile)) {
    SDB_FATAL("xxxxx", sdb::Logger::SSL, "unable to find SSL keyfile '",
              _keyfile, "'");
  }

  // Set up first _sni_entry:
  _sni_entries.clear();
  _sni_entries.emplace_back("", _keyfile);

  try {
    createSslContexts();  // just to test if everything works
  } catch (...) {
    SDB_FATAL("xxxxx", sdb::Logger::SSL, "cannot create SSL context");
  }
}

namespace {
class BIOGuard {
 public:
  explicit BIOGuard(BIO* bio) : bio(bio) {}

  ~BIOGuard() { BIO_free(bio); }

 public:
  BIO* bio;
};
}  // namespace

static inline bool SearchForProtocol(const unsigned char** out,
                                     unsigned char* outlen,
                                     const unsigned char* in,
                                     unsigned int inlen, const char* proto) {
  size_t len = strlen(proto);
  size_t i = 0;
  while (i + len <= inlen) {
    if (memcmp(in + i, proto, len) == 0) {
      *out = (const unsigned char*)(in + i + 1);
      *outlen = proto[0];
      return true;
    }
    i += in[i] + 1;
  }
  return false;
}

static int AlpnSelectProtoCb(SSL* ssl, const unsigned char** out,
                             unsigned char* outlen, const unsigned char* in,
                             unsigned int inlen, void* arg) {
  int rv = 0;
  const bool* prefer_http11_in_alpn = (bool*)arg;
  if (*prefer_http11_in_alpn) {
    if (!SearchForProtocol(out, outlen, in, inlen, "\x8http/1.1")) {
      if (!SearchForProtocol(out, outlen, in, inlen, "\x2h2")) {
        rv = -1;
      }
    }
  } else {
    rv = nghttp2_select_next_protocol((unsigned char**)out, outlen, in, inlen);
  }

  if (rv < 0) {
    return SSL_TLSEXT_ERR_NOACK;
  }

  return SSL_TLSEXT_ERR_OK;
}

asio_ns::ssl::context SslServerFeature::createSslContextInternal(
  std::string keyfilename, std::string& content) {
  // This method creates an SSL context using the keyfile in `keyfilename`
  // It is used internally if the public method `createSslContext`
  // is called and if the hello callback happens and a non-default
  // servername extension is detected, then with a non-empty servername.
  // If all goes well, the string `content` is set to the content of the
  // keyfile.
  try {
    std::string keyfile_content = file_utils::Slurp(keyfilename);
    // create context
    asio_ns::ssl::context ssl_context =
      ::SslContext(SslProtocol(_ssl_protocol), keyfilename);
    content = std::move(keyfile_content);

    // and use this native handle
    asio_ns::ssl::context::native_handle_type native_context =
      ssl_context.native_handle();

    // set cache mode
    SSL_CTX_set_session_cache_mode(native_context, _session_cache
                                                     ? SSL_SESS_CACHE_SERVER
                                                     : SSL_SESS_CACHE_OFF);

    if (_session_cache) {
      SDB_TRACE("xxxxx", sdb::Logger::SSL, "using SSL session caching");
    }

    // set options
    ssl_context.set_options(static_cast<long>(_ssl_options));

    if (!_cipher_list.empty()) {
      if (SSL_CTX_set_cipher_list(native_context, _cipher_list.c_str()) != 1) {
        SDB_ERROR("xxxxx", sdb::Logger::SSL, "cannot set SSL cipher list '",
                  _cipher_list, "': ", LastSslError());
        throw std::runtime_error("cannot create SSL context");
      }
    }

    if (!_ecdh_curve.empty()) {
      int ssl_ecdh_nid = OBJ_sn2nid(_ecdh_curve.c_str());

      if (ssl_ecdh_nid == 0) {
        SDB_ERROR("xxxxx", sdb::Logger::SSL, "SSL error: ", LastSslError(),
                  " Unknown curve name: ", _ecdh_curve);
        throw std::runtime_error("cannot create SSL context");
      }

      // https://www.openssl.org/docs/manmaster/apps/ecparam.html
      EC_KEY* ecdh_key = EC_KEY_new_by_curve_name(ssl_ecdh_nid);
      if (ecdh_key == nullptr) {
        SDB_ERROR("xxxxx", sdb::Logger::SSL, "SSL error: ", LastSslError(),
                  ". unable to create curve by name: ", _ecdh_curve);
        throw std::runtime_error("cannot create SSL context");
      }

      if (SSL_CTX_set_tmp_ecdh(native_context, ecdh_key) != 1) {
        EC_KEY_free(ecdh_key);
        SDB_ERROR("xxxxx", sdb::Logger::SSL, "cannot set ECDH option",
                  LastSslError());
        throw std::runtime_error("cannot create SSL context");
      }

      EC_KEY_free(ecdh_key);
      SSL_CTX_set_options(native_context, SSL_OP_SINGLE_ECDH_USE);
    }

    // set ssl context
    int res = SSL_CTX_set_session_id_context(
      native_context, (const unsigned char*)_rctx.c_str(), (int)_rctx.size());

    if (res != 1) {
      SDB_ERROR("xxxxx", sdb::Logger::SSL,
                "cannot set SSL session id context '", _rctx,
                "': ", LastSslError());
      throw std::runtime_error("cannot create SSL context");
    }

    // check CA
    if (!_cafile.empty()) {
      SDB_TRACE("xxxxx", sdb::Logger::SSL,
                "trying to load CA certificates from '", _cafile, "'");

      res =
        SSL_CTX_load_verify_locations(native_context, _cafile.c_str(), nullptr);

      if (res == 0) {
        SDB_ERROR("xxxxx", sdb::Logger::SSL,
                  "cannot load CA certificates from '", _cafile,
                  "': ", LastSslError());
        throw std::runtime_error("cannot create SSL context");
      }

      STACK_OF(X509_NAME) * cert_names;

      std::string cafile_content = file_utils::Slurp(_cafile);
      cert_names = SSL_load_client_CA_file(_cafile.c_str());
      _cafile_content = cafile_content;

      if (cert_names == nullptr) {
        SDB_ERROR("xxxxx", sdb::Logger::SSL,
                  "cannot load CA certificates from '", _cafile,
                  "': ", LastSslError());
        throw std::runtime_error("cannot create SSL context");
      }

      if (log::GetLogLevel() == sdb::LogLevel::TRACE) {
        for (int i = 0; i < sk_X509_NAME_num(cert_names); ++i) {
          X509_NAME* cert = sk_X509_NAME_value(cert_names, i);

          if (cert) {
            BIOGuard bout(BIO_new(BIO_s_mem()));

            X509_NAME_print_ex(bout.bio, cert, 0,
                               (XN_FLAG_SEP_COMMA_PLUS | XN_FLAG_DN_REV |
                                ASN1_STRFLGS_UTF8_CONVERT) &
                                 ~ASN1_STRFLGS_ESC_MSB);

            char* r;
            long len = BIO_get_mem_data(bout.bio, &r);

            SDB_TRACE("xxxxx", sdb::Logger::SSL, "name: ", std::string(r, len));
          }
        }
      }

      SSL_CTX_set_client_CA_list(native_context, cert_names);
    }

    ssl_context.set_verify_mode(SSL_VERIFY_NONE);

    SSL_CTX_set_alpn_select_cb(ssl_context.native_handle(), AlpnSelectProtoCb,
                               (void*)(&_prefer_http11_in_alpn));

    return ssl_context;
  } catch (const std::exception& ex) {
    SDB_ERROR("xxxxx", sdb::Logger::SSL,
              "failed to create SSL context: ", ex.what());
    throw std::runtime_error("cannot create SSL context");
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::SSL,
              "failed to create SSL context, cannot create HTTPS server");
    throw std::runtime_error("cannot create SSL context");
  }
}

SslServerFeature::SslContextList SslServerFeature::createSslContexts() {
  return std::make_shared<SslContextList::element_type>(
    _sni_entries | std::views::transform([&](auto& entry) {
      return createSslContextInternal(entry.keyfile_name,
                                      entry.keyfile_content);
    }) |
    std::ranges::to<std::vector>());
}

size_t SslServerFeature::chooseSslContext(
  const std::string& server_name) const {
  // Note that the map _sni_server_index is basically immutable after the
  // startup phase, since the number of SNI entries cannot be changed
  // at runtime. Therefore, we do not need any protection here.
  auto it = _sni_server_index.find(server_name);
  if (it == _sni_server_index.end()) {
    return 0;
  } else {
    return it->second;
  }
}

std::string SslServerFeature::stringifySslOptions(uint64_t opts) const {
  std::string result;

#ifdef SSL_OP_MICROSOFT_SESS_ID_BUG
  if (opts & SSL_OP_MICROSOFT_SESS_ID_BUG) {
    result.append(", SSL_OP_MICROSOFT_SESS_ID_BUG");
  }
#endif

#ifdef SSL_OP_NETSCAPE_CHALLENGE_BUG
  if (opts & SSL_OP_NETSCAPE_CHALLENGE_BUG) {
    result.append(", SSL_OP_NETSCAPE_CHALLENGE_BUG");
  }
#endif

#ifdef SSL_OP_LEGACY_SERVER_CONNECT
  if (opts & SSL_OP_LEGACY_SERVER_CONNECT) {
    result.append(", SSL_OP_LEGACY_SERVER_CONNECT");
  }
#endif

#ifdef SSL_OP_NETSCAPE_REUSE_CIPHER_CHANGE_BUG
  if (opts & SSL_OP_NETSCAPE_REUSE_CIPHER_CHANGE_BUG) {
    result.append(", SSL_OP_NETSCAPE_REUSE_CIPHER_CHANGE_BUG");
  }
#endif

#ifdef SSL_OP_TLSEXT_PADDING
  if (opts & SSL_OP_TLSEXT_PADDING) {
    result.append(", SSL_OP_TLSEXT_PADDING");
  }
#endif

#ifdef SSL_OP_MICROSOFT_BIG_SSLV3_BUFFER
  if (opts & SSL_OP_MICROSOFT_BIG_SSLV3_BUFFER) {
    result.append(", SSL_OP_MICROSOFT_BIG_SSLV3_BUFFER");
  }
#endif

#ifdef SSL_OP_SAFARI_ECDHE_ECDSA_BUG
  if (opts & SSL_OP_SAFARI_ECDHE_ECDSA_BUG) {
    result.append(", SSL_OP_SAFARI_ECDHE_ECDSA_BUG");
  }
#endif

#ifdef SSL_OP_SSLEAY_080_CLIENT_DH_BUG
  if (opts & SSL_OP_SSLEAY_080_CLIENT_DH_BUG) {
    result.append(", SSL_OP_SSLEAY_080_CLIENT_DH_BUG");
  }
#endif

#ifdef SSL_OP_TLS_D5_BUG
  if (opts & SSL_OP_TLS_D5_BUG) {
    result.append(", SSL_OP_TLS_D5_BUG");
  }
#endif

#ifdef SSL_OP_TLS_BLOCK_PADDING_BUG
  if (opts & SSL_OP_TLS_BLOCK_PADDING_BUG) {
    result.append(", SSL_OP_TLS_BLOCK_PADDING_BUG");
  }
#endif

#ifdef SSL_OP_MSIE_SSLV2_RSA_PADDING
  if (opts & SSL_OP_MSIE_SSLV2_RSA_PADDING) {
    result.append(", SSL_OP_MSIE_SSLV2_RSA_PADDING");
  }
#endif

#ifdef SSL_OP_SSLREF2_REUSE_CERT_TYPE_BUG
  if (opts & SSL_OP_SSLREF2_REUSE_CERT_TYPE_BUG) {
    result.append(", SSL_OP_SSLREF2_REUSE_CERT_TYPE_BUG");
  }
#endif

#ifdef SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS
  if (opts & SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS) {
    result.append(", SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS");
  }
#endif

#ifdef SSL_OP_NO_QUERY_MTU
  if (opts & SSL_OP_NO_QUERY_MTU) {
    result.append(", SSL_OP_NO_QUERY_MTU");
  }
#endif

#ifdef SSL_OP_COOKIE_EXCHANGE
  if (opts & SSL_OP_COOKIE_EXCHANGE) {
    result.append(", SSL_OP_COOKIE_EXCHANGE");
  }
#endif

#ifdef SSL_OP_NO_TICKET
  if (opts & SSL_OP_NO_TICKET) {
    result.append(", SSL_OP_NO_TICKET");
  }
#endif

#ifdef SSL_OP_CISCO_ANYCONNECT
  if (opts & SSL_OP_CISCO_ANYCONNECT) {
    result.append(", SSL_OP_CISCO_ANYCONNECT");
  }
#endif

#ifdef SSL_OP_NO_SESSION_RESUMPTION_ON_RENEGOTIATION
  if (opts & SSL_OP_NO_SESSION_RESUMPTION_ON_RENEGOTIATION) {
    result.append(", SSL_OP_NO_SESSION_RESUMPTION_ON_RENEGOTIATION");
  }
#endif

#ifdef SSL_OP_NO_COMPRESSION
  if (opts & SSL_OP_NO_COMPRESSION) {
    result.append(", SSL_OP_NO_COMPRESSION");
  }
#endif

#ifdef SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION
  if (opts & SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION) {
    result.append(", SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION");
  }
#endif

#ifdef SSL_OP_SINGLE_ECDH_USE
  if (opts & SSL_OP_SINGLE_ECDH_USE) {
    result.append(", SSL_OP_SINGLE_ECDH_USE");
  }
#endif

#ifdef SSL_OP_SINGLE_DH_USE
  if (opts & SSL_OP_SINGLE_DH_USE) {
    result.append(", SSL_OP_SINGLE_DH_USE");
  }
#endif

#ifdef SSL_OP_EPHEMERAL_RSA
  if (opts & SSL_OP_EPHEMERAL_RSA) {
    result.append(", SSL_OP_EPHEMERAL_RSA");
  }
#endif

#ifdef SSL_OP_CIPHER_SERVER_PREFERENCE
  if (opts & SSL_OP_CIPHER_SERVER_PREFERENCE) {
    result.append(", SSL_OP_CIPHER_SERVER_PREFERENCE");
  }
#endif

#ifdef SSL_OP_TLS_ROLLBACK_BUG
  if (opts & SSL_OP_TLS_ROLLBACK_BUG) {
    result.append(", SSL_OP_TLS_ROLLBACK_BUG");
  }
#endif

#ifdef SSL_OP_NO_SSLv2
  if (opts & SSL_OP_NO_SSLv2) {
    result.append(", SSL_OP_NO_SSLv2");
  }
#endif

#ifdef SSL_OP_NO_SSLv3
  if (opts & SSL_OP_NO_SSLv3) {
    result.append(", SSL_OP_NO_SSLv3");
  }
#endif

#ifdef SSL_OP_NO_TLSv1
  if (opts & SSL_OP_NO_TLSv1) {
    result.append(", SSL_OP_NO_TLSv1");
  }
#endif

#ifdef SSL_OP_NO_TLSv1_2
  if (opts & SSL_OP_NO_TLSv1_2) {
    result.append(", SSL_OP_NO_TLSv1_2");
  }
#endif

#ifdef SSL_OP_NO_TLSv1_1
  if (opts & SSL_OP_NO_TLSv1_1) {
    result.append(", SSL_OP_NO_TLSv1_1");
  }
#endif

#ifdef SSL_OP_NO_DTLSv1
  if (opts & SSL_OP_NO_DTLSv1) {
    result.append(", SSL_OP_NO_DTLSv1");
  }
#endif

#ifdef SSL_OP_NO_DTLSv1_2
  if (opts & SSL_OP_NO_DTLSv1_2) {
    result.append(", SSL_OP_NO_DTLSv1_2");
  }
#endif

#ifdef SSL_OP_NO_SSL_MASK
  if (opts & SSL_OP_NO_SSL_MASK) {
    result.append(", SSL_OP_NO_SSL_MASK");
  }
#endif

#ifdef SSL_OP_PKCS1_CHECK_1
  if (SSL_OP_PKCS1_CHECK_1) {
    if (opts & SSL_OP_PKCS1_CHECK_1) {
      result.append(", SSL_OP_PKCS1_CHECK_1");
    }
  }
#endif

#ifdef SSL_OP_PKCS1_CHECK_2
  if (SSL_OP_PKCS1_CHECK_1) {
    if (opts & SSL_OP_PKCS1_CHECK_2) {
      result.append(", SSL_OP_PKCS1_CHECK_2");
    }
  }
#endif

#ifdef SSL_OP_NETSCAPE_CA_DN_BUG
  if (SSL_OP_NETSCAPE_CA_DN_BUG) {
    if (opts & SSL_OP_NETSCAPE_CA_DN_BUG) {
      result.append(", SSL_OP_NETSCAPE_CA_DN_BUG");
    }
  }
#endif

#ifdef SSL_OP_NETSCAPE_DEMO_CIPHER_CHANGE_BUG
  if (opts & SSL_OP_NETSCAPE_DEMO_CIPHER_CHANGE_BUG) {
    result.append(", SSL_OP_NETSCAPE_DEMO_CIPHER_CHANGE_BUG");
  }
#endif

#ifdef SSL_OP_CRYPTOPRO_TLSEXT_BUG
  if (opts & SSL_OP_CRYPTOPRO_TLSEXT_BUG) {
    result.append(", SSL_OP_CRYPTOPRO_TLSEXT_BUG");
  }
#endif

  if (result.empty()) {
    return result;
  }

  // strip initial comma
  return result.substr(2);
}

static void SplitPem(const std::string& pem, std::vector<std::string>& certs,
                     std::vector<std::string>& keys) {
  std::vector<std::string> result;
  size_t pos = 0;
  while (pos < pem.size()) {
    pos = pem.find("-----", pos);
    if (pos == std::string::npos) {
      return;
    }
    if (pem.compare(pos, 11, "-----BEGIN ") != 0) {
      return;
    }
    size_t pos_end_header = pem.find('\n', pos);
    if (pos_end_header == std::string::npos) {
      return;
    }
    size_t pos_start_footer = pem.find("-----END ", pos_end_header);
    if (pos_start_footer == std::string::npos) {
      return;
    }
    size_t pos_end_footer = pem.find("-----", pos_start_footer + 9);
    if (pos_end_footer == std::string::npos) {
      return;
    }
    pos_end_footer += 5;  // Point to line end, typically or end of file
    size_t p = pos_end_header;
    while (p > pos + 11 && (pem[p] == '\n' || pem[p] == '-' || pem[p] == '\r' ||
                            pem[p] == ' ')) {
      --p;
    }
    std::string_view type(pem.c_str() + pos + 11, (p + 1) - (pos + 11));
    if (type == "CERTIFICATE") {
      certs.emplace_back(pem.c_str() + pos, pos_end_footer - pos);
    } else if (type.find("PRIVATE KEY") != std::string::npos) {
      keys.emplace_back(pem.c_str() + pos, pos_end_footer - pos);
    } else {
      SDB_INFO("xxxxx", Logger::SSL, "Found part of type ", type,
               " in PEM file, ignoring it...");
    }
    pos = pos_end_footer;
  }
}

static void DumpPem(const std::string& pem, vpack::Builder& builder,
                    std::string attr_name) {
  if (pem.empty()) {
    {
      vpack::ObjectBuilder guard(&builder, attr_name);
      return;
    }
  }
  // Compute a SHA256 of the whole file:
  Sha256Functor func;
  func(pem.c_str(), pem.size());

  // Now split into certs and key:
  std::vector<std::string> certs;
  std::vector<std::string> keys;
  SplitPem(pem, certs, keys);

  // Now dump the certs and the hash of the key:
  {
    vpack::ObjectBuilder guard2(&builder, attr_name);
    auto sha256 = func.finalize();
    builder.add("sha256", sha256);
    builder.add("SHA256", sha256);  // deprecated in 3.7 GA
    {
      vpack::ArrayBuilder guard3(&builder, "certificates");
      for (const auto& c : certs) {
        builder.add(c);
      }
    }
    if (!keys.empty()) {
      Sha256Functor func2;
      func2(keys[0].c_str(), keys[0].size());
      sha256 = func2.finalize();
      builder.add("privateKeySha256", sha256);
      builder.add("privateKeySHA256",
                  sha256);  // deprecated in 3.7 GA
    }
  }
}

// Dump all SSL related data into a builder, private keys are hashed.
void SslServerFeature::dumpTLSData(vpack::Builder& builder) const {
  vpack::ObjectBuilder guard(&builder);
  if (_sni_entries.empty()) {
    return;
  }
  DumpPem(_sni_entries[0].keyfile_content, builder, "keyfile");
  DumpPem(_cafile_content, builder, "clientCA");
  if (_sni_entries.size() > 1) {
    vpack::ObjectBuilder guard2(&builder, "SNI");
    for (size_t i = 1; i < _sni_entries.size(); ++i) {
      DumpPem(_sni_entries[i].keyfile_content, builder,
              _sni_entries[i].server_name);
    }
  }
}
