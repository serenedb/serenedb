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

#include "socket-utils.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>

#ifdef SERENEDB_HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "basics/errors.h"
#include "basics/logger/logger.h"

namespace sdb {

////////////////////////////////////////////////////////////////////////////////
/// closes a socket
////////////////////////////////////////////////////////////////////////////////

int Sdbclosesocket(SocketWrapper s) {
  int res = 0;
  if (s.file_descriptor != SERENEDB_INVALID_SOCKET) {
    res = close(s.file_descriptor);

    if (res == -1) {
      int myerrno = errno;
      SDB_WARN("xxxxx", sdb::Logger::FIXME, "socket close error: ", myerrno,
               ": ", strerror(myerrno));
    }
  }
  return res;
}

int Sdbreadsocket(SocketWrapper s, void* buffer, size_t num_bytes_to_read,
                  int flags) {
  int res = read(s.file_descriptor, buffer, num_bytes_to_read);
  return res;
}

////////////////////////////////////////////////////////////////////////////////
/// sets close-on-exit for a socket
////////////////////////////////////////////////////////////////////////////////

bool SdbSetCloseOnExecSocket(SocketWrapper s) {
  long flags = fcntl(s.file_descriptor, F_GETFD, 0);

  if (flags < 0) {
    return false;
  }

  flags = fcntl(s.file_descriptor, F_SETFD, flags | FD_CLOEXEC);

  if (flags < 0) {
    return false;
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// sets non-blocking mode for a socket
////////////////////////////////////////////////////////////////////////////////

bool SdbSetNonBlockingSocket(SocketWrapper s) {
  long flags = fcntl(s.file_descriptor, F_GETFL, 0);

  if (flags < 0) {
    return false;
  }

  flags = fcntl(s.file_descriptor, F_SETFL, flags | O_NONBLOCK);

  if (flags < 0) {
    return false;
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// translates for IPv4 address
///
/// This code is copyright Internet Systems Consortium, Inc. ("ISC")
////////////////////////////////////////////////////////////////////////////////

ErrorCode SdbInetPton4(const char* src, unsigned char* dst) {
  static const char kDigits[] = "0123456789";

  int saw_digit, octets, ch;
  unsigned char tmp[sizeof(struct in_addr)], *tp;

  if (nullptr == src) {
    return ERROR_IP_ADDRESS_INVALID;
  }

  saw_digit = 0;
  octets = 0;
  *(tp = tmp) = 0;

  while ((ch = *src++) != '\0') {
    const char* pch;

    if ((pch = strchr(kDigits, ch)) != nullptr) {
      unsigned int nw = (unsigned int)(*tp * 10 + (pch - kDigits));

      if (saw_digit && *tp == 0) {
        return ERROR_IP_ADDRESS_INVALID;
      }

      if (nw > 255) {
        return ERROR_IP_ADDRESS_INVALID;
      }

      *tp = nw;

      if (!saw_digit) {
        if (++octets > 4) {
          return ERROR_IP_ADDRESS_INVALID;
        }

        saw_digit = 1;
      }
    } else if (ch == '.' && saw_digit) {
      if (octets == 4) {
        return ERROR_IP_ADDRESS_INVALID;
      }

      *++tp = 0;
      saw_digit = 0;
    } else {
      return ERROR_IP_ADDRESS_INVALID;
    }
  }

  if (octets < 4) {
    return ERROR_IP_ADDRESS_INVALID;
  }

  if (nullptr != dst) {
    memcpy(dst, tmp, sizeof(struct in_addr));
  }

  return ERROR_OK;
}

////////////////////////////////////////////////////////////////////////////////
/// translates for IPv6 address
///
/// This code is copyright Internet Systems Consortium, Inc. ("ISC")
////////////////////////////////////////////////////////////////////////////////

ErrorCode SdbInetPton6(const char* src, unsigned char* dst) {
  static const char kXdigitsL[] = "0123456789abcdef";
  static const char kXdigitsU[] = "0123456789ABCDEF";

  unsigned char tmp[sizeof(struct in6_addr)], *tp, *endp, *colonp;
  const char* curtok;
  int ch, seen_xdigits;
  unsigned int val;

  if (nullptr == src) {
    return ERROR_IP_ADDRESS_INVALID;
  }

  memset((tp = tmp), '\0', sizeof tmp);
  endp = tp + sizeof tmp;
  colonp = nullptr;

  /* Leading :: requires some special handling. */
  if (*src == ':') {
    if (*++src != ':') {
      return ERROR_IP_ADDRESS_INVALID;
    }
  }

  curtok = src;
  seen_xdigits = 0;
  val = 0;

  while ((ch = *src++) != '\0') {
    const char* pch;
    const char* xdigits;

    if ((pch = strchr((xdigits = kXdigitsL), ch)) == nullptr) {
      pch = strchr((xdigits = kXdigitsU), ch);
    }

    if (pch != nullptr) {
      val <<= 4;
      val |= (pch - xdigits);

      if (++seen_xdigits > 4) {
        return ERROR_IP_ADDRESS_INVALID;
      }

      continue;
    }

    if (ch == ':') {
      curtok = src;

      if (!seen_xdigits) {
        if (colonp) {
          return ERROR_IP_ADDRESS_INVALID;
        }

        colonp = tp;
        continue;
      } else if (*src == '\0') {
        return ERROR_IP_ADDRESS_INVALID;
      }

      if (tp + sizeof(uint16_t) > endp) {
        return ERROR_IP_ADDRESS_INVALID;
      }

      *tp++ = (unsigned char)(val >> 8) & 0xff;
      *tp++ = (unsigned char)val & 0xff;
      seen_xdigits = 0;
      val = 0;

      continue;
    }

    if (ch == '.' && ((tp + sizeof(struct in_addr)) <= endp)) {
      auto err = SdbInetPton4(curtok, tp);

      if (err == ERROR_OK) {
        tp += sizeof(struct in_addr);
        seen_xdigits = 0;
        break; /*%< '\\0' was seen by inet_pton4(). */
      }
    }

    return ERROR_IP_ADDRESS_INVALID;
  }

  if (seen_xdigits) {
    if (tp + sizeof(uint16_t) > endp) {
      return ERROR_IP_ADDRESS_INVALID;
    }

    *tp++ = (unsigned char)(val >> 8) & 0xff;
    *tp++ = (unsigned char)val & 0xff;
  }

  if (colonp != nullptr) {
    /*
     * Since some memmove()'s erroneously fail to handle
     * overlapping regions, we'll do the shift by hand.
     */
    const int n = (int)(tp - colonp);
    int i;

    if (tp == endp) {
      return ERROR_IP_ADDRESS_INVALID;
    }

    for (i = 1; i <= n; i++) {
      endp[-i] = colonp[n - i];
      colonp[n - i] = 0;
    }

    tp = endp;
  }

  if (tp != endp) {
    return ERROR_IP_ADDRESS_INVALID;
  }

  if (nullptr != dst) {
    memcpy(dst, tmp, sizeof tmp);
  }

  return ERROR_OK;
}

bool Sdbsetsockopttimeout(SocketWrapper s, double timeout) {
  struct timeval tv;

  // shut up Valgrind
  memset(&tv, 0, sizeof(tv));
  tv.tv_sec = (time_t)timeout;
  tv.tv_usec = (suseconds_t)((timeout - (double)tv.tv_sec) * 1000000.0);

  if (Sdbsetsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0) {
    return false;
  }

  if (Sdbsetsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0) {
    return false;
  }
  return true;
}

}  // namespace sdb
