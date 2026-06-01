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

#ifndef __STDC_LIMIT_MACROS
#define STDC_LIMIT_MACROS
#endif

#ifdef __linux__

// necessary defines and includes

#define SERENEDB_PLATFORM "linux"

// force posix source
#if !defined(_POSIX_C_SOURCE)
#define POSIX_C_SOURCE 200809L
#endif

// first include the features file and then define
#include <features.h>

// for INTxx_MIN and INTxx_MAX
#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS 1
#endif

#define SERENEDB_GETRUSAGE_MAXRSS_UNIT 1024

// available include files

#define SERENEDB_HAVE_DIRENT_H 1
#define SERENEDB_HAVE_GETRLIMIT 1
#define SERENEDB_HAVE_NETDB_H 1
#define SERENEDB_HAVE_NETINET_STAR_H 1
#define SERENEDB_HAVE_POLL_H 1
#define SERENEDB_HAVE_SIGNAL_H 1
#define SERENEDB_HAVE_SYS_IOCTL_H 1
#define SERENEDB_HAVE_SYS_PRCTL_H 1
#define SERENEDB_HAVE_SYS_RESOURCE_H 1
#define SERENEDB_HAVE_SYS_SOCKET_H 1
#define SERENEDB_HAVE_SYS_TIME_H 1
#define SERENEDB_HAVE_SYS_TYPES_H 1
#define SERENEDB_HAVE_SYS_WAIT_H 1
#define SERENEDB_HAVE_TERMIOS_H 1

// available functions

#define SERENEDB_HAVE_FORK 1
#define SERENEDB_HAVE_GETGRGID 1
#define SERENEDB_HAVE_GETGRNAM 1
#define SERENEDB_HAVE_GETPWNAM 1
#define SERENEDB_HAVE_GETPWUID 1
#define SERENEDB_HAVE_GETRUSAGE 1
#define SERENEDB_HAVE_GMTIME_R 1
#undef SERENEDB_HAVE_GMTIME_S
#define SERENEDB_HAVE_INITGROUPS 1
#define SERENEDB_HAVE_LOCALTIME_R 1
#undef SERENEDB_HAVE_LOCALTIME_S
#define SERENEDB_HAVE_SETGID 1
#define SERENEDB_HAVE_SETUID 1

#define SERENEDB_HAVE_PRCTL 1

// available features

#define SERENEDB_HAVE_DOMAIN_SOCKETS 1
#define SERENEDB_HAVE_LINUX_PROC 1
#define SERENEDB_HAVE_POSIX_THREADS 1
#define SERENEDB_HAVE_SC_PHYS_PAGES 1

#define SERENEDB_SC_NPROCESSORS_ONLN 1

// files

#define SERENEDB_DIR_SEPARATOR_CHR '/'
#define SERENEDB_DIR_SEPARATOR_STR "/"

#define SERENEDB_O_CLOEXEC O_CLOEXEC

#define SERENEDB_CHDIR ::chdir
#define SERENEDB_CLOSE ::close
#define SERENEDB_CREATE(a, b, c) ::open((a), (b), (c))
#define SERENEDB_FSTAT ::fstat
#define SERENEDB_GETCWD ::getcwd
#define SERENEDB_LSEEK ::lseek
#define SERENEDB_MKDIR(a, b) ::mkdir((a), (b))
#define SERENEDB_OPEN(a, b) ::open((a), (b))
#define SERENEDB_FOPEN(a, b) ::fopen((a), (b))
#define SERENEDB_READ ::read
#define SERENEDB_DUP ::dup
#define SERENEDB_RMDIR ::rmdir
#define SERENEDB_STAT ::stat
#define SERENEDB_STAT_ATIME_SEC(statbuf) (statbuf).st_atim.tv_sec
#define SERENEDB_STAT_MTIME_SEC(statbuf) (statbuf).st_mtim.tv_sec
#define SERENEDB_UNLINK ::unlink
#define SERENEDB_WRITE ::write
#define SERENEDB_FDOPEN(a, b) ::fdopen((a), (b))
#define SERENEDB_IS_INVALID_PIPE(a) ((a) == 0)

#define SERENEDB_ERRORNO_STR ::strerror(errno)

// sockets

#define SERENEDB_INVALID_SOCKET (-1)

// user and group types

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#endif
