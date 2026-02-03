/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/datetime.c
 * src/include/datatype/timestamp.h
 *-------------------------------------------------------------------------
 */

#include "pg/functions/interval.h"

#include <cstring>

#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "common/int.h"
#include "datatype/timestamp.h"
#include "miscadmin.h"
#include "utils/datetime.h"
#include "utils/errcodes.h"
#include "utils/timestamp.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

namespace {

// TODO : move this to libpq when we change their error macro act as our
// THROW_SQL
bool AdjustIntervalForTypmod(Interval& interval, int32_t range,
                             int32_t precision) {
  static const int64 IntervalScales[MAX_INTERVAL_PRECISION + 1] = {
    INT64CONST(1000000), INT64CONST(100000), INT64CONST(10000),
    INT64CONST(1000),    INT64CONST(100),    INT64CONST(10),
    INT64CONST(1)};

  static const int64 IntervalOffsets[MAX_INTERVAL_PRECISION + 1] = {
    INT64CONST(500000), INT64CONST(50000), INT64CONST(5000), INT64CONST(500),
    INT64CONST(50),     INT64CONST(5),     INT64CONST(0)};

  /* Typmod has no effect on infinite intervals */
  if (INTERVAL_NOT_FINITE(&interval)) {
    return true;
  }

  /*
   * Our interpretation of intervals with a limited set of fields is
   * that fields to the right of the last one specified are zeroed out,
   * but those to the left of it remain valid.  Thus for example there
   * is no operational difference between INTERVAL YEAR TO MONTH and
   * INTERVAL MONTH.  In some cases we could meaningfully enforce that
   * higher-order fields are zero; for example INTERVAL DAY could reject
   * nonzero "month" field.  However that seems a bit pointless when we
   * can't do it consistently.  (We cannot enforce a range limit on the
   * highest expected field, since we do not have any equivalent of
   * SQL's <interval leading field precision>.)  If we ever decide to
   * revisit this, interval_support will likely require adjusting.
   *
   * Note: before PG 8.4 we interpreted a limited set of fields as
   * actually causing a "modulo" operation on a given value, potentially
   * losing high-order as well as low-order information.  But there is
   * no support for such behavior in the standard, and it seems fairly
   * undesirable on data consistency grounds anyway.  Now we only
   * perform truncation or rounding of low-order fields.
   */
  if (range == INTERVAL_FULL_RANGE) {
    /* Do nothing... */
  } else if (range == INTERVAL_MASK(YEAR)) {
    interval.month = (interval.month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
    interval.day = 0;
    interval.time = 0;
  } else if (range == INTERVAL_MASK(MONTH)) {
    interval.day = 0;
    interval.time = 0;
  }
  /* YEAR TO MONTH */
  else if (range == (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH))) {
    interval.day = 0;
    interval.time = 0;
  } else if (range == INTERVAL_MASK(DAY)) {
    interval.time = 0;
  } else if (range == INTERVAL_MASK(HOUR)) {
    interval.time = (interval.time / USECS_PER_HOUR) * USECS_PER_HOUR;
  } else if (range == INTERVAL_MASK(MINUTE)) {
    interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
  } else if (range == INTERVAL_MASK(SECOND)) {
    /* fractional-second rounding will be dealt with below */
  }
  /* DAY TO HOUR */
  else if (range == (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR))) {
    interval.time = (interval.time / USECS_PER_HOUR) * USECS_PER_HOUR;
  }
  /* DAY TO MINUTE */
  else if (range ==
           (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) {
    interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
  }
  /* DAY TO SECOND */
  else if (range == (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) |
                     INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND))) {
    /* fractional-second rounding will be dealt with below */
  }
  /* HOUR TO MINUTE */
  else if (range == (INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) {
    interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
  }
  /* HOUR TO SECOND */
  else if (range == (INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) |
                     INTERVAL_MASK(SECOND))) {
    /* fractional-second rounding will be dealt with below */
  }
  /* MINUTE TO SECOND */
  else if (range == (INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND))) {
    /* fractional-second rounding will be dealt with below */
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized range: ", range));
  }

  /* Need to adjust sub-second precision? */
  if (precision != INTERVAL_FULL_PRECISION) {
    if (precision < 0 || precision > MAX_INTERVAL_PRECISION) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        ERR_MSG("interval(", precision, ") precision must be between 0 and",
                MAX_INTERVAL_PRECISION));
    }

    if (interval.time >= INT64CONST(0)) {
      if (pg_add_s64_overflow(interval.time, IntervalOffsets[precision],
                              &interval.time)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        ERR_MSG("interval out of range"));
      }

      interval.time -= interval.time % IntervalScales[precision];
    } else {
      if (pg_sub_s64_overflow(interval.time, IntervalOffsets[precision],
                              &interval.time)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        ERR_MSG("interval out of range"));
      }

      interval.time -= interval.time % IntervalScales[precision];
    }
  }

  return true;
}

// TODO : move this to libpq when we change their error macro act as our
// THROW_SQL
[[noreturn]] void DateTimeParseError(int dterr, DateTimeErrorExtra* extra,
                                     const char* str, const char* datatype) {
  switch (dterr) {
    case DTERR_FIELD_OVERFLOW:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATETIME_FIELD_OVERFLOW),
        ERR_MSG("date/time field value out of range: \"", str, "\""));
    case DTERR_MD_FIELD_OVERFLOW:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATETIME_FIELD_OVERFLOW),
        ERR_MSG("date/time field value out of range: \"", str, "\""),
        ERR_HINT("Perhaps you need a different \"DateStyle\" setting."));
    case DTERR_INTERVAL_OVERFLOW:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERVAL_FIELD_OVERFLOW),
        ERR_MSG("interval field value out of range: \"", str, "\""));
    case DTERR_TZDISP_OVERFLOW:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE),
        ERR_MSG("time zone displacement out of range: \"", str, "\""));
    case DTERR_BAD_TIMEZONE:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("time zone \"", extra->dtee_timezone, "\" not recognized"));
    case DTERR_BAD_ZONE_ABBREV:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_CONFIG_FILE_ERROR),
        ERR_MSG("time zone \"", extra->dtee_timezone, "\" not recognized"),
        ERR_DETAIL("This time zone name appears in the configuration file for "
                   "time zone abbreviation \"",
                   extra->dtee_abbrev, "\"."));
    case DTERR_BAD_FORMAT:
    default:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
        ERR_MSG("invalid input syntax for type ", datatype, ": \"", str, "\""));
  }
}

// based on postgres interval_in function
Interval interval_in(std::string_view input, int32_t range, int32_t precision) {
  Interval result;
  struct pg_itm_in tt, *itm_in = &tt;
  int dtype;
  int nf;
  int dterr;
  char* field[MAXDATEFIELDS];
  int ftype[MAXDATEFIELDS];
  char workbuf[256];
  DateTimeErrorExtra extra;

  itm_in->tm_year = 0;
  itm_in->tm_mon = 0;
  itm_in->tm_mday = 0;
  itm_in->tm_usec = 0;

  // TODO(pashandor789): remove copy
  auto copy = std::string{input};
  dterr = ParseDateTime(copy.c_str(), workbuf, sizeof(workbuf), field, ftype,
                        MAXDATEFIELDS, &nf);
  if (dterr == 0) {
    dterr = DecodeInterval(field, ftype, nf, range, &dtype, itm_in);
  }

  char* str = copy.data();
  /* if those functions think it's a bad format, try ISO8601 style */
  if (dterr == DTERR_BAD_FORMAT) {
    dterr = DecodeISO8601Interval(str, &dtype, itm_in);
  }

  if (dterr != 0) {
    if (dterr == DTERR_FIELD_OVERFLOW) {
      dterr = DTERR_INTERVAL_OVERFLOW;
    }
    DateTimeParseError(dterr, &extra, str, "interval");
  }

  switch (dtype) {
    case DTK_DELTA:
      if (itmin2interval(itm_in, &result) != 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        ERR_MSG("interval out of range"));
      }
      break;
    case DTK_LATE:
      INTERVAL_NOEND(&result);
      break;
    case DTK_EARLY:
      INTERVAL_NOBEGIN(&result);
      break;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
                      ERR_MSG("unexpected dtype", dtype,
                              "while parsing interval \"", str, "\""));
  }

  AdjustIntervalForTypmod(result, range, precision);

  return result;
}

// based on postgres interval_out function
std::string interval_out(const Interval* span) {
  struct pg_itm tt, *itm = &tt;

  if (INTERVAL_IS_NOBEGIN(span)) {
    return EARLY;
  } else if (INTERVAL_IS_NOEND(span)) {
    return LATE;
  } else {
    std::string buf;
    buf.resize(MAXDATELEN + 1, '\0');
    interval2itm(*span, itm);
    EncodeInterval(itm, INTSTYLE_POSTGRES, buf.data());
    SDB_ASSERT(buf.contains('\0'));
    buf.resize(strlen(buf.data()));
    return buf;
  }
}

template<typename Interval>
velox::int128_t PackInterval(const Interval& interval) {
  uint64_t lower =
    (static_cast<uint64_t>(static_cast<uint32_t>(interval.month)) << 32) |
    static_cast<uint64_t>(static_cast<uint32_t>(interval.day));
  uint64_t upper = static_cast<uint64_t>(interval.time);

  return velox::HugeInt::build(upper, lower);
}

template<typename Interval>
Interval UnpackInterval(velox::int128_t packed) {
  Interval interval;
  uint64_t lower = velox::HugeInt::lower(packed);
  interval.month = static_cast<int32_t>(lower >> 32);
  interval.day = static_cast<int32_t>(lower & 0xFFFFFFFF);
  interval.time = static_cast<int64_t>(velox::HugeInt::upper(packed));
  return interval;
}

}  // namespace

velox::int128_t IntervalIn(std::string_view input, int32_t range,
                           int32_t precision) {
  auto interval = interval_in(input, range, precision);
  return PackInterval(interval);
}

std::string IntervalOut(const velox::int128_t& interval) {
  auto unpacked_interval = UnpackInterval<Interval>(interval);
  return interval_out(&unpacked_interval);
}

velox::int128_t PackInterval(const UnpackedInterval& interval) {
  return PackInterval<UnpackedInterval>(interval);
}

UnpackedInterval UnpackInterval(velox::int128_t packed) {
  return UnpackInterval<UnpackedInterval>(packed);
}

}  // namespace sdb::pg
