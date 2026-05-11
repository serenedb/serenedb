# R / RPostgres round-trip matrix harness.

suppressMessages({
  library(DBI)
  library(RPostgres)
  library(yaml)
})

driver_key <- "r_rpostgres"
protocols  <- c("simple", "extended-noparam", "extended-text", "extended-binary")

spec_dir <- Sys.getenv("SDB_DRV_SPEC", unset = file.path("..", "spec"))
host     <- Sys.getenv("SDB_DRV_HOST", "localhost")
port     <- as.integer(Sys.getenv("SDB_DRV_PORT", "5432"))
db       <- Sys.getenv("SDB_DRV_DATABASE", "postgres")
user     <- Sys.getenv("SDB_DRV_USER", "postgres")
run_id   <- Sys.getenv("SDB_DRV_RUN_ID", "0")
schema   <- paste0("drv_", driver_key, "_", run_id)
type_re  <- Sys.getenv("SDB_DRV_TYPES", ".*")
proto_filter <- strsplit(Sys.getenv("SDB_DRV_PROTOCOLS",
                                    paste(protocols, collapse = ",")), ",")[[1]]

types <- yaml::yaml.load_file(file.path(spec_dir, "types.yaml"))

# Build the case list (mirrors spec_loader.py).
cases <- list()
for (t in types) {
  name <- if (!is.null(t$name)) t$name else t$pg_typname
  if (!grepl(type_re, name)) next
  if (isFALSE(t$text) && isFALSE(t$binary)) next
  for (proto in protocols) {
    if (!(proto %in% proto_filter)) next
    supports <- if (proto == "extended-binary") isTRUE(t$binary) else isTRUE(t$text)
    if (!supports) next
    skip <- t$skip[[driver_key]]
    if (!is.null(skip) && (("all" %in% skip) || (proto %in% skip))) next
    samples <- if (is.null(t$samples)) list() else t$samples
    for (i in seq_along(samples)) {
      cases[[length(cases) + 1L]] <- list(
        oid       = t$oid,
        name      = name,
        pg_typname = if (!is.null(t$pg_typname)) t$pg_typname else name,
        cast_sql  = if (!is.null(t$cast_sql)) t$cast_sql else paste0("$1::", t$pg_typname),
        proto     = proto,
        idx       = i - 1L,
        sample    = samples[[i]]
      )
    }
  }
}

con <- dbConnect(RPostgres::Postgres(),
                 host = host, port = port, dbname = db, user = user)
dbExecute(con, paste0('CREATE SCHEMA IF NOT EXISTS "', schema, '"'))
dbExecute(con, paste0('SET search_path TO "', schema, '", public, pg_catalog'))

passed <- 0L
failed <- 0L
junit_lines <- character()

# smoke
v <- dbGetQuery(con, "SELECT 1::int4 AS v")$v
if (length(v) == 1L && v == 1L) {
  passed <- passed + 1L
  junit_lines <- c(junit_lines,
    '  <testcase classname="r_rpostgres" name="smoke_select_one"/>')
} else {
  failed <- failed + 1L
  junit_lines <- c(junit_lines,
    '  <testcase classname="r_rpostgres" name="smoke_select_one"><failure/></testcase>')
}

xml_escape <- function(s) {
  if (is.null(s)) return("")
  s <- gsub("&", "&amp;", s, fixed = TRUE)
  s <- gsub("<", "&lt;",  s, fixed = TRUE)
  s <- gsub(">", "&gt;",  s, fixed = TRUE)
  s
}

server_text <- function(con, pg_typname, sample) {
  if (is.null(sample) || is.na(sample)) return(NA_character_)
  q <- paste0("SELECT (($1::text)::", pg_typname, ")::text AS v")
  r <- dbGetQuery(con, q, params = list(as.character(sample)))
  if (nrow(r) == 0L) NA_character_ else as.character(r$v[1])
}

run_inline <- function(con, c) {
  if (is.null(c$sample) || is.na(c$sample)) {
    sql <- paste0("SELECT NULL::", c$pg_typname, "::text AS v")
  } else {
    lit <- gsub("'", "''", as.character(c$sample), fixed = TRUE)
    sql <- paste0("SELECT '", lit, "'::", c$pg_typname, "::text AS v")
  }
  r <- dbGetQuery(con, sql)
  if (nrow(r) == 0L) NA_character_ else as.character(r$v[1])
}

run_bound <- function(con, c) {
  cast <- sub("\\$1", "\\$1::text", c$cast_sql)
  sql <- paste0("SELECT (", cast, ")::text AS v")
  param <- if (is.null(c$sample) || is.na(c$sample)) NA else as.character(c$sample)
  r <- dbGetQuery(con, sql, params = list(param))
  if (nrow(r) == 0L) NA_character_ else as.character(r$v[1])
}

for (c in cases) {
  id <- paste0(c$oid, "-", c$name, "-", c$proto, "-", c$idx)
  result <- tryCatch({
    expected <- server_text(con, c$pg_typname, c$sample)
    actual <- if (c$proto %in% c("simple", "extended-noparam"))
              run_inline(con, c) else run_bound(con, c)
    list(ok = identical(expected, actual),
         expected = expected, actual = actual, err = NULL)
  }, error = function(e) {
    list(ok = FALSE, expected = NA_character_,
         actual = NA_character_, err = conditionMessage(e))
  })
  if (isTRUE(result$ok)) {
    passed <- passed + 1L
    junit_lines <- c(junit_lines, paste0(
      '  <testcase classname="r_rpostgres" name="', id, '"/>'))
  } else {
    failed <- failed + 1L
    if (!is.null(result$err)) {
      junit_lines <- c(junit_lines, paste0(
        '  <testcase classname="r_rpostgres" name="', id,
        '"><error>', xml_escape(result$err), '</error></testcase>'))
    } else {
      junit_lines <- c(junit_lines, paste0(
        '  <testcase classname="r_rpostgres" name="', id,
        '"><failure>expected=', xml_escape(result$expected),
        ' actual=', xml_escape(result$actual), '</failure></testcase>'))
    }
  }
}

dbDisconnect(con)

if (length(commandArgs(trailingOnly = TRUE)) > 0L) {
  out <- commandArgs(trailingOnly = TRUE)[1]
  writeLines(c(
    '<?xml version="1.0"?>',
    '<testsuite name="r_rpostgres">',
    junit_lines,
    '</testsuite>'
  ), out)
}

message(sprintf("[r_rpostgres] %d passed, %d failed", passed, failed))
quit(status = if (failed == 0L) 0 else 1)
