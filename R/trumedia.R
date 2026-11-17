# TruMedia DirectedQuery client (temp token from master token, CSV queries).

.trm <- new.env()

trumedia_token <- function() {
  if (!is.null(.trm$token) && Sys.time() < .trm$expires) return(.trm$token)
  body <- request("https://api.trumedianetworks.com/v1/siteadmin/api/createTempPBToken") |>
    req_body_json(list(username = env("TRUMEDIA_USERNAME"),
                       sitename = env("TRUMEDIA_SITENAME"),
                       token = env("TRUMEDIA_MASTER_TOKEN"))) |>
    with_retry() |>
    req_perform() |>
    resp_body_json()
  .trm$token <- mask_secret(body$pbTempToken %||% stop("TruMedia token response had no pbTempToken"))
  .trm$expires <- Sys.time() + 20 * 3600  # temp tokens last 24h by default
  .trm$token
}

# Runs one configured query for one season; returns the CSV text.
trumedia_query <- function(cfg, q, season) {
  enc <- function(x) utils::URLencode(as.character(x), reserved = TRUE)
  params <- c(list(seasonYear = season), q$params %||% list())
  params <- params[vapply(params, function(v) length(v) && nzchar(as.character(v)), TRUE)]
  qs <- paste0(names(params), "=", vapply(params, enc, ""), collapse = "&")
  if (length(q$columns)) qs <- paste0(qs, "&columns=", enc(paste(q$columns, collapse = ",")))
  if (nzchar(q$filters %||% "")) {
    f <- q$filters
    qs <- paste0(qs, if (startsWith(f, "&")) f else paste0("&", f))
  }
  url <- sprintf("%s/%s.csv?%s&token=%s", sub("/$", "", cfg$trumedia$base_url), q$format, qs, trumedia_token())
  request(url) |> with_retry() |> req_perform() |> resp_body_string()
}

trumedia_seasons <- function(cfg) {
  s <- unlist(cfg$trumedia$seasons %||% list("current"))
  cur <- as.integer(format(Sys.Date(), "%Y"))
  unique(as.integer(ifelse(s == "current", cur, s)))
}

# Pull every configured query; write trumedia/<name>/season=<y>.parquet.
# Returns list(paths = name -> season -> repo path, errors = character()).
run_trumedia <- function(con, cfg) {
  out <- list()
  errs <- character()
  for (q in cfg$trumedia$queries) {
    for (season in trumedia_seasons(cfg)) {
      path <- sprintf("trumedia/%s/season=%d.parquet", q$name, season)
      res <- tryCatch({
        txt <- trumedia_query(cfg, q, season)
        tmp <- tempfile(fileext = ".csv"); writeLines(txt, tmp)
        n <- dbGetQuery(con, sprintf("SELECT count(*) n FROM read_csv(%s, header = true, sample_size = -1)", sql_str(tmp)))$n
        if (n > 0) {
          dbExecute(con, sprintf(
            "COPY (SELECT *, %d AS Season FROM read_csv(%s, header = true, sample_size = -1)) TO %s (FORMAT parquet, COMPRESSION zstd)",
            season, sql_str(tmp), sql_str(ensure_dir(file.path(OUT, path)))))
        }
        unlink(tmp)
        n
      }, error = function(e) { log_msg("TruMedia %s %d FAILED: %s", q$name, season, redact(conditionMessage(e))); NA })
      if (!is.na(res)) {
        log_msg("TruMedia %s %d: %d rows", q$name, season, res)
        if (res > 0) out[[q$name]][[as.character(season)]] <- path
      } else {
        errs <- c(errs, sprintf("TruMedia %s %d failed", q$name, season))
      }
    }
  }
  list(paths = out, errors = errs)
}
