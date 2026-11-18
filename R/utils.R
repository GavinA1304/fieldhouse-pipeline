# Shared helpers: logging, HTTP retries, Hugging Face reads, local staging.
suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(DBI)
  library(duckdb)
  library(yaml)
})

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a

log_msg <- function(...) {
  cat(format(Sys.time(), "%H:%M:%S"), sprintf(...), "\n")
  flush.console()
}

# ---- keeping data and secrets out of public logs ---------------------------
# The GitHub repo (and so its Actions logs and issues) is public. Nothing that
# reaches a log may contain data or credentials:
#  * mask_secret() tells GitHub to blank a derived token (e.g. TruMedia temp
#    token) everywhere in the log, the way it already blanks repo secrets.
#  * redact() strips quoted values (where DuckDB/HTTP errors would echo data),
#    query strings, and known secrets from any error text before it's printed.
.secrets <- new.env()
mask_secret <- function(x) {
  if (is.character(x) && length(x) == 1 && nzchar(x)) {
    .secrets$v <- unique(c(.secrets$v, x))
    if (nzchar(Sys.getenv("GITHUB_ACTIONS"))) cat(sprintf("::add-mask::%s\n", x))
  }
  invisible(x)
}
redact <- function(msg) {
  m <- paste(msg, collapse = " ")
  for (s in .secrets$v) m <- gsub(s, "***", m, fixed = TRUE)
  m <- gsub("'[^']*'", "'...'", m)
  m <- gsub('"[^"]*"', '"..."', m)
  m <- gsub("\\?[^ ]*", "?...", m)            # URL query strings
  m <- gsub("[[:space:]]+", " ", m)
  substr(m, 1, 300)
}

env <- function(name, required = TRUE) {
  v <- trimws(Sys.getenv(name))   # tolerate a stray space/newline pasted into a secret
  if (required && !nzchar(v)) stop("Missing environment variable ", name, call. = FALSE)
  if (grepl("TOKEN|SECRET|PASSWORD", name)) mask_secret(v)
  v
}

WORK  <- Sys.getenv("FIELDHOUSE_WORK", "build")
OUT   <- file.path(WORK, "out")    # files this run changes, laid out exactly like the HF repo
CACHE <- file.path(WORK, "cache")  # existing HF files downloaded for rebuilds
FAKEHUB <- file.path(WORK, "fakehub")  # stands in for HF during --dry-run

ensure_dir <- function(path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  path
}

# Every HTTP call goes through this: retries 429/5xx and dropped connections
# with backoff, so one flaky response never kills a run.
with_retry <- function(req, tries = 5) {
  req |>
    req_retry(
      max_tries = tries,
      retry_on_failure = TRUE,
      is_transient = function(resp) resp_status(resp) %in% c(408, 429, 500, 502, 503, 504),
      backoff = function(i) min(60, 2^i)
    ) |>
    req_timeout(300)
}

sql_str <- function(x) paste0("'", gsub("'", "''", x), "'")
sql_id  <- function(x) paste0('"', gsub('"', '""', x), '"')
sql_list <- function(paths) paste0("[", paste(sql_str(paths), collapse = ", "), "]")

# ---------------------------------------------------------------------------
# Hugging Face (read side). Uploads happen in scripts/hf_upload.py so they're
# one atomic commit; dry runs copy into build/fakehub instead.
# ---------------------------------------------------------------------------
Hub <- function(repo, token, dry_run = FALSE) {
  self <- new.env()
  self$repo <- repo
  self$token <- token
  self$dry_run <- dry_run
  self$files <- NULL
  self
}

next_link <- function(link) {
  if (is.null(link)) return(NULL)
  parts <- strsplit(link, ",")[[1]]
  p <- parts[grepl('rel="next"', parts)]
  if (!length(p)) return(NULL)
  trimws(sub(".*<([^>]+)>.*", "\\1", p[1]))
}

hub_list <- function(hub) {
  if (!is.null(hub$files)) return(hub$files)
  if (hub$dry_run) {
    dir.create(FAKEHUB, recursive = TRUE, showWarnings = FALSE)
    hub$files <- list.files(FAKEHUB, recursive = TRUE)
    return(hub$files)
  }
  req <- request("https://huggingface.co") |>
    req_url_path_append("api", "datasets", hub$repo, "tree", "main") |>
    req_url_query(recursive = "true") |>
    req_auth_bearer_token(hub$token) |>
    req_error(is_error = function(resp) FALSE) |>
    with_retry()
  first <- req_perform(req)
  if (resp_status(first) == 404) {  # first run: dataset doesn't exist yet
    hub$files <- character()
    return(hub$files)
  }
  if (resp_status(first) >= 400) stop("Hugging Face file listing failed: HTTP ", resp_status(first))
  resps <- list(first)
  nxt <- first
  repeat {  # paginate via Link: <...>; rel="next"
    url <- next_link(resp_header(nxt, "link"))
    if (is.null(url)) break
    nxt <- request(url) |> req_auth_bearer_token(hub$token) |> with_retry() |> req_perform()
    resps[[length(resps) + 1]] <- nxt
  }
  items <- unlist(lapply(resps, function(r) resp_body_json(r)), recursive = FALSE)
  hub$files <- vapply(Filter(function(x) identical(x$type, "file"), items), function(x) x$path, "")
  hub$files
}

# The dataset revision this run started from. The upload commits against it
# (parent_commit), so if anything else wrote in the meantime the commit is
# rejected instead of silently overwriting the other run's work.
hub_sha <- function(hub) {
  if (hub$dry_run) return("")
  resp <- request("https://huggingface.co") |>
    req_url_path_append("api", "datasets", hub$repo, "revision", "main") |>
    req_auth_bearer_token(hub$token) |>
    req_error(is_error = function(resp) FALSE) |>
    with_retry() |>
    req_perform()
  if (resp_status(resp) == 404) return("")
  if (resp_status(resp) >= 400) stop("Hugging Face revision lookup failed: HTTP ", resp_status(resp))
  resp_body_json(resp)$sha %||% ""
}

hub_download <- function(hub, path, dest_root = CACHE) {
  dest <- ensure_dir(file.path(dest_root, path))
  if (file.exists(dest)) return(dest)
  if (hub$dry_run) {
    file.copy(file.path(FAKEHUB, path), dest, overwrite = TRUE)
    return(dest)
  }
  # libcurl drops the Authorization header on the cross-host redirect to the
  # signed CDN URL, which is exactly what HF wants.
  request("https://huggingface.co") |>
    req_url_path_append("datasets", hub$repo, "resolve", "main", strsplit(path, "/")[[1]]) |>
    req_auth_bearer_token(hub$token) |>
    with_retry() |>
    req_perform(path = dest)
  dest
}

# Team-specific config (team codes, TruMedia queries, model names) lives in
# the PRIVATE dataset at config/config.yml, so none of it is in any repo.
# A local config.yml next to the scripts is used if present (dry runs, seeding).
load_config <- function(hub, here = getwd()) {
  local <- file.path(here, "config.yml")
  if (file.exists(local)) return(read_yaml(local))
  if ("config/config.yml" %in% hub_list(hub)) return(read_yaml(hub_download(hub, "config/config.yml")))
  stop("No config found: upload your config.yml to the private dataset at config/config.yml")
}

hub_read_json <- function(hub, path, default) {
  if (!(path %in% hub_list(hub))) return(default)
  fromJSON(hub_download(hub, path), simplifyVector = FALSE)
}

write_json_file <- function(obj, path) {
  writeLines(toJSON(obj, auto_unbox = TRUE, pretty = TRUE, null = "null", na = "null", digits = NA),
             ensure_dir(path))
}

# Dry-run "upload": copy staged files into the fake hub and apply deletions.
fakehub_commit <- function(delete_paths) {
  staged <- list.files(OUT, recursive = TRUE)
  for (f in staged) file.copy(file.path(OUT, f), ensure_dir(file.path(FAKEHUB, f)), overwrite = TRUE)
  for (d in delete_paths) unlink(file.path(FAKEHUB, d))
  log_msg("dry run: committed %d files, deleted %d into %s", length(staged), length(delete_paths), FAKEHUB)
}

duck_connect <- function() {
  con <- dbConnect(duckdb::duckdb())
  tmp <- file.path(WORK, "duck_tmp")
  dir.create(tmp, recursive = TRUE, showWarnings = FALSE)
  dbExecute(con, sprintf("SET temp_directory = %s", sql_str(tmp)))
  dbExecute(con, "SET preserve_insertion_order = false")
  con
}

# ---- Conferences (team code -> conference), from config/team_conferences.csv
# in the private dataset (columns: team_name, conference). New games get
# PitcherConference / BatterConference at load time, like the old sync did.
load_conferences <- function(hub, here = getwd()) {
  local <- file.path(here, "team_conferences.csv")
  path <- if (file.exists(local)) local else if ("config/team_conferences.csv" %in% hub_list(hub)) hub_download(hub, "config/team_conferences.csv") else NA
  if (is.na(path)) return(NULL)
  t <- utils::read.csv(path, colClasses = "character")
  setNames(t$conference, gsub("[^a-z0-9]", "", tolower(trimws(t$team_name))))
}
add_conferences <- function(df, conf) {
  if (is.null(conf)) return(df)
  look <- function(x) { out <- unname(conf[gsub("[^a-z0-9]", "", tolower(trimws(x)))])
                        out[is.na(out) & !is.na(x) & nzchar(x)] <- "Other / Unmapped"; out }
  if ("PitcherTeam" %in% names(df) && !"PitcherConference" %in% names(df)) df$PitcherConference <- look(df$PitcherTeam)
  if ("BatterTeam" %in% names(df) && !"BatterConference" %in% names(df)) df$BatterConference <- look(df$BatterTeam)
  df
}
