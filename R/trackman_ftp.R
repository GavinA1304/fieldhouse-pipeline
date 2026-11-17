# TrackMan FTP source: the CSVs TrackMan drops after each game.
# Same column layout as your historical table, so it's also the fallback if
# the API ever misses sessions (e.g. data shared to you by other teams).

ftp_handle <- function(...) {
  curl::new_handle(userpwd = paste0(env("TRACKMAN_FTP_USER"), ":", env("TRACKMAN_FTP_PASSWORD")),
                   ftp_use_epsv = TRUE, connecttimeout = 60, ...)
}

ftp_list <- function(host, dir) {
  url <- sprintf("ftp://%s/%s", host, dir)
  res <- tryCatch(curl::curl_fetch_memory(url, handle = ftp_handle(dirlistonly = TRUE)),
                  error = function(e) NULL)            # missing day folder = no games that day
  if (is.null(res) || res$status_code >= 400) return(character())
  x <- strsplit(rawToChar(res$content), "\r?\n")[[1]]
  x[nzchar(x)]
}

ftp_dirs_for_dates <- function(templates, dates) {
  out <- character()
  for (d in as.character(dates)) {
    d <- as.Date(d)
    for (t in templates) {
      p <- gsub("{yyyy}", format(d, "%Y"), t, fixed = TRUE)
      p <- gsub("{mm}", format(d, "%m"), p, fixed = TRUE)
      p <- gsub("{dd}", format(d, "%d"), p, fixed = TRUE)
      out <- c(out, p, paste0(p, "unverified/"))
    }
  }
  unique(out)
}

# Returns list of remote CSV paths (relative to host) in the date window.
ftp_discover <- function(cfg, from, to) {
  host <- cfg$trackman$ftp$host
  dirs <- ftp_dirs_for_dates(cfg$trackman$ftp$path_templates, seq(as.Date(from), as.Date(to), by = 1))
  files <- character()
  for (d in dirs) {
    names <- ftp_list(host, d)
    names <- names[grepl("\\.csv$", names, ignore.case = TRUE)]
    files <- c(files, paste0(d, basename(names)))
  }
  unique(files)
}

ftp_pull_file <- function(cfg, remote) {
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp))
  for (attempt in 1:4) {
    ok <- tryCatch({
      curl::curl_download(sprintf("ftp://%s/%s", cfg$trackman$ftp$host, remote), tmp, handle = ftp_handle())
      TRUE
    }, error = function(e) { log_msg("ftp retry %d: %s", attempt, redact(conditionMessage(e))); FALSE })
    if (ok) break
    Sys.sleep(2^attempt)
  }
  if (!ok) stop("FTP download failed after retries")
  # Everything as text first; cast_frame() then applies the fixed schema.
  df <- utils::read.csv(tmp, colClasses = "character", check.names = FALSE,
                        na.strings = c("", "NA"), encoding = "UTF-8")
  if (!nrow(df)) return(df)
  df$Verified <- !grepl("unverified", remote, ignore.case = TRUE)
  df$Source <- "trackman_ftp"
  df$PulledAtUtc <- format(Sys.time(), tz = "UTC", usetz = FALSE)
  if (!"GameUID" %in% names(df)) df$GameUID <- tools::file_path_sans_ext(basename(remote))
  df
}
