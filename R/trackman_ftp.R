# TrackMan FTP source: the CSVs TrackMan drops after each game.
#
# TrackMan's folder layout isn't reliable (files land in extra subfolders,
# under the wrong date, or get replaced in place), so discovery CRAWLS
# folders instead of trusting one exact path:
#   * every run: each day folder in the window, all subfolders, any depth
#   * deep scan (weekly nightly + every backfill): the whole year's tree
# Files are tracked by path + size + modified time, so a replaced file is
# pulled again. The same game found twice is de-duplicated by PitchUID later
# (verified copy wins).

ftp_handle <- function(...) {
  curl::new_handle(userpwd = paste0(env("TRACKMAN_FTP_USER"), ":", env("TRACKMAN_FTP_PASSWORD")),
                   ftp_use_epsv = TRUE, connecttimeout = 60, timeout = 300, ...)
}

.ftp <- new.env()

# One listing request. Retries only on connection problems; a server reply
# like "550 not found" or "500 MLSD not supported" just returns NULL.
ftp_fetch_listing <- function(url, ...) {
  for (attempt in 1:3) {
    res <- tryCatch(curl::curl_fetch_memory(url, handle = ftp_handle(...)), error = function(e) e)
    if (!inherits(res, "error")) return(res)
    msg <- conditionMessage(res)
    transient <- grepl("timed? ?out|reset|Could(n't| not) connect|Recv failure|Send failure|Failure when receiving",
                       msg, ignore.case = TRUE)
    if (!transient) return(NULL)
    Sys.sleep(2^attempt)
  }
  NULL
}

# List one folder -> data.frame(name, is_dir, size, modified). Tries MLSD
# (machine-readable), then LIST (Unix or Windows style), then bare names.
ftp_ls <- function(host, dir) {
  base <- sprintf("ftp://%s/%s", host, ftp_encode(sub("/?$", "/", dir)))
  empty <- data.frame(name = character(), is_dir = logical(), size = numeric(), modified = character())

  res <- if (!isFALSE(.ftp$mlsd)) ftp_fetch_listing(base, customrequest = "MLSD") else NULL
  if (is.null(.ftp$mlsd)) .ftp$mlsd <- !is.null(res) && res$status_code < 400   # probe once per run
  if (!is.null(res) && res$status_code < 400) {
    lines <- strsplit(rawToChar(res$content), "\r?\n")[[1]]
    lines <- lines[nzchar(lines)]
    if (length(lines) && all(grepl("=.*;", lines))) {
      facts <- sub(" .*$", "", lines)
      name <- sub("^[^ ]* ", "", lines)
      get <- function(key) vapply(strsplit(facts, ";"), function(fs) {
        kv <- fs[startsWith(tolower(fs), paste0(key, "="))]
        if (length(kv)) sub("^[^=]*=", "", kv[1]) else NA_character_
      }, "")
      type <- tolower(get("type"))
      out <- data.frame(name = name, is_dir = type %in% c("dir"), size = suppressWarnings(as.numeric(get("size"))),
                        modified = get("modify"))
      return(out[!type %in% c("cdir", "pdir") & !out$name %in% c(".", ".."), , drop = FALSE])
    }
  }

  res <- ftp_fetch_listing(base)
  if (!is.null(res) && res$status_code < 400) {
    lines <- strsplit(rawToChar(res$content), "\r?\n")[[1]]
    lines <- lines[nzchar(lines) & !grepl("^total ", lines)]
    if (length(lines)) {
      unix <- grepl("^[dl-][rwxs-]{9}", lines)
      win <- grepl("^\\d{2}-\\d{2}-\\d{2,4}\\s+\\d", lines)
      out <- empty
      if (any(unix)) {
        p <- strsplit(trimws(lines[unix]), "\\s+")
        out <- rbind(out, data.frame(
          name = vapply(p, function(x) paste(x[-(1:8)], collapse = " "), ""),
          is_dir = startsWith(lines[unix], "d"),
          size = vapply(p, function(x) suppressWarnings(as.numeric(x[5])), 0),
          modified = vapply(p, function(x) paste(x[6:8], collapse = " "), "")))
      }
      if (any(win)) {
        p <- strsplit(trimws(lines[win]), "\\s+")
        out <- rbind(out, data.frame(
          name = vapply(p, function(x) paste(x[-(1:3)], collapse = " "), ""),
          is_dir = vapply(p, function(x) toupper(x[3]) == "<DIR>", TRUE),
          size = vapply(p, function(x) suppressWarnings(as.numeric(x[3])), 0),
          modified = vapply(p, function(x) paste(x[1:2], collapse = " "), "")))
      }
      if (nrow(out)) return(out[!out$name %in% c(".", ".."), , drop = FALSE])
    }
  }

  res <- ftp_fetch_listing(base, dirlistonly = TRUE)
  if (is.null(res) || res$status_code >= 400) return(empty)
  n <- strsplit(rawToChar(res$content), "\r?\n")[[1]]
  n <- basename(n[nzchar(n)])
  data.frame(name = n, is_dir = !grepl("\\.[A-Za-z0-9]{2,4}$", n), size = NA_real_, modified = NA_character_)
}

# Recursively collect CSV files under a folder.
ftp_walk <- function(host, root, max_depth = 6) {
  found <- list()
  queue <- list(list(dir = sub("/?$", "/", root), depth = 0))
  seen <- character()
  while (length(queue)) {
    item <- queue[[1]]; queue <- queue[-1]
    if (item$dir %in% seen) next
    seen <- c(seen, item$dir)
    ls <- ftp_ls(host, item$dir)
    if (!nrow(ls)) next
    files <- ls[!ls$is_dir & grepl("\\.csv$", ls$name, ignore.case = TRUE), , drop = FALSE]
    if (nrow(files)) found[[length(found) + 1]] <- data.frame(
      path = paste0(item$dir, files$name), size = files$size, modified = files$modified)
    if (item$depth < max_depth) {
      for (d in ls$name[ls$is_dir]) queue[[length(queue) + 1]] <- list(dir = paste0(item$dir, d, "/"), depth = item$depth + 1)
    }
  }
  if (!length(found)) return(data.frame(path = character(), size = numeric(), modified = character()))
  out <- do.call(rbind, found)
  out[!duplicated(out$path), , drop = FALSE]
}

fill_template <- function(t, d) {
  d <- as.Date(d)
  t <- gsub("{yyyy}", format(d, "%Y"), t, fixed = TRUE)
  t <- gsub("{mm}", format(d, "%m"), t, fixed = TRUE)
  gsub("{dd}", format(d, "%d"), t, fixed = TRUE)
}

# Date a file belongs to, read from its folder path (yyyy/mm/dd) or file
# name (yyyymmdd); NA if neither is there.
path_date <- function(paths) {
  m <- regmatches(paths, regexpr("(19|20)\\d{2}/[01]\\d/[0-3]\\d", paths))
  out <- rep(as.Date(NA), length(paths))
  has <- grepl("(19|20)\\d{2}/[01]\\d/[0-3]\\d", paths)
  out[has] <- as.Date(gsub("/", "-", m), optional = TRUE)
  m2 <- regmatches(basename(paths), regexpr("(19|20)\\d{6}", basename(paths)))
  has2 <- is.na(out) & grepl("(19|20)\\d{6}", basename(paths))
  out[has2] <- as.Date(m2, format = "%Y%m%d", optional = TRUE)
  out
}

# Returns data.frame(path, size, modified) of candidate CSVs.
ftp_discover <- function(cfg, from, to, deep = FALSE, backfill = FALSE) {
  f <- cfg$trackman$ftp
  host <- f$host
  depth <- as.integer(f$max_depth %||% 6)
  # day folders: legacy path_templates ("v3/{yyyy}/{mm}/{dd}/CSV/") are widened to the day level
  day_t <- unique(c(unlist(f$day_templates), sub("CSV/?$", "", unlist(f$path_templates))))
  day_t <- day_t[nzchar(day_t)]
  if (!length(day_t)) day_t <- "v3/{yyyy}/{mm}/{dd}/"
  dates <- seq(as.Date(from), as.Date(to), by = 1)
  roots <- unique(unlist(lapply(dates, function(d) vapply(day_t, fill_template, "", d = d))))
  if (deep) {  # the whole year's tree (it contains the day folders too)
    year_t <- unlist(f$year_roots) %||% "v3/{yyyy}/"
    years <- unique(format(dates, "%Y"))
    roots <- unique(unlist(lapply(years, function(y) vapply(year_t, fill_template, "", d = paste0(y, "-01-01")))))
  }
  log_msg("FTP: crawling %d folder root(s)%s", length(roots), if (deep) " (deep scan)" else "")
  found <- do.call(rbind, lapply(roots, function(r) ftp_walk(host, r, depth)))
  if (is.null(found) || !nrow(found)) return(data.frame(path = character(), size = numeric(), modified = character()))
  found <- found[!duplicated(found$path), , drop = FALSE]
  # Ignore files from before the history load (they're already in), and files
  # clearly outside the window. Files with no date in their path are kept.
  pd <- path_date(found$path)
  # backfill: exactly the requested window. Nightly deep scan: anything this
  # season not already covered by the history load (ftp.ignore_before).
  floor_date <- if (backfill) as.Date(from) else if (deep && !is.null(f$ignore_before)) as.Date(f$ignore_before) else as.Date(from) - 1
  keep <- is.na(pd) | (pd >= floor_date & pd <= as.Date(to) + 1)
  found[keep, , drop = FALSE]
}

# ---- Games, not files -------------------------------------------------------
# A game folder can hold up to three files sharing one base name:
#   <base>.csv                               verified pitch data (preferred)
#   <base>_unverified.csv                     same pitches, before TrackMan QC
#   <base>[_unverified]_playerpositioning*.csv  fielder positions at release (2024+)
# Positioning files are NEVER loaded as pitches; they're joined onto them.
derive_game_base <- function(x) {
  b <- sub("\\.csv$", "", basename(x), ignore.case = TRUE)
  b <- sub("_playerpositioning.*$", "", b, ignore.case = TRUE)
  sub("_unverified$", "", b, ignore.case = TRUE)
}

ftp_group_games <- function(files) {
  if (!nrow(files)) return(list())
  kind <- ifelse(grepl("_playerpositioning", files$path, ignore.case = TRUE), "fhc",
          ifelse(grepl("_unverified\\.csv$", files$path, ignore.case = TRUE), "unverified", "verified"))
  key <- paste0(dirname(files$path), "/", derive_game_base(files$path))
  games <- list()
  for (k in unique(key)) {
    i <- which(key == k)
    pick <- function(kd) { j <- i[kind[i] == kd]; if (length(j)) files$path[j[1]] else NA_character_ }
    pitch <- pick("verified"); verified <- !is.na(pitch)
    if (!verified) pitch <- pick("unverified")
    if (is.na(pitch)) next                      # positioning file with no pitch file yet
    games[[length(games) + 1]] <- list(base = basename(k), pitch = pitch, verified = verified,
                                       fhc = pick("fhc"), members = files[i, , drop = FALSE])
  }
  games
}

# Fill a defender's missing position from the nearest pitch (within 3) where
# the same named player was tracked - same rule as the old DRS pipeline.
impute_fhc_positions <- function(x, positions = c("1B", "2B", "3B", "SS", "LF", "CF", "RF"), max_gap = 3) {
  if (!"PitchNo" %in% names(x)) return(x)
  pn <- suppressWarnings(as.numeric(x$PitchNo))
  for (pos in positions) {
    xc <- paste0(pos, "_PositionAtReleaseX"); zc <- paste0(pos, "_PositionAtReleaseZ"); nc <- paste0(pos, "_Name")
    if (!all(c(xc, zc, nc) %in% names(x))) next
    xv <- suppressWarnings(as.numeric(x[[xc]])); zv <- suppressWarnings(as.numeric(x[[zc]]))
    nm <- as.character(x[[nc]]); flag <- rep(FALSE, nrow(x))
    obs <- which(is.finite(xv) & is.finite(zv) & !is.na(nm) & nzchar(nm))
    for (i in which(!is.finite(xv) | !is.finite(zv))) {
      if (is.na(nm[i]) || !nzchar(nm[i]) || !is.finite(pn[i])) next
      cand <- obs[nm[obs] == nm[i]]
      if (!length(cand)) next
      gaps <- abs(pn[cand] - pn[i])
      if (min(gaps) <= max_gap) { j <- cand[which.min(gaps)]; xv[i] <- xv[j]; zv[i] <- zv[j]; flag[i] <- TRUE }
    }
    x[[xc]] <- xv; x[[zc]] <- zv; x[[paste0(pos, "_PositionImputed")]] <- flag
  }
  x
}

ftp_read_csv <- function(cfg, remote) {
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp))
  ok <- FALSE
  for (attempt in 1:4) {
    ok <- tryCatch({
      curl::curl_download(sprintf("ftp://%s/%s", cfg$trackman$ftp$host, ftp_encode(remote)), tmp, handle = ftp_handle())
      TRUE
    }, error = function(e) { log_msg("ftp retry %d: %s", attempt, redact(conditionMessage(e))); FALSE })
    if (ok) break
    Sys.sleep(2^attempt)
  }
  if (!ok) stop("FTP download failed after retries")
  utils::read.csv(tmp, colClasses = "character", check.names = FALSE, na.strings = c("", "NA"), encoding = "UTF-8")
}

# Spaces and odd characters in TrackMan file names break FTP URLs unless
# each path segment is encoded (the "/" separators stay as they are).
ftp_encode <- function(path) {
  parts <- strsplit(path, "/", fixed = TRUE)[[1]]
  paste(vapply(parts, function(p) if (nzchar(p)) utils::URLencode(p, reserved = TRUE) else p, ""), collapse = "/")
}

# One game -> one data.frame: pitch rows + positioning columns joined on.
ftp_pull_game <- function(cfg, g) {
  df <- ftp_read_csv(cfg, g$pitch)
  if (!nrow(df)) return(df)
  if (!is.na(g$fhc)) {
    fhc <- tryCatch(impute_fhc_positions(ftp_read_csv(cfg, g$fhc)), error = function(e) NULL)
    keys <- if (!is.null(fhc)) intersect(c("PitchUID", "PlayID"), intersect(names(df), names(fhc))) else character()
    if (length(keys)) {
      fhc <- fhc[, c(keys, setdiff(names(fhc), names(df))), drop = FALSE]
      for (k in keys) { df[[k]] <- as.character(df[[k]]); fhc[[k]] <- as.character(fhc[[k]]) }
      fhc <- fhc[!duplicated(fhc[keys]), , drop = FALSE]
      df <- merge(df, fhc, by = keys, all.x = TRUE, sort = FALSE)
    }
  }
  df$SourceFile <- basename(g$pitch)
  df$Verified <- g$verified
  df$Source <- "trackman_ftp"
  df$PulledAtUtc <- format(Sys.time(), tz = "UTC", usetz = FALSE)
  if (!"GameUID" %in% names(df)) df$GameUID <- g$base
  df
}

# Has this file changed since we last pulled it?
ftp_is_new <- function(row, prev) {
  if (is.null(prev)) return(TRUE)
  same_size <- is.na(row$size) || identical(as.numeric(prev$size %||% NA), as.numeric(row$size))
  same_mod <- is.na(row$modified) || identical(as.character(prev$modified %||% NA), as.character(row$modified))
  !(same_size && same_mod)
}

ftp_pull_file <- function(cfg, remote) {
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp))
  ok <- FALSE
  for (attempt in 1:4) {
    ok <- tryCatch({
      curl::curl_download(sprintf("ftp://%s/%s", cfg$trackman$ftp$host, ftp_encode(remote)), tmp, handle = ftp_handle())
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
