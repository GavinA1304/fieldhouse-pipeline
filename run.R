#!/usr/bin/env Rscript
# Fieldhouse sync (public, generic pipeline: pulls, cleans, merges, summarizes).
# No team config, models or data live in this repo: config comes from the
# private dataset, and model scoring runs in the private repo.
#   Rscript run.R --mode live                 # every 15 min, all year: new sessions only
#   Rscript run.R --mode nightly              # re-check lookback window, TruMedia, compact, summaries, health
#   Rscript run.R --mode backfill --since 2026-06-01 --until 2026-06-30
#   add --dry-run to use a local fake hub (build/fakehub) instead of Hugging Face
#
# This script never uploads directly. It stages changes in build/out and
# writes build/delete_paths.txt; scripts/hf_upload.py applies them as ONE
# atomic commit. A crash anywhere before that leaves Hugging Face untouched.

args <- commandArgs(TRUE)
opt <- function(flag, default = NULL) { i <- match(flag, args); if (is.na(i)) default else args[i + 1] }
MODE <- opt("--mode", "live")
DRY <- "--dry-run" %in% args
FORCE <- "--force" %in% args   # re-pull sessions even if already seen
stopifnot(MODE %in% c("live", "nightly", "backfill"))
Sys.setenv(TZ = "UTC")

file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
HERE <- if (length(file_arg)) dirname(normalizePath(sub("^--file=", "", file_arg))) else getwd()
for (f in c("utils.R", "clean.R", "trackman_api.R", "trackman_ftp.R", "trumedia.R", "trumedia_enrich.R", "build.R")) {
  source(file.path(HERE, "R", f))
}
cfg <- NULL  # loaded from the private dataset inside main()

unlink(c(OUT, CACHE, file.path(WORK, "stage")), recursive = TRUE)
unlink(file.path(WORK, c("delete_paths.txt", "errors.txt", "health_problems.txt", "commit_message.txt", "base_sha.txt", "new_data.txt")))
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)

main <- function() {
  hub <- if (DRY) Hub("dry-run", "", TRUE) else Hub(env("HF_REPO"), env("HF_TOKEN"))
  writeLines(hub_sha(hub), file.path(WORK, "base_sha.txt"))  # read BEFORE anything else
  cfg <<- load_config(hub, HERE)
  empty_obj <- function() setNames(list(), character())
  manifest <- hub_read_json(hub, "manifest.json", list(version = 1))
  for (k in c("trackman", "summary", "trumedia")) if (!length(manifest[[k]])) manifest[[k]] <- empty_obj()
  manifest <- register_collection(manifest, "trackman", cfg)
  state <- hub_read_json(hub, "state/trackman.json", list(api = empty_obj(), ftp = empty_obj()))
  for (k in c("api", "ftp")) if (!length(state[[k]])) state[[k]] <- empty_obj()

  today <- Sys.Date()
  if (MODE == "backfill") {
    FROM <- as.Date(opt("--since") %||% stop("--since is required for backfill"))
    TO <- as.Date(opt("--until", format(today)))
  } else {
    FROM <- today - if (MODE == "live") cfg$trackman$live_lookback_days else cfg$trackman$lookback_days
    TO <- today
  }
  log_msg("mode=%s window=%s..%s dry_run=%s source=%s", MODE, FROM, TO, DRY, cfg$trackman$source)

  con <- duck_connect()
  errors <- character()
  new_deltas <- list()
  now_utc <- function() format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  safe_key <- function(x) gsub("[^A-Za-z0-9_.-]", "_", x)

  # Clean one batch, then split it into per-season delta files.
  conf <- load_conferences(hub, HERE)
  ingest <- function(df, key) {
    if (!nrow(df)) return(0)
    df <- add_conferences(df, conf)
    stage <- file.path(WORK, "stage", paste0(safe_key(key), ".parquet"))
    n <- write_clean_parquet(con, df, stage, cfg)
    if (n[["rows_in"]] != n[["rows_out"]]) log_msg("  dropped %d impossible rows", n[["rows_in"]] - n[["rows_out"]])
    seasons <- dbGetQuery(con, sprintf(
      "SELECT DISTINCT coalesce(CAST(Season AS VARCHAR), 'unknown') s FROM read_parquet(%s)", sql_str(stage)))$s
    for (s in seasons) {
      part <- file.path(WORK, "stage", paste0("season=", s), paste0(safe_key(key), ".parquet"))
      dbExecute(con, sprintf(
        "COPY (SELECT * FROM read_parquet(%s) WHERE coalesce(CAST(Season AS VARCHAR), 'unknown') = %s) TO %s (FORMAT parquet, COMPRESSION zstd)",
        sql_str(stage), sql_str(s), sql_str(ensure_dir(part))))
      new_deltas[[s]] <<- unique(c(new_deltas[[s]], part))
    }
    n[["rows_out"]]
  }

  # ---- TrackMan API ------------------------------------------------------------
  if (cfg$trackman$source %in% c("api", "both")) {
    kinds <- c("game", if (isTRUE(cfg$trackman$include_practice)) "practice")
    for (kind in kinds) {
      stype <- if (kind == "game") cfg$trackman$session_type else cfg$trackman$practice_session_type %||% "All"
      sessions <- tryCatch(tm_discover(FROM, TO, kind, stype), error = function(e) {
        errors <<- c(errors, paste("TrackMan discovery failed:", redact(conditionMessage(e)))); list()
      })
      log_msg("TrackMan %s sessions in window: %d", kind, length(sessions))
      for (s in sessions) {
        id <- s$sessionId
        prev <- state$api[[id]]
        verified <- isTRUE(s$verified)
        if (!FORCE && !is.null(prev) && (isTRUE(prev$verified) || !verified)) next  # nothing new
        year <- substr(s$gameDateLocal %||% s$gameDateUtc %||% format(today), 1, 4)
        raw_dir <- if (isTRUE(cfg$trackman$keep_raw_json)) file.path(OUT, sprintf("trackman/raw_json/season=%s", year))
        rows <- tryCatch({
          df <- tm_pull_session(s, kind, raw_dir)
          ingest(df, paste0("api_", kind, "_", id))
        }, error = function(e) {
          errors <<- c(errors, sprintf("TrackMan session %s failed: %s", id, redact(conditionMessage(e)))); NA
        })
        if (!is.na(rows)) {
          state$api[[id]] <- list(verified = verified, date = substr(s$gameDateLocal %||% "", 1, 10),
                                  kind = kind, rows = rows, pulled_utc = now_utc(),
                                  teams = paste(s$awayTeam$shortName %||% "?", "@", s$homeTeam$shortName %||% "?"))
          log_msg("  pulled %s session %s (%d rows%s)", kind, id, rows, if (verified) ", verified" else "")
        }
      }
    }
  }

  # ---- TrackMan FTP ------------------------------------------------------------
  if (cfg$trackman$source %in% c("ftp", "both")) {
    # Deep scan of the whole season's folder tree on every backfill and once a
    # week at night, to catch files TrackMan filed in odd places.
    deep_day <- as.integer(cfg$trackman$ftp$deep_scan_weekday %||% 1)   # 1 = Monday
    deep <- MODE == "backfill" || (MODE == "nightly" && as.integer(format(today, "%u")) == deep_day) || "--deep" %in% args
    files <- tryCatch(ftp_discover(cfg, FROM, TO, deep, backfill = MODE == "backfill"), error = function(e) {
      errors <<- c(errors, paste("FTP listing failed:", redact(conditionMessage(e))))
      data.frame(path = character(), size = numeric(), modified = character())
    })
    games <- ftp_group_games(files)
    is_new <- function(g) FORCE || any(vapply(seq_len(nrow(g$members)), function(i)
      ftp_is_new(g$members[i, ], state$ftp[[g$members$path[i]]]), TRUE))
    todo <- Filter(is_new, games)
    log_msg("FTP: %d CSVs found (%d games), %d games new or changed", nrow(files), length(games), length(todo))
    for (i in seq_along(todo)) {
      g <- todo[[i]]
      rows <- tryCatch(ingest(ftp_pull_game(cfg, g), paste0("ftp_", gsub("[^A-Za-z0-9]", "_", sub("\\.csv$", "", g$pitch, ignore.case = TRUE)))),
                       error = function(e) { errors <<- c(errors, sprintf("FTP game %d failed: %s", i, redact(conditionMessage(e)))); NA })
      if (!is.na(rows)) {
        for (j in seq_len(nrow(g$members))) {
          m <- g$members[j, ]
          state$ftp[[m$path]] <- list(rows = rows, size = m$size, modified = m$modified, pulled_utc = now_utc())
        }
        log_msg("  game %d of %d: %d rows kept%s%s", i, length(todo), rows,
                if (g$verified) ", verified" else ", unverified", if (!is.na(g$fhc)) ", +positioning" else "")
      }
    }
  }

  # New sessions are folded into ONE live file per season
  # (trackman/delta/season=YYYY/live.parquet), so the app never has to open
  # hundreds of small files on a busy college Saturday.
  delete_paths <- character()
  for (s in names(new_deltas)) {
    entry <- manifest$trackman[[s]] %||% list()
    old <- unlist(entry$deltas)
    old_local <- vapply(old, function(d) if (d %in% hub_list(hub)) hub_download(hub, d) else NA_character_, "")
    live_rel <- file.path(delta_dir(s), "live.parquet")
    merge_files(con, cfg, sprintf("live %s", s), c(old_local[!is.na(old_local)], new_deltas[[s]]),
                file.path(OUT, live_rel))
    delete_paths <- c(delete_paths, intersect(setdiff(old, live_rel), hub_list(hub)))
    entry$deltas <- list(live_rel)
    entry$updated_utc <- now_utc()
    manifest$trackman[[s]] <- entry
  }
  n_new <- length(unlist(new_deltas))

  # ---- nightly / backfill: TruMedia, compaction, summaries, health -------------
  if (MODE != "live") {
    if (isTRUE(cfg$trumedia$enabled)) {
      tm <- run_trumedia(con, cfg)
      errors <- c(errors, tm$errors)
      for (nm in names(tm$paths)) {
        cur <- manifest$trumedia[[nm]] %||% empty_obj()
        for (s in names(tm$paths[[nm]])) cur[[s]] <- tm$paths[[nm]][[s]]
        manifest$trumedia[[nm]] <- cur
      }
    }

    # Seasons to rebuild: every season with new live data, plus seasons the
    # private scoring job re-scored (their summaries need the new averages).
    ms <- models_enabled(cfg)
    for (s in names(manifest$trackman)) {
      entry <- manifest$trackman[[s]]
      deltas <- unlist(entry$deltas)
      due <- tm_enrich_due(cfg, MODE, s)        # weekly TruMedia fielder fill (TeamPlays)
      due_p <- tm_pitches_due(cfg, MODE, s)     # nightly TruMedia pitch fill (new games only)
      if (!length(deltas) && !isTRUE(entry$summaries_stale) && !due && !due_p) next
      out_file <- file.path(OUT, season_path(s))
      base_local <- if (!is.null(entry$pitches) && entry$pitches %in% hub_list(hub)) hub_download(hub, entry$pitches) else NA
      models_used <- entry$models %||% empty_obj()
      if (length(deltas)) {
        delta_local <- vapply(deltas, function(d) {
          local <- file.path(OUT, d)
          if (file.exists(local)) local else if (d %in% hub_list(hub)) hub_download(hub, d) else NA_character_
        }, "")
        delta_local <- delta_local[!is.na(delta_local)]
        # Live rows the private job hasn't scored yet -> flag the season so it
        # gets re-scored, instead of quietly keeping blanks.
        if (length(delta_local) && length(ms)) {
          have <- dbGetQuery(con, sprintf("DESCRIBE SELECT * FROM read_parquet(%s, union_by_name = true)", sql_list(delta_local)))$column_name
          for (m in ms) {
            col <- unlist(m$columns)[1]
            if (is.null(col)) next
            unscored <- !(col %in% have) || dbGetQuery(con, sprintf(
              "SELECT count(*) n FROM read_parquet(%s, union_by_name = true) WHERE PitchUID IS NOT NULL AND %s IS NULL",
              sql_list(delta_local), sql_id(col)))$n > 0
            if (unscored) models_used[[m$name]] <- "partial"
          }
        }
        rows <- merge_files(con, cfg, sprintf("season %s", s), c(base_local, delta_local), out_file)  # stops on failed check
        if (is.null(rows)) next
        source_file <- out_file
      } else {
        if (is.na(base_local)) next
        rows <- entry$rows
        source_file <- base_local                    # summaries only; season file unchanged
        if (due || due_p) { file.copy(base_local, ensure_dir(out_file), overwrite = TRUE); source_file <- out_file }
      }
      pitches_filled <- 0
      if (due_p && source_file == out_file) {
        if (is.null(state$tm_games)) state$tm_games <- empty_obj()
        state$tm_games <- tryCatch({
          st <- tm_enrich_pitches(con, cfg, s, source_file, state$tm_games)
          pitches_filled <- attr(st, "filled") %||% 0
          attr(st, "filled") <- NULL
          st
        }, error = function(e) {
          errors <<- c(errors, sprintf("TruMedia pitch fill %s failed (data still loaded): %s", s, redact(conditionMessage(e))))
          state$tm_games })
      }
      # Nightly pitch check found no new games and nothing else changed:
      # don't re-upload an identical season file.
      if (!length(deltas) && !due && pitches_filled == 0 && source_file == out_file) {
        unlink(out_file); source_file <- base_local
        if (!isTRUE(entry$summaries_stale)) { unlink(base_local); next }
      }
      if (due) tryCatch(tm_enrich_file(con, cfg, s, source_file), error = function(e)
        errors <<- c(errors, sprintf("TruMedia fielder fill %s failed (data still loaded): %s", s, redact(conditionMessage(e)))))
      built <- build_season_outputs(con, s, source_file, cfg)
      manifest$trackman[[s]] <- list(pitches = season_path(s), rows = rows, deltas = list(),
                                     models = models_used, summaries_stale = FALSE,
                                     last_game = built$last_game, last_our_game = built$last_our_game,
                                     updated_utc = now_utc())
      for (nm in names(built$paths)) {
        cur <- manifest$summary[[nm]] %||% empty_obj()
        cur[[s]] <- built$paths[[nm]]
        manifest$summary[[nm]] <- cur
      }
      # merged deltas: remote ones get deleted in the same commit that adds the
      # verified season file; local ones simply don't get uploaded.
      delete_paths <- unique(c(delete_paths, intersect(deltas, hub_list(hub))))
      unlink(file.path(OUT, deltas))
      if (!is.na(base_local)) unlink(base_local)
    }

    # Other collections (Cape League): no live feed yet, but when the private
    # scorer re-scores them their summaries need rebuilding too.
    for (coll in setdiff(names(manifest$collections), "trackman")) {
      SK <- summary_key(coll)
      if (!length(manifest[[SK]])) manifest[[SK]] <- empty_obj()
      for (s in names(manifest[[coll]])) {
        entry <- manifest[[coll]][[s]]
        if (!isTRUE(entry$summaries_stale) || is.null(entry$pitches) || !(entry$pitches %in% hub_list(hub))) next
        local <- hub_download(hub, entry$pitches)
        built <- tryCatch(build_season_outputs(con, s, local, cfg, coll), error = function(e) {
          errors <<- c(errors, sprintf("%s %s summaries failed: %s", coll, s, redact(conditionMessage(e)))); NULL })
        unlink(local)
        if (is.null(built)) next
        entry$summaries_stale <- FALSE
        entry$updated_utc <- now_utc()
        manifest[[coll]][[s]] <- entry
        for (nm in names(built$paths)) {
          cur <- manifest[[SK]][[nm]] %||% empty_obj()
          cur[[s]] <- built$paths[[nm]]
          manifest[[SK]][[nm]] <- cur
        }
        log_msg("%s %s: summaries rebuilt", coll, s)
      }
    }

    problems <- health_check(cfg, manifest)
    manifest$health <- list(problems = as.list(problems), checked_utc = now_utc())
    if (length(problems)) writeLines(problems, file.path(WORK, "health_problems.txt"))
    for (p in problems) log_msg("HEALTH: %s", p)
  }

  # ---- stage manifest/state, unless a live run found nothing -------------------
  if (MODE != "live" || n_new > 0) {
    manifest$updated_utc <- now_utc()
    manifest$team_codes <- as.list(cfg$team$codes)
    manifest[[paste0("last_", MODE, "_utc")]] <- now_utc()
    manifest$last_errors <- as.list(errors)
    write_json_file(manifest, file.path(OUT, "manifest.json"))
    write_json_file(state, file.path(OUT, "state/trackman.json"))
  }
  writeLines(delete_paths, file.path(WORK, "delete_paths.txt"))
  writeLines(sprintf("%s sync: %d new files, %d merged", MODE, n_new, length(delete_paths)),
             file.path(WORK, "commit_message.txt"))
  if (length(errors)) writeLines(errors, file.path(WORK, "errors.txt"))
  for (e in errors) log_msg("ERROR: %s", e)
  if (n_new > 0) writeLines("yes", file.path(WORK, "new_data.txt"))  # tells the workflow to wake the private scorer
  if (DRY) fakehub_commit(delete_paths)
  dbDisconnect(con, shutdown = TRUE)
  log_msg("done: %d new delta files, %d errors", n_new, length(errors))
}

# Any crash prints a redacted one-line reason (never data) and fails the job.
# Hugging Face is untouched because the upload step never runs.
tryCatch(main(), error = function(e) {
  cat("FATAL:", redact(conditionMessage(e)), "\n")
  quit(status = 1)
})
